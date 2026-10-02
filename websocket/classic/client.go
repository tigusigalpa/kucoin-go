// Package classic implements a reconnecting WebSocket client for
// KuCoin's Classic (Spot/Margin/Futures) WebSocket API. Construct a
// Client with a token from classic/spot/ws or classic/futures/ws (the
// REST bullet-token endpoints) — this package only handles the socket
// connection itself.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/introduction
package classic

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Message is a single push received on a subscribed topic.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/introduction
type Message struct {
	Type        string          `json:"type"`
	Topic       string          `json:"topic"`
	Subject     string          `json:"subject"`
	UserID      string          `json:"userId,omitempty"`
	ChannelType string          `json:"channelType,omitempty"`
	Data        json.RawMessage `json:"data"`
}

// Logger is a minimal structured-logging interface.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Debug(string, ...any) {}
func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}

// Option configures a Client at construction time.
type Option func(*Client)

// WithLogger sets a structured logger for connection lifecycle events.
func WithLogger(l Logger) Option {
	return func(c *Client) { c.logger = l }
}

// WithAutoReconnect toggles automatic reconnection with exponential
// backoff on unexpected disconnects. Enabled by default; disable for
// private/trading streams if you want reconnects to be explicit.
func WithAutoReconnect(enabled bool) Option {
	return func(c *Client) { c.autoReconnect = enabled }
}

// WithPingInterval overrides the ping cadence. It defaults to the value
// from the REST token response if left zero.
func WithPingInterval(d time.Duration) Option {
	return func(c *Client) { c.pingInterval = d }
}

// WithPingTimeout overrides the pong-wait timeout. It defaults to the value
// from the REST token response if left zero.
func WithPingTimeout(d time.Duration) Option {
	return func(c *Client) { c.pingTimeout = d }
}

const (
	defaultPingInterval = 18 * time.Second
	defaultPingTimeout  = 10 * time.Second
	subscribeAckTimeout = 10 * time.Second
	reconnectMin        = 1 * time.Second
	reconnectMax        = 60 * time.Second
	subBufferSize       = 256
)

var (
	// ErrAlreadyConnected is returned when Connect is called while a socket is
	// already active. One Client owns exactly one WebSocket connection.
	ErrAlreadyConnected = errors.New("kucoin: classic ws: already connected")
	// ErrReconnecting is returned when a caller tries to manually connect while
	// this Client is restoring an interrupted connection.
	ErrReconnecting = errors.New("kucoin: classic ws: reconnect in progress")
)

type subscription struct {
	topic          string
	privateChannel bool
	ch             chan Message
}

// Client is a reconnecting WebSocket client for one Classic WS endpoint
// (Spot/Margin or Futures — both share the same wire protocol; only the
// host/token differ).
type Client struct {
	endpoint  string
	token     string
	connectID string

	pingInterval  time.Duration
	pingTimeout   time.Duration
	logger        Logger
	autoReconnect bool

	mu            sync.RWMutex
	writeMu       sync.Mutex
	conn          *websocket.Conn
	subscriptions map[string]*subscription
	ackWaiters    map[string]chan struct{}
	closed        bool
	reconnecting  bool
	shutdown      chan struct{}
	done          chan struct{}
	welcomed      chan struct{}
}

// NewClient creates a Client for a single Classic WS instance server.
// endpoint and token normally come straight from a bullet-token REST
// call's InstanceServers[0].Endpoint / Token fields.
func NewClient(endpoint, token string, opts ...Option) *Client {
	c := &Client{
		endpoint:      endpoint,
		token:         token,
		connectID:     randomID(),
		pingInterval:  defaultPingInterval,
		pingTimeout:   defaultPingTimeout,
		logger:        noopLogger{},
		autoReconnect: true,
		subscriptions: make(map[string]*subscription),
		ackWaiters:    make(map[string]chan struct{}),
		shutdown:      make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func randomID() string {
	buf := make([]byte, 8)
	_, _ = rand.Read(buf)
	return fmt.Sprintf("%x", buf)
}

// Connect dials the endpoint and blocks until KuCoin's welcome message
// arrives (or ctx is done / a timeout elapses).
func (c *Client) Connect(ctx context.Context) error {
	return c.connect(ctx, false)
}

func (c *Client) connect(ctx context.Context, reconnect bool) error {
	c.mu.Lock()
	if reconnect {
		if c.closed {
			c.mu.Unlock()
			return context.Canceled
		}
	} else {
		if c.conn != nil {
			c.mu.Unlock()
			return ErrAlreadyConnected
		}
		if c.reconnecting {
			c.mu.Unlock()
			return ErrReconnecting
		}
		if c.closed {
			c.closed = false
			c.shutdown = make(chan struct{})
		}
	}
	c.done = make(chan struct{})
	c.welcomed = make(chan struct{})
	done := c.done
	welcomed := c.welcomed
	c.mu.Unlock()

	conn, err := c.dial(ctx)
	if err != nil {
		c.discardConnection(nil, done)
		return err
	}

	c.mu.Lock()
	if c.closed || c.done != done {
		c.mu.Unlock()
		_ = conn.Close()
		return context.Canceled
	}
	c.conn = conn
	c.mu.Unlock()

	_ = conn.SetReadDeadline(time.Now().Add(c.pingInterval + c.pingTimeout))
	go c.readPump(conn, done)
	go c.pingPump(conn, done)

	select {
	case <-welcomed:
		c.logger.Info("kucoin: classic ws connected")
		return nil
	case <-time.After(subscribeAckTimeout):
		c.disconnect(conn, done)
		return fmt.Errorf("kucoin: classic ws: timed out waiting for welcome message")
	case <-ctx.Done():
		c.disconnect(conn, done)
		return ctx.Err()
	}
}

func (c *Client) dial(ctx context.Context) (*websocket.Conn, error) {
	endpoint, err := url.Parse(c.endpoint)
	if err != nil {
		return nil, fmt.Errorf("kucoin: classic ws: parse endpoint: %w", err)
	}
	query := endpoint.Query()
	query.Set("token", c.token)
	query.Set("connectId", c.connectID)
	endpoint.RawQuery = query.Encode()

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("kucoin: classic ws dial: %w", err)
	}
	return conn, nil
}

// Subscribe subscribes to a topic (e.g. "/market/ticker:BTC-USDT") and
// returns a buffered channel of pushes. Blocks until KuCoin acknowledges
// the subscription or subscribeAckTimeout elapses. The subscription is
// automatically restored after a reconnect.
func (c *Client) Subscribe(ctx context.Context, topic string, privateChannel bool) (<-chan Message, error) {
	key := topic
	c.mu.Lock()
	sub, exists := c.subscriptions[key]
	if !exists {
		sub = &subscription{topic: topic, privateChannel: privateChannel, ch: make(chan Message, subBufferSize)}
		c.subscriptions[key] = sub
	}
	id := randomID()
	ack := make(chan struct{})
	c.ackWaiters[id] = ack
	c.mu.Unlock()
	cleanup := func() {
		c.mu.Lock()
		delete(c.ackWaiters, id)
		if !exists && c.subscriptions[key] == sub {
			delete(c.subscriptions, key)
			close(sub.ch)
		}
		c.mu.Unlock()
	}

	req := map[string]any{
		"id":             id,
		"type":           "subscribe",
		"topic":          topic,
		"privateChannel": privateChannel,
		"response":       true,
	}
	if err := c.writeJSON(req); err != nil {
		cleanup()
		return nil, err
	}

	select {
	case <-ack:
		return sub.ch, nil
	case <-time.After(subscribeAckTimeout):
		cleanup()
		return nil, fmt.Errorf("kucoin: classic ws: subscribe to %q timed out waiting for ack", topic)
	case <-ctx.Done():
		cleanup()
		return nil, ctx.Err()
	}
}

// Unsubscribe removes a topic subscription and closes its data channel.
func (c *Client) Unsubscribe(topic string) error {
	c.mu.Lock()
	sub, exists := c.subscriptions[topic]
	if exists {
		delete(c.subscriptions, topic)
	}
	c.mu.Unlock()

	if !exists {
		return nil
	}
	close(sub.ch)

	return c.writeJSON(map[string]any{
		"id":             randomID(),
		"type":           "unsubscribe",
		"topic":          topic,
		"privateChannel": sub.privateChannel,
		"response":       false,
	})
}

// Close terminates the connection and stops all background loops. Safe
// to call multiple times.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.conn
	done := c.done
	shutdown := c.shutdown
	c.conn = nil
	c.done = nil
	c.welcomed = nil
	subscriptions := c.subscriptions
	c.subscriptions = make(map[string]*subscription)
	c.ackWaiters = make(map[string]chan struct{})
	closeSignal(done)
	closeSignal(shutdown)
	c.mu.Unlock()

	for _, sub := range subscriptions {
		close(sub.ch)
	}
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (c *Client) writeJSON(v any) error {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	return c.writeJSONTo(conn, v)
}

func (c *Client) writeJSONTo(conn *websocket.Conn, v any) error {
	if conn == nil {
		return fmt.Errorf("kucoin: classic ws: not connected")
	}
	// gorilla/websocket permits only one concurrent writer per connection.
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.mu.RLock()
	current := c.conn
	closed := c.closed
	c.mu.RUnlock()
	if closed || current != conn {
		return fmt.Errorf("kucoin: classic ws: not connected")
	}
	return conn.WriteJSON(v)
}

func (c *Client) pingPump(conn *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if err := c.writeJSONTo(conn, map[string]string{"id": randomID(), "type": "ping"}); err != nil {
				c.logger.Warn("kucoin: classic ws ping failed", "error", err)
			}
		}
	}
}

func (c *Client) readPump(conn *websocket.Conn, done chan struct{}) {
	for {
		select {
		case <-done:
			return
		default:
		}

		_, raw, err := conn.ReadMessage()
		if err != nil {
			c.logger.Warn("kucoin: classic ws read error", "error", err)
			if c.disconnect(conn, done) && c.autoReconnect {
				c.startReconnect()
			}
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(c.pingInterval + c.pingTimeout))

		c.handleMessageForConnection(conn, done, raw)
	}
}

func closeSignal(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// disconnect removes a specific connection only if it is still the active
// generation. This prevents a stale reader from tearing down a newer socket.
func (c *Client) disconnect(conn *websocket.Conn, done chan struct{}) bool {
	c.mu.Lock()
	active := c.conn == conn && c.done == done
	if active {
		c.conn = nil
		closeSignal(done)
	}
	closed := c.closed
	c.mu.Unlock()
	_ = conn.Close()
	return active && !closed
}

func (c *Client) discardConnection(conn *websocket.Conn, done chan struct{}) {
	c.mu.Lock()
	if c.done == done {
		if c.conn == conn {
			c.conn = nil
		}
		c.done = nil
		c.welcomed = nil
		closeSignal(done)
	}
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (c *Client) isClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

func (c *Client) startReconnect() {
	c.mu.Lock()
	if c.closed || c.reconnecting {
		c.mu.Unlock()
		return
	}
	c.reconnecting = true
	c.mu.Unlock()
	go c.reconnectLoop()
}

func (c *Client) handleMessage(raw []byte) {
	c.handleMessageForConnection(nil, nil, raw)
}

// handleMessageForConnection rejects frames from a superseded socket before
// they can acknowledge a subscription, complete a new welcome handshake, or
// reach a current subscriber. The nil form keeps the package-level decoder
// testable without a network connection.
func (c *Client) handleMessageForConnection(conn *websocket.Conn, done chan struct{}, raw []byte) {
	if conn != nil {
		c.mu.RLock()
		active := c.conn == conn && c.done == done
		c.mu.RUnlock()
		if !active {
			return
		}
	}

	var envelope struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		c.logger.Warn("kucoin: classic ws decode failed", "error", err)
		return
	}

	switch envelope.Type {
	case "welcome":
		c.mu.RLock()
		welcomed := c.welcomed
		c.mu.RUnlock()
		if welcomed != nil {
			select {
			case <-welcomed:
			default:
				close(welcomed)
			}
		}
		return
	case "pong":
		return // heartbeat acknowledged; nothing else to do
	case "ack":
		c.mu.Lock()
		if ch, ok := c.ackWaiters[envelope.ID]; ok {
			delete(c.ackWaiters, envelope.ID)
			close(ch)
		}
		c.mu.Unlock()
		return
	case "error":
		c.logger.Error("kucoin: classic ws error message", "raw", string(raw))
		return
	}

	var msg Message
	if err := json.Unmarshal(raw, &msg); err != nil {
		c.logger.Warn("kucoin: classic ws decode push failed", "error", err)
		return
	}

	c.mu.RLock()
	sub, ok := c.subscriptions[msg.Topic]
	c.mu.RUnlock()
	if !ok {
		return
	}
	select {
	case sub.ch <- msg:
	default:
		c.logger.Warn("kucoin: classic ws subscriber channel full, dropping message", "topic", msg.Topic)
	}
}

func (c *Client) reconnectLoop() {
	c.mu.RLock()
	shutdown := c.shutdown
	c.mu.RUnlock()
	defer func() {
		c.mu.Lock()
		c.reconnecting = false
		c.mu.Unlock()
	}()

	backoff := reconnectMin
	for {
		if c.isClosed() {
			return
		}
		timer := time.NewTimer(backoff)
		select {
		case <-shutdown:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}

		ctx, cancel := context.WithTimeout(context.Background(), subscribeAckTimeout)
		err := c.connect(ctx, true)
		cancel()
		if err != nil {
			if c.isClosed() {
				return
			}
			c.logger.Warn("kucoin: classic ws reconnect failed", "error", err, "backoff", backoff)
			backoff *= 2
			if backoff > reconnectMax {
				backoff = reconnectMax
			}
			continue
		}

		c.resubscribeAll()
		c.logger.Info("kucoin: classic ws reconnected")
		return
	}
}

func (c *Client) resubscribeAll() {
	c.mu.RLock()
	subs := make([]*subscription, 0, len(c.subscriptions))
	for _, sub := range c.subscriptions {
		subs = append(subs, sub)
	}
	c.mu.RUnlock()

	for _, sub := range subs {
		req := map[string]any{
			"id":             randomID(),
			"type":           "subscribe",
			"topic":          sub.topic,
			"privateChannel": sub.privateChannel,
			"response":       false,
		}
		if err := c.writeJSON(req); err != nil {
			c.logger.Warn("kucoin: classic ws resubscribe failed", "topic", sub.topic, "error", err)
		}
	}
}
