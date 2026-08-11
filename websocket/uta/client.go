// Package uta implements a reconnecting WebSocket client for KuCoin's
// UTA WebSocket API.
//
// KuCoin explicitly documents the UTA WebSocket API as pre-release:
// "DO NOT use this API in production environments or live trading under
// any circumstances. The endpoint URLs, parameters, response structures,
// and overall behavior are subject to change without prior notice."
// (https://www.kucoin.com/docs-new/websocket-api/base-info/introduction-uta).
// This client exists for evaluation/development against that API as-is;
// treat it as unstable until KuCoin lifts that warning.
//
// The wire protocol is entirely different from Classic's (different
// envelope field names for subscribe/ack/welcome/push) — see
// websocket/classic for that one. Construct a Client with a token from
// uta/ws.GetPrivateToken for private channels; public channels need no
// token at all.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/introduction-uta
package uta

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Fixed UTA WebSocket hosts. Unlike Classic, the token response carries
// no instanceServers list — these hosts are documented constants.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/get-private-token-uta
const (
	PublicSpotWSURL    = "wss://x-push-spot.kucoin.com"
	PublicFuturesWSURL = "wss://x-push-futures.kucoin.com"
	PrivateWSURL       = "wss://wsapi-push.kucoin.com"
)

// Push is a single channel data push. T identifies the channel+product
// (e.g. "ticker.SPOT", "obu.FUTURES"); P is the gateway push timestamp in
// nanoseconds; Data is the raw per-channel payload — decode it yourself
// per the specific channel's documented shape (only a handful of UTA
// channels are implemented as typed models in this SDK so far).
type Push struct {
	T    string          `json:"T"`
	P    int64           `json:"P"`
	Data json.RawMessage `json:"d"`
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

func WithLogger(l Logger) Option {
	return func(c *Client) { c.logger = l }
}

// WithAutoReconnect toggles automatic reconnection with exponential
// backoff on unexpected disconnects. Enabled by default.
func WithAutoReconnect(enabled bool) Option {
	return func(c *Client) { c.autoReconnect = enabled }
}

const (
	// KuCoin documents UTA ping frequency must not exceed 1/sec and pongs
	// should arrive within ~3s; a much more relaxed default interval is
	// used here since a ping-per-second is only a ceiling, not a
	// requirement.
	defaultPingInterval = 15 * time.Second
	pongWaitTimeout     = 10 * time.Second
	welcomeWaitTimeout  = 10 * time.Second
	reconnectMin        = 1 * time.Second
	reconnectMax        = 60 * time.Second
	subBufferSize       = 256
)

type subscription struct {
	channel   string
	tradeType string
	symbol    string
	ch        chan Push
}

// Client is a reconnecting WebSocket client for one UTA WS host (public
// spot, public futures, or private — see the exported *WSURL constants).
type Client struct {
	host  string
	token string // empty for public channels

	pingInterval  time.Duration
	logger        Logger
	autoReconnect bool

	mu            sync.RWMutex
	writeMu       sync.Mutex
	conn          *websocket.Conn
	subscriptions map[string]*subscription
	ackWaiters    map[string]chan struct{}
	closed        bool
	done          chan struct{}
	welcomed      chan struct{}
}

// NewClient creates a Client for one UTA WS host. token is required for
// PrivateWSURL (from uta/ws.GetPrivateToken) and ignored for the public
// hosts.
func NewClient(host, token string, opts ...Option) *Client {
	c := &Client{
		host:          host,
		token:         token,
		pingInterval:  defaultPingInterval,
		logger:        noopLogger{},
		autoReconnect: true,
		subscriptions: make(map[string]*subscription),
		ackWaiters:    make(map[string]chan struct{}),
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

// Connect dials the host and blocks until KuCoin's welcome message
// arrives (or ctx is done / a timeout elapses).
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	c.closed = false
	c.done = make(chan struct{})
	c.welcomed = make(chan struct{})
	c.mu.Unlock()

	if err := c.dial(ctx); err != nil {
		return err
	}

	go c.readPump()
	go c.pingPump()

	select {
	case <-c.welcomed:
		c.logger.Info("kucoin: uta ws connected")
		return nil
	case <-time.After(welcomeWaitTimeout):
		return fmt.Errorf("kucoin: uta ws: timed out waiting for welcome message")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) dial(ctx context.Context) error {
	host, err := url.Parse(c.host)
	if err != nil {
		return fmt.Errorf("kucoin: uta ws: parse host: %w", err)
	}
	if c.token != "" {
		query := host.Query()
		query.Set("token", c.token)
		host.RawQuery = query.Encode()
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, host.String(), nil)
	if err != nil {
		return fmt.Errorf("kucoin: uta ws dial: %w", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(c.pingInterval + pongWaitTimeout))
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	return nil
}

// Subscribe subscribes to a channel (e.g. "ticker", "obu", "order",
// "orderAll") for a tradeType ("SPOT", "FUTURES", "UNIFIED") and optional
// symbol, returning a buffered channel of pushes. KuCoin's ack for this
// protocol is not consistently documented, so Subscribe registers the
// subscription and sends the request but does not block waiting for an
// ack — pushes (and any ack, if one arrives) are both delivered
// immediately once sent. The subscription is automatically restored
// after a reconnect.
func (c *Client) Subscribe(channel, tradeType, symbol string) (<-chan Push, error) {
	key := channel + ":" + tradeType + ":" + symbol
	c.mu.Lock()
	sub, exists := c.subscriptions[key]
	if !exists {
		sub = &subscription{channel: channel, tradeType: tradeType, symbol: symbol, ch: make(chan Push, subBufferSize)}
		c.subscriptions[key] = sub
	}
	c.mu.Unlock()

	req := map[string]any{
		"id":        randomID(),
		"action":    "subscribe",
		"channel":   channel,
		"tradeType": tradeType,
	}
	if symbol != "" {
		req["symbol"] = symbol
	}
	if err := c.writeJSON(req); err != nil {
		return nil, err
	}
	return sub.ch, nil
}

// Unsubscribe removes a channel subscription and closes its data
// channel.
func (c *Client) Unsubscribe(channel, tradeType, symbol string) error {
	key := channel + ":" + tradeType + ":" + symbol
	c.mu.Lock()
	sub, exists := c.subscriptions[key]
	if exists {
		delete(c.subscriptions, key)
	}
	c.mu.Unlock()

	if !exists {
		return nil
	}
	close(sub.ch)

	req := map[string]any{
		"id":        randomID(),
		"action":    "unsubscribe",
		"channel":   channel,
		"tradeType": tradeType,
	}
	if symbol != "" {
		req["symbol"] = symbol
	}
	return c.writeJSON(req)
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
	c.mu.Unlock()

	if done != nil {
		close(done)
	}
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (c *Client) writeJSON(v any) error {
	// gorilla/websocket permits only one concurrent writer per connection.
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("kucoin: uta ws: not connected")
	}
	return conn.WriteJSON(v)
}

func (c *Client) pingPump() {
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if err := c.writeJSON(map[string]string{"id": randomID(), "type": "ping"}); err != nil {
				c.logger.Warn("kucoin: uta ws ping failed", "error", err)
			}
		}
	}
}

func (c *Client) readPump() {
	for {
		c.mu.RLock()
		conn := c.conn
		closed := c.closed
		c.mu.RUnlock()
		if closed || conn == nil {
			return
		}

		_, raw, err := conn.ReadMessage()
		if err != nil {
			c.logger.Warn("kucoin: uta ws read error", "error", err)
			if c.autoReconnect && !c.isClosed() {
				go c.reconnectLoop()
			}
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(c.pingInterval + pongWaitTimeout))

		c.handleMessage(raw)
	}
}

func (c *Client) isClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

func (c *Client) handleMessage(raw []byte) {
	// Welcome: {"sessionId":"...","message":"welcome","pingInterval":30000}
	var welcome struct {
		SessionID    string `json:"sessionId"`
		Message      string `json:"message"`
		PingInterval int64  `json:"pingInterval"`
	}
	if err := json.Unmarshal(raw, &welcome); err == nil && welcome.Message == "welcome" {
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
	}

	// Pong / ack: {"id":"...","type":"pong","ts":...} or {"id":"...","result":"true"}
	var control struct {
		ID     string `json:"id"`
		Type   string `json:"type"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal(raw, &control); err == nil {
		if control.Type == "pong" {
			return
		}
		if control.Result != "" {
			c.mu.Lock()
			if ch, ok := c.ackWaiters[control.ID]; ok {
				delete(c.ackWaiters, control.ID)
				close(ch)
			}
			c.mu.Unlock()
			return
		}
	}

	var push Push
	if err := json.Unmarshal(raw, &push); err != nil || push.T == "" {
		c.logger.Warn("kucoin: uta ws decode push failed or unrecognized message", "raw", string(raw))
		return
	}

	c.dispatch(push)
}

// dispatch fans a push out to every subscription whose channel+tradeType
// prefix matches push.T (e.g. T="ticker.SPOT" matches a "ticker"/"SPOT"
// subscription regardless of which symbol was requested, since the UTA
// protocol doesn't echo the symbol back on the push envelope itself).
func (c *Client) dispatch(push Push) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, sub := range c.subscriptions {
		if push.T != sub.channel+"."+sub.tradeType {
			continue
		}
		select {
		case sub.ch <- push:
		default:
			c.logger.Warn("kucoin: uta ws subscriber channel full, dropping message", "T", push.T)
		}
	}
}

func (c *Client) reconnectLoop() {
	backoff := reconnectMin
	for {
		if c.isClosed() {
			return
		}
		time.Sleep(backoff)

		ctx, cancel := context.WithTimeout(context.Background(), welcomeWaitTimeout)
		err := c.Connect(ctx)
		cancel()
		if err != nil {
			c.logger.Warn("kucoin: uta ws reconnect failed", "error", err, "backoff", backoff)
			backoff *= 2
			if backoff > reconnectMax {
				backoff = reconnectMax
			}
			continue
		}

		c.resubscribeAll()
		c.logger.Info("kucoin: uta ws reconnected")
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
			"id":        randomID(),
			"action":    "subscribe",
			"channel":   sub.channel,
			"tradeType": sub.tradeType,
		}
		if sub.symbol != "" {
			req["symbol"] = sub.symbol
		}
		if err := c.writeJSON(req); err != nil {
			c.logger.Warn("kucoin: uta ws resubscribe failed", "channel", sub.channel, "error", err)
		}
	}
}
