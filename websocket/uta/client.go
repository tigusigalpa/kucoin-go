// Package uta implements a reconnecting WebSocket client for KuCoin's current
// UTA WebSocket v2 push API. It intentionally remains separate from
// websocket/classic: their authentication and message envelopes differ.
//
// Current private UTA v2 channels authenticate after welcome using an HMAC
// signature. Supply WithCredentials for that path. The token argument of
// NewClient remains only for source compatibility with the legacy token-based
// UTA API; do not use it for new private UTA v2 integrations.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/introduction
package uta

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tigusigalpa/kucoin-go/auth"
	"github.com/tigusigalpa/kucoin-go/transport"
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

var (
	// ErrAlreadyConnected is returned when Connect is called while a socket is
	// already active. One Client owns exactly one WebSocket connection.
	ErrAlreadyConnected = errors.New("kucoin: uta ws: already connected")
	// ErrReconnecting is returned when a caller tries to manually connect while
	// this Client is restoring an interrupted connection.
	ErrReconnecting = errors.New("kucoin: uta ws: reconnect in progress")
	// ErrAuthenticationFailed is returned when KuCoin rejects the explicit
	// private-channel authentication request.
	ErrAuthenticationFailed = errors.New("kucoin: uta ws: authentication failed")
	// ErrIncompleteCredentials is returned before writing an authentication
	// request when WithCredentials did not receive a complete key set.
	ErrIncompleteCredentials = errors.New("kucoin: uta ws: complete API credentials are required for authentication")
	// ErrSubscriptionFailed is returned when KuCoin negatively acknowledges a
	// UTA channel subscription.
	ErrSubscriptionFailed = errors.New("kucoin: uta ws: subscription failed")
)

// Push is a single channel data push. T identifies the channel+product
// (e.g. "ticker.SPOT", "obu.FUTURES"); P is the gateway push timestamp in
// nanoseconds; Data is the raw per-channel payload. Prefer a typed helper
// such as SubscribeTicker where one is available.
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

// WithLogger sets a structured logger for connection lifecycle events.
func WithLogger(l Logger) Option {
	return func(c *Client) { c.logger = l }
}

// WithAutoReconnect toggles automatic reconnection with exponential
// backoff on unexpected disconnects. Enabled by default.
func WithAutoReconnect(enabled bool) Option {
	return func(c *Client) { c.autoReconnect = enabled }
}

// WithCredentials enables current UTA v2 private-channel authentication.
// KuCoin requires API key, secret and passphrase after the server's welcome
// frame. The credentials are used only to build the auth frame and are never
// logged.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/introduction
func WithCredentials(credentials transport.Credentials) Option {
	return func(c *Client) { c.credentials = &credentials }
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
	tickerCh  chan Ticker
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
	ackWaiters    map[string]chan bool
	authWaiters   map[string]chan bool
	credentials   *transport.Credentials
	closed        bool
	reconnecting  bool
	shutdown      chan struct{}
	done          chan struct{}
	welcomed      chan struct{}
}

// NewClient creates a Client for one UTA WS host. token is retained for
// source compatibility with the legacy UTA API. Current private UTA v2
// channels should instead use WithCredentials.
func NewClient(host, token string, opts ...Option) *Client {
	c := &Client{
		host:          host,
		token:         token,
		pingInterval:  defaultPingInterval,
		logger:        noopLogger{},
		autoReconnect: true,
		subscriptions: make(map[string]*subscription),
		ackWaiters:    make(map[string]chan bool),
		authWaiters:   make(map[string]chan bool),
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

// Connect dials the host and blocks until KuCoin's welcome message
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

	_ = conn.SetReadDeadline(time.Now().Add(c.pingInterval + pongWaitTimeout))
	go c.readPump(conn, done)
	go c.pingPump(conn, done)

	select {
	case <-welcomed:
		if err := c.authenticate(ctx, conn); err != nil {
			c.disconnect(conn, done)
			return err
		}
		c.logger.Info("kucoin: uta ws connected")
		return nil
	case <-time.After(welcomeWaitTimeout):
		c.disconnect(conn, done)
		return fmt.Errorf("kucoin: uta ws: timed out waiting for welcome message")
	case <-ctx.Done():
		c.disconnect(conn, done)
		return ctx.Err()
	}
}

func (c *Client) authenticate(ctx context.Context, conn *websocket.Conn) error {
	c.mu.RLock()
	credentials := c.credentials
	done := c.done
	c.mu.RUnlock()
	if credentials == nil {
		return nil
	}
	if credentials.APIKey == "" || credentials.APISecret == "" || credentials.APIPassphrase == "" {
		return ErrIncompleteCredentials
	}

	timestamp := auth.TimestampMillis(time.Now())
	signer := auth.NewSigner(credentials.APISecret)
	id := randomID()
	ack := make(chan bool, 1)

	c.mu.Lock()
	if c.conn != conn || c.closed {
		c.mu.Unlock()
		return context.Canceled
	}
	c.authWaiters[id] = ack
	c.mu.Unlock()

	request := map[string]string{
		"id":                id,
		"op":                "auth",
		"kc-api-key":        credentials.APIKey,
		"kc-api-sign":       signer.Sign(timestamp, "POST", "/api/websocket/users/verify", ""),
		"kc-api-timestamp":  timestamp,
		"kc-api-passphrase": signer.SignPassphrase(credentials.APIPassphrase),
	}
	if err := c.writeJSONTo(conn, request); err != nil {
		c.removeAuthWaiter(id, ack)
		return err
	}

	timer := time.NewTimer(welcomeWaitTimeout)
	defer timer.Stop()
	select {
	case ok := <-ack:
		if !ok {
			return ErrAuthenticationFailed
		}
		return nil
	case <-ctx.Done():
		c.removeAuthWaiter(id, ack)
		return ctx.Err()
	case <-done:
		c.removeAuthWaiter(id, ack)
		return context.Canceled
	case <-timer.C:
		c.removeAuthWaiter(id, ack)
		return fmt.Errorf("kucoin: uta ws: timed out waiting for authentication")
	}
}

func (c *Client) removeAuthWaiter(id string, waiter chan bool) {
	c.mu.Lock()
	if c.authWaiters[id] == waiter {
		delete(c.authWaiters, id)
	}
	c.mu.Unlock()
}

func (c *Client) dial(ctx context.Context) (*websocket.Conn, error) {
	host, err := url.Parse(c.host)
	if err != nil {
		return nil, fmt.Errorf("kucoin: uta ws: parse host: %w", err)
	}
	if c.token != "" {
		query := host.Query()
		query.Set("token", c.token)
		host.RawQuery = query.Encode()
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, host.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("kucoin: uta ws dial: %w", err)
	}
	return conn, nil
}

// Subscribe subscribes to a channel (e.g. "ticker", "obu", "order",
// "orderAll") for a tradeType ("SPOT", "FUTURES", "UNIFIED") and optional
// symbol, returning a buffered channel of pushes. KuCoin's ack for this
// protocol acknowledges a successful subscription with result=true.
// Subscribe waits for that acknowledgement before returning its buffered
// channel. The subscription is automatically restored after a reconnect.
func (c *Client) Subscribe(channel, tradeType, symbol string) (<-chan Push, error) {
	key := channel + ":" + tradeType + ":" + symbol
	c.mu.Lock()
	sub, exists := c.subscriptions[key]
	if exists {
		c.mu.Unlock()
		return sub.ch, nil
	}
	sub = &subscription{channel: channel, tradeType: tradeType, symbol: symbol, ch: make(chan Push, subBufferSize)}
	c.subscriptions[key] = sub
	id := randomID()
	ack := make(chan bool, 1)
	c.ackWaiters[id] = ack
	done := c.done
	c.mu.Unlock()
	cleanup := func() {
		c.mu.Lock()
		if c.ackWaiters[id] == ack {
			delete(c.ackWaiters, id)
		}
		if c.subscriptions[key] == sub {
			delete(c.subscriptions, key)
			close(sub.ch)
			if sub.tickerCh != nil {
				close(sub.tickerCh)
			}
		}
		c.mu.Unlock()
	}

	req := map[string]any{
		"id":        id,
		"action":    "subscribe",
		"channel":   channel,
		"tradeType": tradeType,
	}
	if symbol != "" {
		req["symbol"] = symbol
	}
	if err := c.writeJSON(req); err != nil {
		cleanup()
		return nil, err
	}

	select {
	case ok := <-ack:
		if !ok {
			cleanup()
			return nil, ErrSubscriptionFailed
		}
		return sub.ch, nil
	case <-time.After(welcomeWaitTimeout):
		cleanup()
		return nil, fmt.Errorf("kucoin: uta ws: subscribe to %q timed out waiting for acknowledgement", channel)
	case <-done:
		cleanup()
		return nil, context.Canceled
	}
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
	if sub.tickerCh != nil {
		close(sub.tickerCh)
	}

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
	shutdown := c.shutdown
	c.conn = nil
	c.done = nil
	c.welcomed = nil
	subscriptions := c.subscriptions
	c.subscriptions = make(map[string]*subscription)
	c.ackWaiters = make(map[string]chan bool)
	c.authWaiters = make(map[string]chan bool)
	closeSignal(done)
	closeSignal(shutdown)
	c.mu.Unlock()

	for _, sub := range subscriptions {
		close(sub.ch)
		if sub.tickerCh != nil {
			close(sub.tickerCh)
		}
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
		return fmt.Errorf("kucoin: uta ws: not connected")
	}
	// gorilla/websocket permits only one concurrent writer per connection.
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.mu.RLock()
	current := c.conn
	closed := c.closed
	c.mu.RUnlock()
	if closed || current != conn {
		return fmt.Errorf("kucoin: uta ws: not connected")
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
			if err := c.writeJSONTo(conn, map[string]string{"id": randomID(), "op": "ping", "timestamp": strconv.FormatInt(time.Now().UnixMilli(), 10)}); err != nil {
				c.logger.Warn("kucoin: uta ws ping failed", "error", err)
			}
		}
	}
}

func controlResultOK(raw json.RawMessage) bool {
	var boolean bool
	if err := json.Unmarshal(raw, &boolean); err == nil {
		return boolean
	}
	var text string
	return json.Unmarshal(raw, &text) == nil && text == "true"
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
			c.logger.Warn("kucoin: uta ws read error", "error", err)
			if c.disconnect(conn, done) && c.autoReconnect {
				c.startReconnect()
			}
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(c.pingInterval + pongWaitTimeout))

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
// they can complete a new welcome/authentication handshake or reach a current
// subscription. The nil form keeps the package-level decoder testable without
// a network connection.
func (c *Client) handleMessageForConnection(conn *websocket.Conn, done chan struct{}, raw []byte) {
	if conn != nil {
		c.mu.RLock()
		active := c.conn == conn && c.done == done
		c.mu.RUnlock()
		if !active {
			return
		}
	}

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

	// Pong / ack: {"id":"...","op":"pong"} or
	// {"id":"...","result":true}. The API documentation has used both
	// boolean and string result forms, so accept both safely.
	var control struct {
		ID     string          `json:"id"`
		Type   string          `json:"type"`
		Op     string          `json:"op"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(raw, &control); err == nil {
		if control.Type == "pong" || control.Op == "pong" {
			return
		}
		if len(control.Result) != 0 {
			ok := controlResultOK(control.Result)
			c.mu.Lock()
			if ch, exists := c.authWaiters[control.ID]; exists {
				delete(c.authWaiters, control.ID)
				ch <- ok
			}
			if ch, exists := c.ackWaiters[control.ID]; exists {
				delete(c.ackWaiters, control.ID)
				ch <- ok
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
		if sub.tickerCh != nil {
			ticker, err := DecodeTicker(push)
			if err != nil {
				c.logger.Warn("kucoin: uta ws decode ticker push failed", "error", err)
				continue
			}
			select {
			case sub.tickerCh <- ticker:
			default:
				c.logger.Warn("kucoin: uta ws typed ticker channel full, dropping message", "symbol", ticker.Symbol)
			}
		}
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

		ctx, cancel := context.WithTimeout(context.Background(), welcomeWaitTimeout)
		err := c.connect(ctx, true)
		cancel()
		if err != nil {
			if c.isClosed() {
				return
			}
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
