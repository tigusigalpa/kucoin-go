// Package classic implements a managed WebSocket client for KuCoin's Classic
// (Spot/Margin/Futures) WebSocket API: connection lifecycle, heartbeats,
// reconnection with a fresh token, resubscription and topic routing.
//
// A Client owns exactly one connection. Build it either with a fixed endpoint
// and token (NewClient — the token expires after 24 hours, so a long-lived
// client should not use this form) or with a TokenSource (NewClientWithTokenSource),
// which is asked for a new token and endpoint list for every (re)connection.
// The Futures and Spot/Margin typed streaming packages build on this client and
// are what applications should normally use; this package is the low-level
// layer for raw topics and for channels KuCoin adds after this SDK was
// released.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/introduction
package classic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	classicws "github.com/tigusigalpa/kucoin-go/classic/ws"
	"github.com/tigusigalpa/kucoin-go/internal/wsengine"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/types"
)

// Message is one push received on a subscribed topic.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/introduction
type Message struct {
	// ID is the frame ID, present on some pushes.
	ID types.ID `json:"id,omitempty"`
	// Type is "message" for data pushes.
	Type string `json:"type"`
	// Topic is the topic of the single symbol the push belongs to, which may
	// be narrower than the topic that was subscribed (multi-symbol topics).
	Topic string `json:"topic"`
	// Subject names the kind of update within the topic.
	Subject string `json:"subject"`
	// Sn is the sequence number some topics attach to the envelope (Futures
	// level-2 and ticker). It is zero when absent.
	Sn          types.Int64 `json:"sn,omitempty"`
	UserID      string      `json:"userId,omitempty"`
	ChannelType string      `json:"channelType,omitempty"`
	// Data is the raw topic payload. Typed streaming packages decode it for
	// you; decode it yourself only for topics they do not cover.
	Data json.RawMessage `json:"data"`
}

// Logger is a minimal structured-logging interface; it is the same type as
// stream.Logger.
type Logger = stream.Logger

// Sentinel errors, shared with every other WebSocket client of this module so
// errors.Is works across them.
var (
	// ErrAlreadyConnected is returned when Connect is called while a socket is
	// already active. One Client owns exactly one WebSocket connection.
	ErrAlreadyConnected = stream.ErrAlreadyConnected
	// ErrReconnecting is returned when a caller tries to manually connect while
	// this Client is restoring an interrupted connection.
	ErrReconnecting = stream.ErrReconnecting
)

// TokenSource supplies the connection token and instance servers. It is called
// before every connection attempt, so implementations must return a fresh
// token (the REST bullet-token calls do). Wrap an error with stream.Permanent
// when retrying cannot help, for example missing credentials for a private
// token.
type TokenSource interface {
	GetToken(ctx context.Context) (*classicws.Token, error)
}

// TokenSourceFunc adapts a function to TokenSource, for example
// TokenSourceFunc(client.Classic.Futures.Ws.GetPublicToken).
type TokenSourceFunc func(ctx context.Context) (*classicws.Token, error)

// GetToken implements TokenSource.
func (f TokenSourceFunc) GetToken(ctx context.Context) (*classicws.Token, error) { return f(ctx) }

// Option configures a Client at construction time.
type Option func(*Client)

// WithLogger sets a structured logger for connection lifecycle events.
func WithLogger(l Logger) Option {
	return func(c *Client) { c.cfg.Logger = l }
}

// WithAutoReconnect toggles automatic reconnection with exponential backoff on
// unexpected disconnects. Enabled by default.
func WithAutoReconnect(enabled bool) Option {
	return func(c *Client) { c.cfg.Reconnect.Disabled = !enabled }
}

// WithPingInterval overrides the ping cadence. When zero, the interval KuCoin
// advertises in the token response is used (halved, as a safety margin).
func WithPingInterval(d time.Duration) Option {
	return func(c *Client) { c.cfg.PingInterval = d }
}

// WithPingTimeout overrides how long the client waits for any sign of life
// after a ping. When zero, the timeout KuCoin advertises is used.
func WithPingTimeout(d time.Duration) Option {
	return func(c *Client) { c.cfg.PingTimeout = d }
}

// WithStreamOptions applies the shared connection options of package stream
// (reconnect policy, timeouts, buffer sizes, event handler, dialer, ...).
func WithStreamOptions(opts ...stream.Option) Option {
	return func(c *Client) {
		for _, opt := range opts {
			if opt != nil {
				opt(&c.cfg)
			}
		}
	}
}

// Client is a reconnecting WebSocket client for one Classic WS endpoint
// (Spot/Margin or Futures — both share the same wire protocol; only the
// host/token differ). It is safe for concurrent use.
type Client struct {
	cfg   stream.Config
	proto *protocol
	conn  *wsengine.Conn

	// raw holds the subscriptions made through Subscribe, one per topic.
	raw wsengine.Shared[Message]
}

// NewClient creates a Client for a single Classic WS instance server. endpoint
// and token normally come straight from a bullet-token REST call's
// InstanceServers[0].Endpoint / Token fields.
//
// KuCoin tokens are valid for 24 hours and the server closes the connection
// after that, so a client built with a fixed token cannot reconnect once the
// token expired. Prefer NewClientWithTokenSource for anything long-lived.
func NewClient(endpoint, token string, opts ...Option) *Client {
	return newClient(&protocol{endpoint: endpoint, token: token}, opts)
}

// NewClientWithTokenSource creates a Client that asks src for a fresh token and
// instance-server list before every connection attempt, including every
// reconnect. The ping interval and timeout advertised by the token response are
// honoured.
func NewClientWithTokenSource(src TokenSource, opts ...Option) *Client {
	return newClient(&protocol{source: src}, opts)
}

func newClient(p *protocol, opts []Option) *Client {
	c := &Client{proto: p}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	c.cfg = c.cfg.WithDefaults()
	c.conn = wsengine.New(p, c.cfg)
	return c
}

// Connect dials the endpoint and blocks until KuCoin's welcome message arrives
// (or ctx is done / the connect timeout elapses).
func (c *Client) Connect(ctx context.Context) error { return c.conn.Connect(ctx) }

// Close terminates the connection, ends every subscription and waits for the
// client's goroutines to exit. Safe to call multiple times.
func (c *Client) Close() error { return c.conn.Close() }

// Shutdown is Close with a caller-supplied deadline.
func (c *Client) Shutdown(ctx context.Context) error { return c.conn.Shutdown(ctx) }

// State returns the connection lifecycle state.
func (c *Client) State() stream.State { return c.conn.State() }

// Events returns the lifecycle event channel (see stream.Event). It is closed
// when the client closes.
func (c *Client) Events() <-chan stream.Event { return c.conn.Events() }

// Stats returns a snapshot of the connection counters.
func (c *Client) Stats() stream.Stats { return c.conn.Stats() }

// Done is closed when the client is closed, by Close or by a fatal error.
func (c *Client) Done() <-chan struct{} { return c.conn.Done() }

// Err returns the fatal error that closed the client, or nil.
func (c *Client) Err() error { return c.conn.Err() }

// ReportDecodeError records a push that a typed decoder rejected; the error is
// counted in Stats and published as a stream.EventDecodeError.
func (c *Client) ReportDecodeError(err error) { c.conn.ReportDecodeError(err) }

// Subscribe subscribes to a topic (e.g. "/market/ticker:BTC-USDT") and returns a
// channel of raw pushes. It blocks until KuCoin acknowledges the subscription
// and returns a typed error (*stream.ServerError, matching for example
// stream.ErrTopicNotFound) when KuCoin rejects it. The subscription is restored
// automatically after a reconnect. The channel is closed by Unsubscribe, by
// Close and when the subscription fails.
//
// Subscribing a topic that is already subscribed through Subscribe returns the
// existing channel, also when several goroutines subscribe it at the same time.
// Applications that want typed payloads should use the streaming packages (for
// example classic/futures/streaming) instead.
func (c *Client) Subscribe(ctx context.Context, topic string, privateChannel bool) (<-chan Message, error) {
	sub, err := c.raw.Get(ctx, topic, func() (*stream.Subscription[Message], error) {
		return SubscribeTyped(ctx, c, topic, privateChannel, func(m *Message) (Message, bool, error) { return *m, true, nil })
	})
	if err != nil {
		return nil, err
	}
	return sub.C(), nil
}

// Unsubscribe removes a topic subscription and closes its data channel. It is a
// no-op for a topic that is not subscribed.
func (c *Client) Unsubscribe(topic string) error {
	if sub := c.raw.Remove(topic); sub != nil {
		return sub.Close()
	}
	return c.conn.UnsubscribeName(topic)
}

// DecodeFunc converts a push into a typed update. Return ok=false to skip a push
// that carries no update and an error for a malformed payload (it is reported
// and skipped; the subscription continues).
type DecodeFunc[T any] func(m *Message) (v T, ok bool, err error)

// SubscribeTyped subscribes to topic and delivers every push, decoded by
// decode, on the returned stream.Subscription. It is the building block of the
// typed streaming packages and the supported way to consume a channel this SDK
// does not know yet.
//
// topic may list several symbols after the colon, separated by commas; each
// push is routed by its own single-symbol topic.
func SubscribeTyped[T any](ctx context.Context, c *Client, topic string, privateChannel bool, decode DecodeFunc[T], opts ...stream.SubscribeOption) (*stream.Subscription[T], error) {
	sc := stream.NewSubscribeConfig(opts...)
	var handle atomic.Pointer[wsengine.SubHandle]
	sub := stream.NewSubscription[T](topic, 0, func() error {
		if h := handle.Load(); h != nil {
			return h.Close()
		}
		return nil
	})
	decoder := func(f stream.Frame) (T, bool, error) {
		msg, ok := f.Msg.(*Message)
		if !ok {
			var zero T
			return zero, false, nil
		}
		return decode(msg)
	}
	h, err := c.conn.Subscribe(ctx, wsengine.Spec{
		Name:        topic,
		Routes:      routesFor(topic),
		Subscribe:   subscribeFrame(topic, privateChannel),
		Unsubscribe: unsubscribeFrame(topic, privateChannel),
		Handler:     stream.NewHandler(sub, decoder, c.conn.ReportDecodeError),
		Buffer:      sc.Buffer,
		Overflow:    sc.Overflow,
		OverflowSet: sc.OverflowSet,
	})
	if err != nil {
		return nil, err
	}
	handle.Store(h)
	sub.BindDropped(h.Dropped)
	return sub, nil
}

// SubscribeHandler subscribes to topic and feeds every update to h, in order,
// from a single goroutine. It is for consumers that need more than a stream of
// decoded values, such as order-book maintenance, which must react to gaps and
// reconnects. Closing the returned handle unsubscribes.
func (c *Client) SubscribeHandler(ctx context.Context, topic string, privateChannel bool, h stream.Handler, opts ...stream.SubscribeOption) (*Handle, error) {
	if h == nil {
		return nil, errors.New("kucoin: classic ws: SubscribeHandler needs a handler")
	}
	sc := stream.NewSubscribeConfig(opts...)
	sh, err := c.conn.Subscribe(ctx, wsengine.Spec{
		Name:        topic,
		Routes:      routesFor(topic),
		Subscribe:   subscribeFrame(topic, privateChannel),
		Unsubscribe: unsubscribeFrame(topic, privateChannel),
		Handler:     h,
		Buffer:      sc.Buffer,
		Overflow:    sc.Overflow,
		OverflowSet: sc.OverflowSet,
	})
	if err != nil {
		return nil, err
	}
	return &Handle{h: sh}, nil
}

// Handle is a live subscription created by SubscribeHandler.
type Handle struct{ h *wsengine.SubHandle }

// Topic returns the subscribed topic.
func (h *Handle) Topic() string { return h.h.Name() }

// Dropped returns how many updates the overflow policy discarded for this
// subscription so far.
func (h *Handle) Dropped() uint64 { return h.h.Dropped() }

// Close unsubscribes. It is idempotent.
func (h *Handle) Close() error { return h.h.Close() }

// String describes the client for logs.
func (c *Client) String() string { return fmt.Sprintf("classic.Client(%s)", c.conn.State()) }
