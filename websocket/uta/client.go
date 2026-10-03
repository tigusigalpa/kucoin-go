// Package uta implements a managed WebSocket client for KuCoin's current UTA
// WebSocket v2 push API: connection lifecycle, heartbeats, signed
// authentication of private connections, reconnection, resubscription and
// routing of pushes to subscriptions by channel, symbol, depth and interval.
// It intentionally remains separate from websocket/classic: their
// authentication and message envelopes differ.
//
// Current private UTA v2 channels authenticate after the welcome frame with an
// HMAC signature. Supply WithCredentials for that path. The token argument of
// NewClient remains only for source compatibility with the legacy token-based
// UTA API; do not use it for new private UTA v2 integrations.
//
// The typed streaming package uta/v2/streaming builds on this client and is what
// applications should normally use; this package is the low-level layer for raw
// pushes and for channels KuCoin adds after this SDK was released.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/introduction
package uta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/tigusigalpa/kucoin-go/internal/wsengine"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
)

// Fixed UTA WebSocket hosts. Unlike Classic, no token request is needed: these
// hosts are documented constants.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/introduction
const (
	PublicSpotWSURL    = "wss://x-push-spot.kucoin.com"
	PublicFuturesWSURL = "wss://x-push-futures.kucoin.com"
	PrivateWSURL       = "wss://wsapi-push.kucoin.com"
)

// Sentinel errors, shared with every other WebSocket client of this module so
// errors.Is works across them.
var (
	// ErrAlreadyConnected is returned when Connect is called while a socket is
	// already active. One Client owns exactly one WebSocket connection.
	ErrAlreadyConnected = stream.ErrAlreadyConnected
	// ErrReconnecting is returned when a caller tries to manually connect while
	// this Client is restoring an interrupted connection.
	ErrReconnecting = stream.ErrReconnecting
	// ErrAuthenticationFailed is returned when KuCoin rejects the explicit
	// private-channel authentication request.
	ErrAuthenticationFailed = stream.ErrAuthFailed
	// ErrIncompleteCredentials is returned before writing an authentication
	// request when WithCredentials did not receive a complete key set.
	ErrIncompleteCredentials = stream.ErrIncompleteCredentials
	// ErrSubscriptionFailed is returned (wrapping the *stream.ServerError with
	// KuCoin's reason) when KuCoin negatively acknowledges a subscription.
	ErrSubscriptionFailed = errors.New("kucoin: uta ws: subscription failed")
)

// Push is a single channel data push. T identifies the channel and product
// (e.g. "ticker.SPOT", "obu.FUTURES") or, for a few channels, just the channel
// ("mark-price"); P is the gateway push timestamp in nanoseconds; Data is the raw
// per-channel payload. Prefer the typed streams of uta/v2/streaming where one
// exists.
type Push struct {
	T string `json:"T"`
	P int64  `json:"P"`
	// Kind is "snapshot" or "delta" on order-book pushes.
	Kind string `json:"t,omitempty"`
	// Depth is the order-book depth the push belongs to ("1", "5", "50",
	// "increment", "increment@10ms").
	Depth string          `json:"dp,omitempty"`
	Data  json.RawMessage `json:"d"`
}

// Logger is a minimal structured-logging interface; it is the same type as
// stream.Logger.
type Logger = stream.Logger

// Option configures a Client at construction time.
type Option func(*Client)

// WithLogger sets a structured logger for connection lifecycle events.
func WithLogger(l Logger) Option { return func(c *Client) { c.cfg.Logger = l } }

// WithAutoReconnect toggles automatic reconnection with exponential backoff on
// unexpected disconnects. Enabled by default.
func WithAutoReconnect(enabled bool) Option {
	return func(c *Client) { c.cfg.Reconnect.Disabled = !enabled }
}

// WithCredentials enables current UTA v2 private-channel authentication. KuCoin
// requires API key, secret and passphrase after the server's welcome frame. The
// credentials are used only to build the auth frame and are never logged. The
// connection re-authenticates on every reconnect.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/introduction
func WithCredentials(credentials transport.Credentials) Option {
	return func(c *Client) { c.proto.creds = &credentials }
}

// WithClock sets the clock used for the authentication timestamp (for example a
// clock corrected against KuCoin's server time).
func WithClock(clock transport.Clock) Option { return func(c *Client) { c.proto.clock = clock } }

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

// Client is a reconnecting WebSocket client for one UTA WS host (public spot,
// public futures, or private — see the exported *WSURL constants). It is safe
// for concurrent use.
type Client struct {
	cfg   stream.Config
	proto *protocol
	conn  *wsengine.Conn

	// raw holds the subscriptions made through SubscribeSpec, one per channel
	// description; ticker those made through SubscribeTicker.
	raw    wsengine.Shared[Push]
	ticker wsengine.Shared[Ticker]
}

// NewClient creates a Client for one UTA WS host. token is retained for source
// compatibility with the legacy UTA API. Current private UTA v2 channels should
// instead use WithCredentials.
func NewClient(host, token string, opts ...Option) *Client {
	c := &Client{
		proto: &protocol{host: host, token: token},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	c.cfg = c.cfg.WithDefaults()
	c.conn = wsengine.New(c.proto, c.cfg)
	return c
}

// Connect dials the host and blocks until KuCoin's welcome message arrives and,
// for a client with credentials, authentication succeeded (or ctx is done / the
// connect timeout elapses).
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

// Subscribe subscribes to a channel (e.g. "ticker", "obu", "order", "orderAll")
// for a tradeType ("SPOT", "FUTURES", "UNIFIED") and optional symbol, returning
// a channel of raw pushes. It waits for KuCoin's acknowledgement; a rejection is
// returned wrapping both ErrSubscriptionFailed and the *stream.ServerError with
// KuCoin's reason. The subscription is restored automatically after a
// reconnect.
//
// Only pushes for the requested symbol are delivered. Channels that need more
// parameters (kline interval, order-book depth, several symbols) are
// subscribed with SubscribeSpec; typed payloads come from uta/v2/streaming.
func (c *Client) Subscribe(channel, tradeType, symbol string) (<-chan Push, error) {
	spec := SubscribeSpec{Channel: channel, TradeType: tradeType}
	if symbol != "" {
		spec.Symbols = []string{symbol}
	}
	return c.SubscribeSpec(context.Background(), spec)
}

// SubscribeSpec subscribes to the channel described by spec and returns a
// channel of raw pushes; see Subscribe for the semantics. Subscribing a spec
// that is already subscribed through SubscribeSpec returns the existing
// channel, also when several goroutines subscribe it at the same time.
func (c *Client) SubscribeSpec(ctx context.Context, spec SubscribeSpec) (<-chan Push, error) {
	sub, err := c.raw.Get(ctx, spec.Name(), func() (*stream.Subscription[Push], error) {
		return SubscribeTyped(ctx, c, spec, func(p *Push) (Push, bool, error) { return *p, true, nil })
	})
	if err != nil {
		return nil, err
	}
	return sub.C(), nil
}

// Unsubscribe removes a channel subscription and closes its data channel. It is
// a no-op for a subscription that does not exist.
func (c *Client) Unsubscribe(channel, tradeType, symbol string) error {
	spec := SubscribeSpec{Channel: channel, TradeType: tradeType}
	if symbol != "" {
		spec.Symbols = []string{symbol}
	}
	return c.UnsubscribeSpec(spec)
}

// UnsubscribeSpec removes the subscription described by spec.
func (c *Client) UnsubscribeSpec(spec SubscribeSpec) error {
	name := spec.Name()
	if sub := c.raw.Remove(name); sub != nil {
		return sub.Close()
	}
	return c.conn.UnsubscribeName(name)
}

// DecodeFunc converts a push into a typed update. Return ok=false to skip a push
// that carries no update and an error for a malformed payload (it is reported
// and skipped; the subscription continues).
type DecodeFunc[T any] func(p *Push) (v T, ok bool, err error)

// SubscribeTyped subscribes to the channel described by spec and delivers every
// push, decoded by decode, on the returned stream.Subscription. It is the
// building block of the typed streaming packages and the supported way to consume
// a channel this SDK does not know yet.
func SubscribeTyped[T any](ctx context.Context, c *Client, spec SubscribeSpec, decode DecodeFunc[T], opts ...stream.SubscribeOption) (*stream.Subscription[T], error) {
	sc := stream.NewSubscribeConfig(opts...)
	var handle atomic.Pointer[wsengine.SubHandle]
	sub := stream.NewSubscription[T](spec.Name(), 0, func() error {
		if h := handle.Load(); h != nil {
			return h.Close()
		}
		return nil
	})
	decoder := func(f stream.Frame) (T, bool, error) {
		push, ok := f.Msg.(*Push)
		if !ok {
			var zero T
			return zero, false, nil
		}
		return decode(push)
	}
	h, err := c.conn.Subscribe(ctx, c.engineSpec(spec, stream.NewHandler(sub, decoder, c.conn.ReportDecodeError), sc))
	if err != nil {
		return nil, wrapSubscribeError(err)
	}
	handle.Store(h)
	sub.BindDropped(h.Dropped)
	return sub, nil
}

// SubscribeHandler subscribes to the channel described by spec and feeds every
// update to h, in order, from a single goroutine. It is for consumers that need
// more than a stream of decoded values, such as order-book maintenance, which
// must react to gaps and reconnects. Closing the returned handle unsubscribes.
func (c *Client) SubscribeHandler(ctx context.Context, spec SubscribeSpec, h stream.Handler, opts ...stream.SubscribeOption) (*Handle, error) {
	if h == nil {
		return nil, errors.New("kucoin: uta ws: SubscribeHandler needs a handler")
	}
	sh, err := c.conn.Subscribe(ctx, c.engineSpec(spec, h, stream.NewSubscribeConfig(opts...)))
	if err != nil {
		return nil, wrapSubscribeError(err)
	}
	return &Handle{h: sh}, nil
}

func (c *Client) engineSpec(spec SubscribeSpec, h stream.Handler, sc stream.SubscribeConfig) wsengine.Spec {
	return wsengine.Spec{
		Name:        spec.Name(),
		Routes:      spec.routes(),
		Subscribe:   func(id string) []byte { return spec.frame("subscribe", id) },
		Unsubscribe: func(id string) []byte { return spec.frame("unsubscribe", id) },
		Handler:     h,
		Buffer:      sc.Buffer,
		Overflow:    sc.Overflow,
		OverflowSet: sc.OverflowSet,
	}
}

// wrapSubscribeError makes a rejection by KuCoin match ErrSubscriptionFailed
// while keeping the *stream.ServerError.
func wrapSubscribeError(err error) error {
	var se *stream.ServerError
	if errors.As(err, &se) {
		return fmt.Errorf("%w: %w", ErrSubscriptionFailed, err)
	}
	return err
}

// Handle is a live subscription created by SubscribeHandler.
type Handle struct{ h *wsengine.SubHandle }

// Name returns the subscription identity.
func (h *Handle) Name() string { return h.h.Name() }

// Dropped returns how many updates the overflow policy discarded for this
// subscription so far.
func (h *Handle) Dropped() uint64 { return h.h.Dropped() }

// Close unsubscribes. It is idempotent.
func (h *Handle) Close() error { return h.h.Close() }
