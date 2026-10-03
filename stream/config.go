package stream

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Logger is a minimal structured-logging interface. Implementations must
// never log credentials. It has the same shape as the Logger types of the
// REST transport, so one adapter serves both.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// NopLogger discards every message.
type NopLogger struct{}

// Debug discards a debug message.
func (NopLogger) Debug(string, ...any) {}

// Info discards an informational message.
func (NopLogger) Info(string, ...any) {}

// Warn discards a warning.
func (NopLogger) Warn(string, ...any) {}

// Error discards an error message.
func (NopLogger) Error(string, ...any) {}

// OverflowPolicy decides what happens when a subscription's bounded queue is
// full because its consumer is slower than the market.
type OverflowPolicy int

const (
	// DropOldest discards the oldest queued update to make room, so the
	// consumer always catches up to the newest data. Every drop is counted in
	// Subscription.Dropped and reported to sequenced streams as a gap. This is
	// the default.
	DropOldest OverflowPolicy = iota
	// DropNewest discards the incoming update and keeps the queued backlog.
	DropNewest
	// FailSubscription ends the subscription with ErrSlowConsumer as soon as
	// its queue overflows, for consumers that must never silently lose data.
	FailSubscription
)

// String returns the policy name.
func (p OverflowPolicy) String() string {
	switch p {
	case DropOldest:
		return "drop_oldest"
	case DropNewest:
		return "drop_newest"
	case FailSubscription:
		return "fail_subscription"
	default:
		return "unknown"
	}
}

// ReconnectPolicy controls automatic reconnection after an unexpected
// disconnect. The delay before attempt n is
// min(MaxDelay, MinDelay*Factor^(n-1)) reduced by up to Jitter of itself, so
// that many clients dropped at the same moment (for example by KuCoin's load
// balancer) do not reconnect in lockstep.
type ReconnectPolicy struct {
	// Disabled turns automatic reconnection off; a lost connection then ends
	// the client with the cause as its error.
	Disabled bool
	// MinDelay is the delay bound before the first attempt. Default 500ms.
	MinDelay time.Duration
	// MaxDelay caps the delay. Default 60s.
	MaxDelay time.Duration
	// Factor multiplies the delay after every failed attempt. Default 2.
	Factor float64
	// Jitter is the fraction of each delay that is randomised, in [0, 1].
	// Default 0.5.
	Jitter float64
	// StableAfter is how long a connection must stay up before the attempt
	// counter and delay start over. Default 30s.
	StableAfter time.Duration
	// MaxAttempts gives up after this many consecutive failed attempts and
	// ends the client with the last error. Zero means retry forever.
	MaxAttempts int
}

// MessageLimit tunes how a connection paces the requests it sends — subscribe,
// unsubscribe and authentication messages — so that it stays inside KuCoin's limit
// on client messages. Going over that limit can get the connection dropped, and a
// restore after a reconnect, which subscribes everything again at once, is what
// exceeds it most easily; the connection therefore waits instead.
//
// The zero value keeps the limit KuCoin publishes for the dialect: 100 messages
// per 10 seconds for Classic, 300 (public) or 100 (private) for UTA. A tenth of
// it stays in reserve for the heartbeats.
//
// Docs: https://www.kucoin.com/docs-new/rate-limit-rule-classic and
// https://www.kucoin.com/docs-new/rate-limit-rule-uta
type MessageLimit struct {
	// Messages per Window. Zero (with a zero Window) keeps KuCoin's limit.
	Messages int
	Window   time.Duration
	// Unlimited turns the pacing off.
	Unlimited bool
}

// Config holds every tunable of a managed WebSocket connection. The zero
// value is valid: unset fields take the documented defaults. Build one with
// the functional options.
type Config struct {
	// Logger receives lifecycle diagnostics. Default: NopLogger.
	Logger Logger
	// Reconnect configures automatic reconnection (enabled by default).
	Reconnect ReconnectPolicy
	// InitialConnectAttempts is how many times the first connection is tried
	// before Connect gives up. Default 1 (no retry).
	InitialConnectAttempts int
	// ConnectTimeout bounds token acquisition, dialling, the welcome message
	// and authentication of one connection attempt. Default 15s.
	ConnectTimeout time.Duration
	// AckTimeout bounds the wait for the reply to a subscribe, unsubscribe or
	// authentication request. Default 10s.
	AckTimeout time.Duration
	// WriteTimeout bounds one frame write. A write that exceeds it kills the
	// connection. Default 10s.
	WriteTimeout time.Duration
	// ReadLimit is the maximum accepted frame size in bytes. Default 8 MiB.
	ReadLimit int64
	// PingInterval and PingTimeout override the heartbeat parameters that
	// KuCoin advertises (18s / 10s today). Zero keeps KuCoin's values. KuCoin
	// drops a connection that pings more than once per second.
	PingInterval time.Duration
	PingTimeout  time.Duration
	// MessageLimit tunes the pacing of subscribe, unsubscribe and authentication
	// messages; see MessageLimit. The zero value keeps KuCoin's published limit.
	MessageLimit MessageLimit
	// BufferSize is the default capacity of each subscription queue.
	// Default 1024.
	BufferSize int
	// Overflow is the default OverflowPolicy of new subscriptions.
	Overflow OverflowPolicy
	// OnEvent, if set, receives lifecycle events from a dedicated goroutine
	// (a slow handler can lose events but never stalls the connection). When
	// set, the Events channel of the client is closed and unused.
	OnEvent func(Event)
	// EventBuffer is the capacity of the Events channel. When it is full the
	// oldest event is discarded. Default 128.
	EventBuffer int
	// Dialer opens the WebSocket. Default: a copy of the gorilla default
	// dialer with proxy support and a 15s handshake timeout. Inject your own
	// to set a proxy, TLS configuration or custom resolver.
	Dialer *websocket.Dialer
	// Header is sent with the WebSocket handshake request.
	Header http.Header
}

// Option mutates a Config.
type Option func(*Config)

// WithLogger sets the logger for lifecycle diagnostics.
func WithLogger(l Logger) Option { return func(c *Config) { c.Logger = l } }

// WithReconnect replaces the reconnect policy.
func WithReconnect(p ReconnectPolicy) Option { return func(c *Config) { c.Reconnect = p } }

// WithAutoReconnect enables or disables automatic reconnection while keeping
// the other reconnect settings.
func WithAutoReconnect(enabled bool) Option {
	return func(c *Config) { c.Reconnect.Disabled = !enabled }
}

// WithInitialConnectAttempts retries the first connection up to n times, using
// the reconnect backoff between attempts, before Connect returns an error.
func WithInitialConnectAttempts(n int) Option {
	return func(c *Config) { c.InitialConnectAttempts = n }
}

// WithConnectTimeout bounds one connection attempt.
func WithConnectTimeout(d time.Duration) Option { return func(c *Config) { c.ConnectTimeout = d } }

// WithAckTimeout bounds the wait for a subscribe, unsubscribe or auth reply.
func WithAckTimeout(d time.Duration) Option { return func(c *Config) { c.AckTimeout = d } }

// WithWriteTimeout bounds one frame write.
func WithWriteTimeout(d time.Duration) Option { return func(c *Config) { c.WriteTimeout = d } }

// WithReadLimit caps the accepted frame size in bytes.
func WithReadLimit(n int64) Option { return func(c *Config) { c.ReadLimit = n } }

// WithPingInterval overrides KuCoin's recommended ping interval.
func WithPingInterval(d time.Duration) Option { return func(c *Config) { c.PingInterval = d } }

// WithPingTimeout overrides KuCoin's recommended pong timeout.
func WithPingTimeout(d time.Duration) Option { return func(c *Config) { c.PingTimeout = d } }

// WithMessageLimit paces the connection's requests to at most messages per
// window instead of KuCoin's published limit; see MessageLimit.
func WithMessageLimit(messages int, window time.Duration) Option {
	return func(c *Config) { c.MessageLimit = MessageLimit{Messages: messages, Window: window} }
}

// WithoutMessagePacing turns the pacing of the connection's requests off. KuCoin
// may drop a connection that sends more messages than its limit allows.
func WithoutMessagePacing() Option {
	return func(c *Config) { c.MessageLimit = MessageLimit{Unlimited: true} }
}

// WithBufferSize sets the default capacity of each subscription queue.
func WithBufferSize(n int) Option { return func(c *Config) { c.BufferSize = n } }

// WithOverflowPolicy sets the default overflow policy of new subscriptions.
func WithOverflowPolicy(p OverflowPolicy) Option { return func(c *Config) { c.Overflow = p } }

// WithEventHandler delivers lifecycle events to fn from a dedicated goroutine.
func WithEventHandler(fn func(Event)) Option { return func(c *Config) { c.OnEvent = fn } }

// WithEventBuffer sets the capacity of the Events channel.
func WithEventBuffer(n int) Option { return func(c *Config) { c.EventBuffer = n } }

// WithDialer injects the WebSocket dialer (proxy, TLS, resolver).
func WithDialer(d *websocket.Dialer) Option { return func(c *Config) { c.Dialer = d } }

// WithHeader sets headers sent with the WebSocket handshake.
func WithHeader(h http.Header) Option { return func(c *Config) { c.Header = h } }

// WithConfig replaces the whole Config; options applied after it still take
// effect.
func WithConfig(cfg Config) Option { return func(c *Config) { *c = cfg } }

// NewConfig applies opts to a zero Config and fills in every default.
func NewConfig(opts ...Option) Config {
	var cfg Config
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg.WithDefaults()
}

// WithDefaults returns a copy of c with every unset field replaced by its
// documented default.
func (c Config) WithDefaults() Config {
	if c.Logger == nil {
		c.Logger = NopLogger{}
	}
	r := &c.Reconnect
	if r.MinDelay <= 0 {
		r.MinDelay = 500 * time.Millisecond
	}
	if r.MaxDelay <= 0 {
		r.MaxDelay = 60 * time.Second
	}
	if r.MaxDelay < r.MinDelay {
		r.MaxDelay = r.MinDelay
	}
	if r.Factor < 1 {
		r.Factor = 2
	}
	if r.Jitter <= 0 || r.Jitter > 1 {
		r.Jitter = 0.5
	}
	if r.StableAfter <= 0 {
		r.StableAfter = 30 * time.Second
	}
	if c.InitialConnectAttempts <= 0 {
		c.InitialConnectAttempts = 1
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 15 * time.Second
	}
	if c.AckTimeout <= 0 {
		c.AckTimeout = 10 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 10 * time.Second
	}
	if c.ReadLimit <= 0 {
		c.ReadLimit = 8 << 20
	}
	if c.BufferSize <= 0 {
		c.BufferSize = 1024
	}
	if c.EventBuffer <= 0 {
		c.EventBuffer = 128
	}
	if c.Dialer == nil {
		d := *websocket.DefaultDialer
		d.HandshakeTimeout = 15 * time.Second
		c.Dialer = &d
	}
	return c
}

// SubscribeConfig holds the per-subscription settings.
type SubscribeConfig struct {
	// Buffer overrides Config.BufferSize for this subscription; zero keeps it.
	Buffer int
	// Overflow overrides Config.Overflow for this subscription.
	Overflow OverflowPolicy
	// OverflowSet records that Overflow was set explicitly (the zero value of
	// OverflowPolicy is a valid policy, DropOldest).
	OverflowSet bool
}

// SubscribeOption mutates a SubscribeConfig.
type SubscribeOption func(*SubscribeConfig)

// WithBuffer sets the capacity of this subscription's queue.
func WithBuffer(n int) SubscribeOption { return func(c *SubscribeConfig) { c.Buffer = n } }

// WithOverflow sets the overflow policy of this subscription.
func WithOverflow(p OverflowPolicy) SubscribeOption {
	return func(c *SubscribeConfig) { c.Overflow, c.OverflowSet = p, true }
}

// NewSubscribeConfig applies opts to a zero SubscribeConfig.
func NewSubscribeConfig(opts ...SubscribeOption) SubscribeConfig {
	var cfg SubscribeConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}
