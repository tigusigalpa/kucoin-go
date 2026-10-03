// Package wsengine is the shared connection engine behind every KuCoin
// WebSocket client of this module. It owns everything that is identical for
// the Classic and UTA wire dialects: dialling, the welcome handshake,
// heartbeats with a pong watchdog, serialised writes with deadlines, request /
// acknowledgement correlation, per-subscription bounded queues with ordered
// delivery, reconnect with jittered backoff, ordered resubscription, lifecycle
// events and a graceful, leak-free shutdown.
//
// A dialect plugs in through the Protocol interface, which only has to know how
// to build an endpoint URL, classify inbound frames and build ping frames. One
// that also implements MessageLimiter gets its requests paced to the server's
// message limit.
package wsengine

import (
	"context"
	"time"

	"github.com/tigusigalpa/kucoin-go/stream"
)

// Kind classifies an inbound frame.
type Kind uint8

// Inbound frame kinds.
const (
	// KindUnknown frames are ignored (and counted).
	KindUnknown Kind = iota
	// KindWelcome is the server greeting that makes a connection usable.
	KindWelcome
	// KindPong answers a ping.
	KindPong
	// KindAck is the positive reply to the request identified by ID.
	KindAck
	// KindNack is the negative reply to the request identified by ID; Err holds
	// the reason.
	KindNack
	// KindPush is a data push to be routed to subscriptions.
	KindPush
	// KindError is an error frame. When its ID matches a pending request it
	// is that request's negative reply, otherwise it is reported as an
	// unsolicited server error.
	KindError
)

// Inbound is the classification of one inbound text frame.
type Inbound struct {
	Kind Kind
	// ID correlates Ack, Nack and Error frames with the request they answer.
	ID string
	// Err is the typed reason for Nack and Error frames.
	Err error
	// Route is the primary routing key of a Push. RouteAlt, when non-empty, is
	// a second key the push is also delivered to (used for wildcard
	// subscriptions).
	Route    string
	RouteAlt string
	// Msg is the protocol's parsed envelope of a Push, passed through to
	// handlers as stream.Frame.Msg.
	Msg any
	// PingInterval and PingTimeout are the heartbeat parameters a Welcome
	// frame advertises; zero means the frame did not carry them.
	PingInterval time.Duration
	PingTimeout  time.Duration
}

// Endpoint is what a connection attempt dials.
type Endpoint struct {
	// URL is the full wss:// URL including any token and connect ID.
	URL string
	// PingInterval and PingTimeout are heartbeat parameters known before
	// connecting (the Classic token response carries them); zero if unknown.
	PingInterval time.Duration
	PingTimeout  time.Duration
}

// Requester sends a request frame and waits for KuCoin's reply.
type Requester interface {
	// Request writes frame, which must carry id, and waits for the matching
	// reply. It returns nil for a positive reply, the reply's error for a
	// negative one, stream.ErrAckTimeout when none arrives in time, and the
	// connection's failure cause if the connection dies first.
	Request(ctx context.Context, id string, frame []byte) error
}

// Protocol adapts one KuCoin WebSocket dialect to the engine.
type Protocol interface {
	// Name identifies the dialect in logs.
	Name() string
	// Endpoint returns what to dial for one connection attempt. It is called
	// before every attempt, so it can fetch a fresh token. A returned error
	// wrapped with stream.Permanent stops the reconnect loop.
	Endpoint(ctx context.Context) (Endpoint, error)
	// Classify inspects one inbound text frame.
	Classify(raw []byte) Inbound
	// Ping builds the application-level heartbeat frame carrying id.
	Ping(id string) []byte
	// Welcomed runs after the welcome frame of every (re)connection, before any
	// subscription is sent, and may authenticate through r. An error aborts the
	// connection attempt; wrap it with stream.Permanent to stop retrying.
	Welcomed(ctx context.Context, r Requester) error
}

// Spec describes one subscription to the engine.
type Spec struct {
	// Name uniquely identifies the subscription on the connection; a second
	// Subscribe with the same Name fails with stream.ErrAlreadySubscribed.
	Name string
	// Routes are the routing keys whose pushes this subscription receives.
	Routes []string
	// Subscribe builds the subscribe frame carrying id; Unsubscribe builds the
	// unsubscribe frame.
	Subscribe   func(id string) []byte
	Unsubscribe func(id string) []byte
	// Handler processes the subscription's updates in order.
	Handler stream.Handler
	// Buffer and Overflow override the connection defaults when non-zero /
	// OverflowSet.
	Buffer      int
	Overflow    stream.OverflowPolicy
	OverflowSet bool
}
