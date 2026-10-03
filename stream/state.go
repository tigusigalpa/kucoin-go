package stream

import "time"

// State is the lifecycle state of a managed WebSocket connection.
type State int32

// Connection states, in the order a healthy connection normally visits them.
const (
	// StateIdle is the state of a client that has not connected yet, or whose
	// first connection attempt failed.
	StateIdle State = iota
	// StateConnecting means the first connection is being established.
	StateConnecting
	// StateConnected means the welcome message was received, any required
	// authentication succeeded and subscriptions are live.
	StateConnected
	// StateReconnecting means the connection was lost and is being
	// re-established; subscriptions are restored automatically.
	StateReconnecting
	// StateClosing means Close or Shutdown is in progress.
	StateClosing
	// StateClosed is terminal: the client was closed or hit a fatal error.
	StateClosed
)

// String returns a lower-case state name.
func (s State) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateReconnecting:
		return "reconnecting"
	case StateClosing:
		return "closing"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// EventType identifies a lifecycle Event.
type EventType int

// Lifecycle event types.
const (
	// EventConnected is emitted once when the initial connection is ready.
	EventConnected EventType = iota + 1
	// EventDisconnected is emitted when an established connection is lost.
	// Err holds the cause. A reconnect follows unless reconnecting is disabled
	// or the error is fatal.
	EventDisconnected
	// EventReconnecting is emitted before each reconnect attempt, with the
	// attempt number and the delay that preceded it.
	EventReconnecting
	// EventReconnected is emitted after a lost connection was re-established
	// and every surviving subscription was subscribed again. Updates that
	// occurred while disconnected are not replayed; sequenced streams resync.
	EventReconnected
	// EventSubscriptionFailed is emitted when KuCoin rejects a subscription
	// that is being restored after a reconnect. The subscription ends with
	// that error.
	EventSubscriptionFailed
	// EventOverflow is emitted when a slow consumer caused updates to be
	// dropped (or its subscription to be failed, per the OverflowPolicy).
	EventOverflow
	// EventDecodeError is emitted when an inbound push could not be decoded
	// into its typed payload. The stream keeps running.
	EventDecodeError
	// EventServerError is emitted for an error frame from KuCoin that is not
	// the reply to a request, such as a ping-timeout notice.
	EventServerError
	// EventClosed is emitted once when the client reaches StateClosed. Err is
	// nil for a requested Close and non-nil for a fatal failure.
	EventClosed
)

// String returns a snake-case event name.
func (t EventType) String() string {
	switch t {
	case EventConnected:
		return "connected"
	case EventDisconnected:
		return "disconnected"
	case EventReconnecting:
		return "reconnecting"
	case EventReconnected:
		return "reconnected"
	case EventSubscriptionFailed:
		return "subscription_failed"
	case EventOverflow:
		return "overflow"
	case EventDecodeError:
		return "decode_error"
	case EventServerError:
		return "server_error"
	case EventClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Event describes something that happened to a connection. Which fields are
// meaningful depends on Type.
type Event struct {
	Type EventType
	Time time.Time
	// Err is the cause for Disconnected, SubscriptionFailed, DecodeError,
	// ServerError and a fatal Closed event.
	Err error
	// Attempt is the 1-based reconnect attempt number (Reconnecting).
	Attempt int
	// Backoff is the delay waited before the attempt (Reconnecting).
	Backoff time.Duration
	// Subscription names the affected subscription (topic or channel).
	Subscription string
	// Dropped is the cumulative number of updates dropped for the
	// subscription (Overflow).
	Dropped uint64
	// Generation counts successful connections; it increases by one with every
	// (re)connect, so consumers can tell which connection an event belongs to.
	Generation uint64
}

// Stats is a point-in-time snapshot of a connection's counters.
type Stats struct {
	State State
	// Generation is the number of successful connections so far.
	Generation uint64
	// Reconnects is the number of successful reconnections.
	Reconnects uint64
	// Subscriptions is the number of live subscriptions.
	Subscriptions int
	// FramesReceived counts every inbound frame.
	FramesReceived uint64
	// PushesDropped counts pushes dropped by the overflow policy.
	PushesDropped uint64
	// DecodeErrors counts pushes that failed to decode.
	DecodeErrors uint64
	// ConnectedSince is when the current connection became ready; zero when
	// not connected.
	ConnectedSince time.Time
	// LastFrame is when the last inbound frame arrived; zero if none.
	LastFrame time.Time
}
