package stream

import "time"

// Frame is one inbound data push, as handed to a Handler.
type Frame struct {
	// Raw is the complete inbound text frame. The receiver owns the slice.
	Raw []byte
	// Route is the routing key the frame matched (a topic for Classic
	// connections).
	Route string
	// Msg is the protocol's parsed envelope: *classic.Message for Classic
	// connections and *uta.Push for UTA connections.
	Msg any
	// Generation is the connection generation the frame arrived on.
	Generation uint64
	// ReceivedAt is when the frame was read from the socket.
	ReceivedAt time.Time
}

// Handler processes the updates of one subscription. All methods except
// OnAbort are called from a single goroutine, in the order the events
// occurred, so an implementation needs no locking of its own state. OnFrame
// may block to apply back-pressure: while it blocks, new frames queue up to
// the subscription buffer and then fall under the OverflowPolicy.
type Handler interface {
	// OnFrame receives one push.
	OnFrame(Frame)
	// OnReset reports that the connection was lost and re-established. Frames
	// delivered before this call belong to the previous connection and updates
	// may have been missed; a sequenced stream must resynchronise.
	OnReset(generation uint64)
	// OnGap reports that frames dropped by the overflow policy were lost
	// before the next OnFrame call. A sequenced stream must resynchronise.
	OnGap(dropped uint64)
	// OnAbort reports that the subscription is over: err is nil after an
	// Unsubscribe or Close and non-nil when the subscription failed. It may be
	// called from any goroutine, possibly while OnFrame is still running, and
	// must unblock a pending OnFrame. It is called at most once.
	OnAbort(err error)
	// OnClosed is called once, last, from the delivery goroutine after OnAbort
	// and after the final OnFrame has returned; no other method is called
	// afterwards. It is the place to release resources such as channels.
	OnClosed()
}

// HandlerFuncs adapts plain functions to Handler. Nil fields are no-ops.
type HandlerFuncs struct {
	Frame  func(Frame)
	Reset  func(generation uint64)
	Gap    func(dropped uint64)
	Abort  func(err error)
	Closed func()
}

// OnFrame implements Handler.
func (h HandlerFuncs) OnFrame(f Frame) {
	if h.Frame != nil {
		h.Frame(f)
	}
}

// OnReset implements Handler.
func (h HandlerFuncs) OnReset(generation uint64) {
	if h.Reset != nil {
		h.Reset(generation)
	}
}

// OnGap implements Handler.
func (h HandlerFuncs) OnGap(dropped uint64) {
	if h.Gap != nil {
		h.Gap(dropped)
	}
}

// OnAbort implements Handler.
func (h HandlerFuncs) OnAbort(err error) {
	if h.Abort != nil {
		h.Abort(err)
	}
}

// OnClosed implements Handler.
func (h HandlerFuncs) OnClosed() {
	if h.Closed != nil {
		h.Closed()
	}
}
