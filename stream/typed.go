package stream

// Decoder converts one frame into a typed update. Return ok=false to skip a
// frame that carries no update (for example a heartbeat-style notice) and a
// non-nil error for a frame that is malformed: the error is reported through
// the connection's EventDecodeError and the frame is skipped, the subscription
// keeps running.
type Decoder[T any] func(f Frame) (v T, ok bool, err error)

// typedHandler feeds a Subscription from a Decoder.
type typedHandler[T any] struct {
	sub    *Subscription[T]
	decode Decoder[T]
	report func(error)
}

// NewHandler adapts a typed Subscription and a Decoder to the Handler
// interface: every frame is decoded and delivered in order (blocking while the
// consumer is busy, which applies back-pressure to the subscription queue),
// frames lost to queue overflow are counted in Subscription.Dropped, and the
// subscription ends when the connection aborts the handler. report receives
// decoding failures as *DecodeError; it may be nil.
func NewHandler[T any](sub *Subscription[T], decode Decoder[T], report func(error)) Handler {
	return &typedHandler[T]{sub: sub, decode: decode, report: report}
}

func (h *typedHandler[T]) OnFrame(f Frame) {
	v, ok, err := h.decode(f)
	if err != nil {
		if h.report != nil {
			h.report(&DecodeError{Channel: f.Route, Raw: f.Raw, Err: err})
		}
		return
	}
	if ok {
		h.sub.Deliver(v)
	}
}

func (h *typedHandler[T]) OnReset(uint64)    {}
func (h *typedHandler[T]) OnGap(n uint64)    { h.sub.AddDropped(n) }
func (h *typedHandler[T]) OnAbort(err error) { h.sub.Finish(err) }
func (h *typedHandler[T]) OnClosed()         { h.sub.Seal() }
