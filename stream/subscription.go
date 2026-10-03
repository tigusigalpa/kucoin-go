package stream

import (
	"sync"
	"sync/atomic"
	"time"
)

// Subscription is the consumer-facing handle of one typed subscription.
//
//	sub, err := session.SubscribeTrades(ctx, "XBTUSDTM")
//	if err != nil { ... }
//	defer sub.Close()
//	for trade := range sub.C() { ... }
//
// C delivers updates in order and is closed when the subscription ends, for
// whatever reason; after that Err reports why (nil when the end was requested
// with Close or by closing the session). A Subscription is safe for
// concurrent use.
type Subscription[T any] struct {
	key     string
	out     chan T
	done    chan struct{}
	closeFn func() error

	finishOnce sync.Once
	sealOnce   sync.Once
	closeOnce  sync.Once
	closeErr   error

	mu         sync.Mutex
	err        error
	dropped    atomic.Uint64
	dropSource atomic.Pointer[func() uint64]
}

// NewSubscription creates the handle for a subscription named key. buffer is
// the capacity of the delivery channel (0 means unbuffered; the engine queue in
// front of it already provides buffering). closeFn is invoked by Close to
// unsubscribe and may be nil.
//
// NewSubscription and the producer-side methods (Deliver, Finish, Seal,
// AddDropped) are used by the streaming packages of this module and by code
// that adds support for a channel KuCoin introduces later; applications
// consume a Subscription through C, Err, Done and Close.
func NewSubscription[T any](key string, buffer int, closeFn func() error) *Subscription[T] {
	if buffer < 0 {
		buffer = 0
	}
	return &Subscription[T]{
		key:     key,
		out:     make(chan T, buffer),
		done:    make(chan struct{}),
		closeFn: closeFn,
	}
}

// C returns the channel of updates. It is closed when the subscription ends.
func (s *Subscription[T]) C() <-chan T { return s.out }

// Done is closed as soon as the subscription has ended, which may be slightly
// before C is closed. Use it to wait without receiving.
func (s *Subscription[T]) Done() <-chan struct{} { return s.done }

// Err returns why the subscription ended: nil while it is running and after a
// requested end (Close, session Close), otherwise the cause, for example a
// *ServerError, ErrSlowConsumer or a resynchronisation failure. It is final
// once C is closed.
func (s *Subscription[T]) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Key returns the subscription identity (topic or channel description).
func (s *Subscription[T]) Key() string { return s.key }

// Dropped returns how many updates the overflow policy discarded for this
// subscription because its consumer was too slow. It is accurate immediately,
// even while the consumer is not reading.
func (s *Subscription[T]) Dropped() uint64 {
	if src := s.dropSource.Load(); src != nil {
		return (*src)()
	}
	return s.dropped.Load()
}

// BindDropped makes Dropped report the given live counter instead of the
// count accumulated through AddDropped. It is a producer-side method, used by
// the streaming packages once the connection-level subscription exists.
func (s *Subscription[T]) BindDropped(source func() uint64) {
	s.dropSource.Store(&source)
}

// Close unsubscribes and ends the subscription. It is idempotent and safe to
// call from any goroutine. The returned error reports a failed unsubscribe
// request; the subscription is ended locally either way.
func (s *Subscription[T]) Close() error {
	s.closeOnce.Do(func() {
		if s.closeFn != nil {
			s.closeErr = s.closeFn()
		}
		s.Finish(nil)
	})
	return s.closeErr
}

// Deliver hands v to the consumer, blocking until it is received or the
// subscription ends. It returns false when the subscription ended and v was not
// delivered. It may be called from several goroutines (the updates of different
// callers interleave in any order); Seal must come after the last call has
// returned. The usual arrangement is a single delivering goroutine that also
// seals.
func (s *Subscription[T]) Deliver(v T) bool {
	select {
	case <-s.done:
		return false
	default:
	}
	select {
	case s.out <- v:
		return true
	case <-s.done:
		return false
	}
}

// DeliverWithin is Deliver with a deadline: it returns false, without delivering
// v, when the consumer did not receive it within d or the subscription ended
// first. Producers use it for a final courtesy message that an absent consumer
// must not be allowed to hold up. It follows the same rules as Deliver.
func (s *Subscription[T]) DeliverWithin(v T, d time.Duration) bool {
	select {
	case <-s.done:
		return false
	default:
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case s.out <- v:
		return true
	case <-s.done:
		return false
	case <-t.C:
		return false
	}
}

// Finish ends the subscription with err (nil for a requested end). Only the
// first call has an effect. It never closes C; the producer goroutine does that
// through Seal once it stops delivering.
func (s *Subscription[T]) Finish(err error) {
	s.finishOnce.Do(func() {
		s.mu.Lock()
		s.err = err
		s.mu.Unlock()
		close(s.done)
	})
}

// Seal closes C. It must be called exactly once, after the last Deliver has
// returned. It also finishes the subscription if that has not happened yet.
func (s *Subscription[T]) Seal() {
	s.Finish(nil)
	s.sealOnce.Do(func() { close(s.out) })
}

// AddDropped records n updates discarded by the overflow policy.
func (s *Subscription[T]) AddDropped(n uint64) { s.dropped.Add(n) }
