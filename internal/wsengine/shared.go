package wsengine

import (
	"context"
	"errors"
	"sync"

	"github.com/tigusigalpa/kucoin-go/stream"
)

// errCreatePanicked is what callers waiting on a creation that panicked see.
var errCreatePanicked = errors.New("kucoin: ws: creating the subscription panicked")

// Shared keeps one live typed subscription per key. Requests for a key that
// arrive while its subscription is being created share that one attempt: the
// engine accepts a single subscription per topic, so without this the losers of
// a race would get stream.ErrAlreadySubscribed instead of the channel the raw
// clients promise for a topic that is already subscribed. The zero value is
// ready to use.
type Shared[T any] struct {
	mu sync.Mutex
	m  map[string]*sharedEntry[T]
}

type sharedEntry[T any] struct {
	ready chan struct{} // closed once sub or err is set
	sub   *stream.Subscription[T]
	err   error
}

// Get returns the live subscription for key. When there is none it calls create
// — once, however many callers ask at the same time — and the others wait for
// that attempt and receive its subscription. A failed creation is not
// remembered: each waiter then tries for itself, so one caller's cancelled
// context never fails another caller's request. A subscription that has ended
// is replaced.
func (s *Shared[T]) Get(ctx context.Context, key string, create func() (*stream.Subscription[T], error)) (*stream.Subscription[T], error) {
	for {
		s.mu.Lock()
		if s.m == nil {
			s.m = make(map[string]*sharedEntry[T])
		}
		e, ok := s.m[key]
		if !ok {
			e = &sharedEntry[T]{ready: make(chan struct{})}
			s.m[key] = e
			s.mu.Unlock()
			return s.build(key, e, create)
		}
		s.mu.Unlock()

		select {
		case <-e.ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if e.err != nil {
			continue // its creator failed and removed the entry: create it ourselves
		}
		select {
		case <-e.sub.Done():
			s.forget(key, e)
		default:
			return e.sub, nil
		}
	}
}

// build runs create for the entry e that this caller registered, and publishes
// the outcome to everybody waiting on it.
func (s *Shared[T]) build(key string, e *sharedEntry[T], create func() (*stream.Subscription[T], error)) (*stream.Subscription[T], error) {
	completed := false
	defer func() {
		if !completed { // create panicked: the waiters must not hang
			e.err = errCreatePanicked
		}
		if e.err != nil {
			s.forget(key, e)
		}
		close(e.ready)
	}()
	sub, err := create()
	e.sub, e.err = sub, err
	completed = true
	return sub, err
}

func (s *Shared[T]) forget(key string, e *sharedEntry[T]) {
	s.mu.Lock()
	if s.m[key] == e {
		delete(s.m, key)
	}
	s.mu.Unlock()
}

// Remove forgets the subscription for key and returns it, or nil when there is
// none. A subscription that is still being created is waited for.
func (s *Shared[T]) Remove(key string) *stream.Subscription[T] {
	s.mu.Lock()
	e := s.m[key]
	delete(s.m, key)
	s.mu.Unlock()
	if e == nil {
		return nil
	}
	<-e.ready
	return e.sub
}
