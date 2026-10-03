package wsengine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/stream"
)

func plainSub(key string) *stream.Subscription[int] {
	return stream.NewSubscription[int](key, 0, nil)
}

func TestShared_ConcurrentGetsShareOneCreation(t *testing.T) {
	var s Shared[int]
	var created atomic.Int32
	release := make(chan struct{})
	create := func() (*stream.Subscription[int], error) {
		created.Add(1)
		<-release
		return plainSub("k"), nil
	}
	const callers = 16
	got := make([]*stream.Subscription[int], callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sub, err := s.Get(context.Background(), "k", create)
			if err != nil {
				t.Errorf("Get: %v", err)
			}
			got[i] = sub
		}(i)
	}
	eventually(t, func() bool { return created.Load() >= 1 }, "the creation to start")
	time.Sleep(30 * time.Millisecond) // the others queue up behind it
	close(release)
	wg.Wait()
	if n := created.Load(); n != 1 {
		t.Fatalf("create ran %d times, want exactly once", n)
	}
	for i := 1; i < callers; i++ {
		if got[i] == nil || got[i] != got[0] {
			t.Fatalf("caller %d got %p, caller 0 got %p: they must share one subscription", i, got[i], got[0])
		}
	}
	// A later request finds the live subscription without creating anything.
	again, err := s.Get(context.Background(), "k", create)
	if err != nil || again != got[0] || created.Load() != 1 {
		t.Fatalf("second Get = %p, %v (creations: %d)", again, err, created.Load())
	}
}

func TestShared_AFailedCreationIsNotInheritedByTheWaiters(t *testing.T) {
	var s Shared[int]
	boom := errors.New("subscription rejected")
	release := make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, err := s.Get(context.Background(), "k", func() (*stream.Subscription[int], error) {
			<-release
			return nil, boom
		})
		first <- err
	}()
	time.Sleep(30 * time.Millisecond)
	second := make(chan *stream.Subscription[int], 1)
	go func() {
		sub, err := s.Get(context.Background(), "k", func() (*stream.Subscription[int], error) { return plainSub("k"), nil })
		if err != nil {
			t.Errorf("the waiter must create its own subscription instead of inheriting %v", err)
		}
		second <- sub
	}()
	time.Sleep(30 * time.Millisecond)
	close(release)
	if err := <-first; !errors.Is(err, boom) {
		t.Fatalf("creator = %v, want the rejection", err)
	}
	if sub := <-second; sub == nil {
		t.Fatal("the waiter got no subscription")
	}
}

func TestShared_AWaitersContextEndsItsWaitOnly(t *testing.T) {
	var s Shared[int]
	release := make(chan struct{})
	creator := make(chan *stream.Subscription[int], 1)
	go func() {
		sub, _ := s.Get(context.Background(), "k", func() (*stream.Subscription[int], error) {
			<-release
			return plainSub("k"), nil
		})
		creator <- sub
	}()
	time.Sleep(30 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, err := s.Get(ctx, "k", func() (*stream.Subscription[int], error) { return plainSub("other"), nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter = %v, want its own deadline", err)
	}
	close(release)
	if sub := <-creator; sub == nil || sub.Key() != "k" {
		t.Fatalf("the creation must be unaffected, got %v", sub)
	}
}

func TestShared_AnEndedSubscriptionIsReplaced(t *testing.T) {
	var s Shared[int]
	var created atomic.Int32
	create := func() (*stream.Subscription[int], error) {
		created.Add(1)
		return plainSub("k"), nil
	}
	a, _ := s.Get(context.Background(), "k", create)
	_ = a.Close()
	b, err := s.Get(context.Background(), "k", create)
	if err != nil || b == a || created.Load() != 2 {
		t.Fatalf("Get after the end = %p (old %p), %v, creations %d", b, a, err, created.Load())
	}
}

func TestShared_APanickingCreationDoesNotHangTheWaiters(t *testing.T) {
	var s Shared[int]
	release := make(chan struct{})
	crashed := make(chan any, 1)
	go func() {
		defer func() { crashed <- recover() }()
		_, _ = s.Get(context.Background(), "k", func() (*stream.Subscription[int], error) {
			<-release
			panic("boom")
		})
	}()
	time.Sleep(30 * time.Millisecond)
	waiter := make(chan *stream.Subscription[int], 1)
	go func() {
		sub, _ := s.Get(context.Background(), "k", func() (*stream.Subscription[int], error) { return plainSub("k"), nil })
		waiter <- sub
	}()
	time.Sleep(30 * time.Millisecond)
	close(release)
	if r := <-crashed; r == nil {
		t.Fatal("the panic must propagate to the caller that created")
	}
	select {
	case sub := <-waiter:
		if sub == nil {
			t.Fatal("the waiter must be able to create its own subscription")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a waiter hung behind a creation that panicked")
	}
}

func TestShared_RemoveWaitsForTheCreationAndForgetsTheKey(t *testing.T) {
	var s Shared[int]
	if got := s.Remove("nothing"); got != nil {
		t.Fatalf("Remove of an unknown key = %v", got)
	}
	release := make(chan struct{})
	created := make(chan *stream.Subscription[int], 1)
	go func() {
		sub, _ := s.Get(context.Background(), "k", func() (*stream.Subscription[int], error) {
			<-release
			return plainSub("k"), nil
		})
		created <- sub
	}()
	time.Sleep(30 * time.Millisecond)
	removed := make(chan *stream.Subscription[int], 1)
	go func() { removed <- s.Remove("k") }()
	select {
	case <-removed:
		t.Fatal("Remove must wait for the creation in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	made, gone := <-created, <-removed
	if made == nil || gone != made {
		t.Fatalf("Remove returned %p, the creation made %p", gone, made)
	}
	again, _ := s.Get(context.Background(), "k", func() (*stream.Subscription[int], error) { return plainSub("k"), nil })
	if again == made {
		t.Fatal("a removed key must be created anew")
	}
}
