package orderbook

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/stream"
)

// The Syncer delivers its events from a goroutine of its own and never while
// holding its lock. These tests pin down what that buys: a consumer that is slow,
// absent or calling back into the Syncer can neither stall the book nor deadlock.

// gatedConsumer is an OnEvent that blocks until released, recording what it saw.
type gatedConsumer struct {
	gate    chan struct{}
	once    sync.Once
	entered chan struct{}

	mu  sync.Mutex
	got []Event
}

func newGatedConsumer() *gatedConsumer {
	return &gatedConsumer{gate: make(chan struct{}), entered: make(chan struct{}, 1024)}
}

func (c *gatedConsumer) onEvent(ev Event) {
	c.entered <- struct{}{}
	<-c.gate
	c.mu.Lock()
	c.got = append(c.got, ev)
	c.mu.Unlock()
}

func (c *gatedConsumer) release() { c.once.Do(func() { close(c.gate) }) }

func (c *gatedConsumer) events() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Event(nil), c.got...)
}

func (c *gatedConsumer) types() string {
	parts := []string{}
	for _, ev := range c.events() {
		parts = append(parts, ev.Type.String())
	}
	return fmt.Sprint(parts)
}

func (c *gatedConsumer) waitFor(t *testing.T, cond func([]Event) bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond(c.events()) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; events: %s", what, c.types())
		}
		time.Sleep(time.Millisecond)
	}
}

// queueLen reports how many events wait for delivery.
func (s *Syncer) queueLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.evq)
}

func TestSyncer_StartAndStateNeverWaitForTheConsumer(t *testing.T) {
	wstest.CheckLeaks(t)
	c := newGatedConsumer()
	bids, asks := baseLevels()
	rest := newSnapshots(snap(10, bids, asks))
	s := NewSyncer(SyncerConfig{Symbol: "XBTUSDTM", Snapshot: rest.fetch, OnEvent: c.onEvent, RetryMin: time.Millisecond})
	t.Cleanup(s.Close)
	t.Cleanup(c.release) // runs first: the consumer must be free before Close waits for it

	// The first frame can arrive before Start is called: it begins the sync by
	// itself, the snapshot arrives and its event reaches a consumer that is not
	// reading yet. The goroutine that then calls Start used to wait for that
	// consumer while holding the lock the consumer's own calls needed.
	s.HandleDelta(delta(11, chg(Bid, "100", "11")))
	<-c.entered

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		s.Start()
		_ = s.State()
		_ = s.Resyncs()
		_ = s.DroppedEvents()
		s.HandleDelta(delta(12, chg(Bid, "100", "12")))
		s.HandleLoss(errors.New("anything"))
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("the Syncer blocked on a consumer that was not reading")
	}
}

func TestSyncer_ConsumerMayCallBackIntoTheSyncer(t *testing.T) {
	wstest.CheckLeaks(t)
	bids, asks := baseLevels()
	var latest atomic.Int64 // a REST stand-in that always returns the current book
	latest.Store(10)
	fetch := func(context.Context) (Snapshot, error) {
		return Snapshot{Sequence: latest.Load(), Bids: bids, Asks: asks}, nil
	}
	var (
		s     *Syncer
		calls atomic.Int64
		gaps  atomic.Int64
	)
	s = NewSyncer(SyncerConfig{
		Symbol: "XBTUSDTM", Snapshot: fetch, RetryMin: time.Millisecond,
		OnEvent: func(ev Event) {
			// What an application does in its event loop.
			_ = s.State()
			_ = s.Resyncs()
			_ = s.Book().Snapshot(5)
			if ev.Type == EventStale && gaps.Add(1) == 1 {
				s.HandleGap() // even asking for another resynchronisation from inside the callback
			}
			calls.Add(1)
		},
	})
	t.Cleanup(s.Close)
	s.Start()
	for seq := int64(11); seq < 400; seq++ {
		latest.Store(seq)
		s.HandleDelta(delta(seq, chg(Bid, "100", fmt.Sprint(seq))))
		if seq%97 == 0 {
			s.HandleReset()
		}
	}
	// Quiet again: the book must converge on the stream however the callbacks
	// interleaved with it, and the consumer must have seen the state changes.
	final := latest.Add(1)
	s.HandleReset()
	s.HandleDelta(delta(final, chg(Bid, "100", "1")))
	deadline := time.Now().Add(5 * time.Second)
	for s.State() != SyncSynced || s.Book().Sequence() < final {
		if time.Now().After(deadline) {
			t.Fatalf("no convergence: state=%v sequence=%d want>=%d", s.State(), s.Book().Sequence(), final)
		}
		time.Sleep(time.Millisecond)
	}
	for gaps.Load() == 0 || calls.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("the consumer saw %d events, %d of them stale", calls.Load(), gaps.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSyncer_SlowEventConsumerNeverStallsTheBook(t *testing.T) {
	wstest.CheckLeaks(t)
	c := newGatedConsumer()
	bids, asks := baseLevels()
	rest := newSnapshots(snap(10, bids, asks))
	s := NewSyncer(SyncerConfig{Symbol: "XBTUSDTM", Snapshot: rest.fetch, OnEvent: c.onEvent, EventQueue: 8, RetryMin: time.Millisecond})
	t.Cleanup(s.Close)
	t.Cleanup(c.release)

	s.Start()
	<-c.entered // the first event is in the consumer's hands; it will not read anything else for now

	done := make(chan struct{})
	go func() {
		defer close(done)
		for seq := int64(11); seq <= 500; seq++ {
			s.HandleDelta(delta(seq, chg(Bid, "100", fmt.Sprint(seq))))
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("feeding the Syncer blocked on a slow event consumer")
	}
	if s.State() != SyncSynced || s.Book().Sequence() != 500 {
		t.Fatalf("the book must follow the stream regardless: state=%v sequence=%d", s.State(), s.Book().Sequence())
	}
	if bid, _ := s.Book().BestBid(); bid.Size != "500" {
		t.Fatalf("best bid size = %s", bid.Size)
	}
	if s.DroppedEvents() == 0 {
		t.Fatal("the overflow must be counted")
	}
	if n := s.queueLen(); n > 9 {
		t.Fatalf("the queue grew to %d events although it is capped at 8", n)
	}
	if s.Resyncs() != 0 {
		t.Fatalf("a slow consumer must not make the book resynchronise, resyncs=%d", s.Resyncs())
	}

	c.release()
	c.waitFor(t, func(evs []Event) bool { return len(evs) > 1 && evs[len(evs)-1].Sequence == 500 }, "the consumer to catch up")
	evs := c.events()
	if evs[0].Type != EventSnapshot {
		t.Fatalf("events: %s", c.types())
	}
	// The updates that did not fit were replaced by a reload marker, an
	// EventSnapshot after the first one; whatever followed it is in order.
	snapshots, updates := 0, 0
	for _, ev := range evs {
		switch ev.Type {
		case EventSnapshot:
			snapshots++
		case EventUpdate:
			updates++
		}
	}
	if snapshots < 2 || uint64(updates)+s.DroppedEvents() < 490 {
		t.Fatalf("snapshots=%d updates=%d dropped=%d: %s", snapshots, updates, s.DroppedEvents(), c.types())
	}
}

// A consumer that follows the documented rules — reload from Event.Book on every
// EventSnapshot, apply an update only when its Sequence is newer than the mirror's
// — ends up with exactly the live book however slow it is.
func TestSyncer_ReloadMarkersLetASlowMirrorCatchUp(t *testing.T) {
	wstest.CheckLeaks(t)
	feed := rand.New(rand.NewSource(7))  // the feeding goroutine's generator
	delay := rand.New(rand.NewSource(8)) // the consumer's: *rand.Rand is not safe for concurrent use
	bids, asks := baseLevels()
	rest := newSnapshots(snap(10, bids, asks))

	mirror := New("XBTUSDTM")
	var mu sync.Mutex
	var applyErr error
	var lastSeq int64
	valid := false
	s := NewSyncer(SyncerConfig{
		Symbol: "XBTUSDTM", Snapshot: rest.fetch, EventQueue: 16, RetryMin: time.Millisecond,
		OnEvent: func(ev Event) {
			time.Sleep(time.Duration(delay.Intn(150)) * time.Microsecond) // a consumer slower than the feed
			mu.Lock()
			defer mu.Unlock()
			switch ev.Type {
			case EventSnapshot:
				snap, ok := ev.Book.SnapshotIfReady(0)
				if valid = ok; ok {
					if err := mirror.Reset(snap); err != nil && applyErr == nil {
						applyErr = err
					}
				}
			case EventStale:
				valid = false
			case EventUpdate:
				if !valid || ev.Sequence <= mirror.Sequence() {
					break // not following yet, or already contained in the book the mirror was reloaded from
				}
				if _, err := mirror.Apply(Delta{Start: mirror.Sequence() + 1, End: ev.Sequence, Changes: ev.Changes}); err != nil && applyErr == nil {
					applyErr = err
				}
			}
			lastSeq = ev.Sequence
		},
	})
	t.Cleanup(s.Close)
	s.Start()

	const last = 6000
	for seq := int64(11); seq <= last; seq++ {
		s.HandleDelta(delta(seq, chg(Bid, fmt.Sprint(100+feed.Intn(7)), fmt.Sprint(1+feed.Intn(50))), chg(Ask, fmt.Sprint(110+feed.Intn(7)), fmt.Sprint(feed.Intn(50)))))
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		done := lastSeq == last
		mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the consumer never caught up")
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if applyErr != nil {
		t.Fatalf("the mirror could not follow the events: %v", applyErr)
	}
	if got, want := fmt.Sprint(mirror.Snapshot(0)), fmt.Sprint(s.Book().Snapshot(0)); got != want {
		t.Fatalf("mirror and book differ:\n mirror %s\n   book %s", got, want)
	}
	if s.DroppedEvents() == 0 {
		t.Log("the consumer happened to keep up; the property still holds, but nothing was collapsed")
	}
}

func TestSyncer_FailSubscriptionPolicyEndsWithASlowConsumerError(t *testing.T) {
	wstest.CheckLeaks(t)
	c := newGatedConsumer()
	bids, asks := baseLevels()
	rest := newSnapshots(snap(10, bids, asks))
	s := NewSyncer(SyncerConfig{
		Symbol: "XBTUSDTM", Snapshot: rest.fetch, OnEvent: c.onEvent, EventQueue: 4,
		EventOverflow: stream.FailSubscription, RetryMin: time.Millisecond,
	})
	t.Cleanup(s.Close)
	t.Cleanup(c.release)
	s.Start()
	<-c.entered
	for seq := int64(11); seq <= 40; seq++ {
		s.HandleDelta(delta(seq, chg(Bid, "100", fmt.Sprint(seq))))
	}
	if s.State() != SyncFailed || s.Book().Ready() {
		t.Fatalf("state=%v ready=%v: the Syncer must stop instead of dropping updates", s.State(), s.Book().Ready())
	}
	s.HandleDelta(delta(41)) // a failed Syncer ignores further input
	if s.DroppedEvents() != 0 {
		t.Fatalf("nothing may be silently dropped under FailSubscription: %d", s.DroppedEvents())
	}
	c.release()
	c.waitFor(t, func(evs []Event) bool { return len(evs) > 0 && evs[len(evs)-1].Type == EventFailed }, "the failure event")
	evs := c.events()
	last := evs[len(evs)-1]
	if !errors.Is(last.Err, stream.ErrSlowConsumer) {
		t.Fatalf("failure cause = %v", last.Err)
	}
	for _, ev := range evs[:len(evs)-1] {
		if ev.Type == EventFailed {
			t.Fatalf("more than one failure event: %s", c.types())
		}
	}
}

func TestSyncer_QueuedStateChangesStayBoundedForAConsumerThatNeverReads(t *testing.T) {
	wstest.CheckLeaks(t)
	c := newGatedConsumer()
	bids, asks := baseLevels()
	s := NewSyncer(SyncerConfig{Symbol: "XBTUSDTM", OnEvent: c.onEvent, EventQueue: 3}) // stream-snapshot mode
	t.Cleanup(s.Close)
	t.Cleanup(c.release)
	s.HandleSnapshot(Snapshot{Sequence: 1, Bids: bids, Asks: asks})
	<-c.entered
	for i := int64(2); i < 200; i++ { // 400 state changes: a flapping connection and a consumer that is gone
		s.HandleReset()
		s.HandleSnapshot(Snapshot{Sequence: i, Bids: bids, Asks: asks})
	}
	if n := s.queueLen(); n > 2*3+2 {
		t.Fatalf("the queue grew to %d events", n)
	}
	c.release()
	c.waitFor(t, func(evs []Event) bool { return len(evs) > 1 && evs[len(evs)-1].Sequence == 199 }, "the latest state")
	evs := c.events()
	if last := evs[len(evs)-1]; last.Type != EventSnapshot {
		t.Fatalf("the newest event must describe the newest state: %s", c.types())
	}
}

func TestSyncer_CloseDiscardsWhatIsStillQueued(t *testing.T) {
	wstest.CheckLeaks(t)
	c := newGatedConsumer()
	bids, asks := baseLevels()
	s := NewSyncer(SyncerConfig{Symbol: "XBTUSDTM", OnEvent: c.onEvent})
	s.HandleSnapshot(Snapshot{Sequence: 1, Bids: bids, Asks: asks})
	<-c.entered // event 1 is in the consumer's hands
	for seq := int64(2); seq < 20; seq++ {
		s.HandleDelta(delta(seq, chg(Bid, "100", fmt.Sprint(seq))))
	}
	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close must wait for the event the consumer is holding")
	case <-time.After(50 * time.Millisecond):
	}
	c.release()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return once the consumer was free")
	}
	if evs := c.events(); len(evs) != 1 || evs[0].Type != EventSnapshot {
		t.Fatalf("events queued behind the in-flight one must be discarded: %s", c.types())
	}
	s.HandleDelta(delta(20, chg(Bid, "100", "20"))) // closed: nothing is queued or delivered
	s.HandleSnapshot(Snapshot{Sequence: 30, Bids: bids, Asks: asks})
	time.Sleep(20 * time.Millisecond)
	if len(c.events()) != 1 {
		t.Fatalf("a closed Syncer must not deliver: %s", c.types())
	}
}

func TestSyncer_EventQueueDefault(t *testing.T) {
	if got := NewSyncer(SyncerConfig{Symbol: "S"}).cfg.EventQueue; got != 1024 {
		t.Fatalf("default EventQueue = %d", got)
	}
	s := NewSyncer(SyncerConfig{Symbol: "S", EventQueue: 5})
	if s.cfg.EventQueue != 5 {
		t.Fatalf("EventQueue = %d", s.cfg.EventQueue)
	}
	s.Close()
}

// Every entry point of the Syncer, hammered from several goroutines at once while
// the REST stand-in fails and stalls at random and the event consumer is slow:
// nothing may race, deadlock or leak, and once the noise stops the book must
// converge on the stream.
func TestSyncer_RandomOperationsFromManyGoroutines(t *testing.T) {
	wstest.CheckLeaks(t)
	bids, asks := baseLevels()
	var latest, fetches, consumed atomic.Int64
	latest.Store(10)
	fetch := func(ctx context.Context) (Snapshot, error) {
		switch n := fetches.Add(1); {
		case n%7 == 0:
			return Snapshot{}, errors.New("transient REST failure")
		case n%11 == 0:
			select {
			case <-time.After(2 * time.Millisecond):
			case <-ctx.Done():
				return Snapshot{}, ctx.Err()
			}
		}
		return Snapshot{Sequence: latest.Load(), Bids: bids, Asks: asks}, nil
	}
	var s *Syncer
	var slow atomic.Bool
	s = NewSyncer(SyncerConfig{
		Symbol: "XBTUSDTM", Snapshot: fetch, EventQueue: 32, MaxAttempts: 1 << 20,
		RetryMin: time.Millisecond, RetryMax: 2 * time.Millisecond, ResetGrace: 2 * time.Millisecond,
		OnEvent: func(ev Event) {
			consumed.Add(1)
			_ = s.State()
			_ = ev.Book.Sequence()
			if slow.Load() {
				time.Sleep(200 * time.Microsecond)
			}
		},
	})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var seq atomic.Int64
	seq.Store(10)
	feed := func(seed int64) {
		defer wg.Done()
		rng := rand.New(rand.NewSource(seed))
		for {
			select {
			case <-stop:
				return
			default:
			}
			n := seq.Add(1)
			if rng.Intn(40) == 0 {
				n = seq.Add(int64(1 + rng.Intn(5))) // a gap in the stream
			}
			latest.Store(n)
			s.HandleDelta(delta(n, chg(Bid, fmt.Sprint(100+rng.Intn(5)), fmt.Sprint(rng.Intn(9)))))
			if rng.Intn(50) == 0 {
				time.Sleep(time.Duration(rng.Intn(300)) * time.Microsecond)
			}
		}
	}
	disturb := func(seed int64) {
		defer wg.Done()
		rng := rand.New(rand.NewSource(seed))
		for {
			select {
			case <-stop:
				return
			default:
			}
			switch rng.Intn(6) {
			case 0:
				s.HandleReset()
			case 1:
				s.HandleGap()
			case 2:
				s.HandleLoss(errors.New("undecodable"))
			case 3:
				s.HandleSnapshot(Snapshot{Sequence: latest.Load() - int64(rng.Intn(3)), Bids: bids, Asks: asks})
			case 4:
				slow.Store(!slow.Load())
			default:
				s.Start()
			}
			time.Sleep(time.Duration(rng.Intn(400)) * time.Microsecond)
		}
	}
	read := func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = s.State()
			_ = s.Resyncs()
			_ = s.DroppedEvents()
			_ = s.Book().Snapshot(4)
			_, _ = s.Book().Mid()
			runtime.Gosched() // with one CPU a spinning reader would starve the feeders
		}
	}
	s.Start()
	wg.Add(5)
	go feed(1)
	go feed(2)
	go disturb(3)
	go read()
	go read()
	time.Sleep(1200 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Quiet again: one more reset, one more update, and the book must follow.
	slow.Store(false)
	final := seq.Add(1)
	latest.Store(final)
	s.HandleReset()
	s.HandleDelta(delta(final, chg(Bid, "100", "1")))
	deadline := time.Now().Add(10 * time.Second)
	for s.State() != SyncSynced || s.Book().Sequence() < final {
		if time.Now().After(deadline) {
			t.Fatalf("no convergence: state=%v sequence=%d want>=%d resyncs=%d fetches=%d", s.State(), s.Book().Sequence(), final, s.Resyncs(), fetches.Load())
		}
		time.Sleep(time.Millisecond)
	}
	if consumed.Load() == 0 {
		t.Fatal("the consumer saw nothing")
	}

	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
}
