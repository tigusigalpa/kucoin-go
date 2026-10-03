package orderbook

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/stream"
)

// recorder collects the events a Syncer emits.
type recorder struct {
	mu     sync.Mutex
	events []Event
	notify chan struct{}
}

func newRecorder() *recorder { return &recorder{notify: make(chan struct{}, 1024)} }

func (r *recorder) on(ev Event) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
}

func (r *recorder) types() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	parts := make([]string, len(r.events))
	for i, e := range r.events {
		parts[i] = e.Type.String()
	}
	return strings.Join(parts, ",")
}

func (r *recorder) count(t EventType) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.events {
		if e.Type == t {
			n++
		}
	}
	return n
}

func (r *recorder) last() Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.events[len(r.events)-1]
}

func (r *recorder) event(i int) Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.events[i]
}

// waitTypes waits until the delivered events are exactly want ("snapshot,update").
// Events are delivered asynchronously, in order, by the Syncer's own goroutine.
func (r *recorder) waitTypes(t *testing.T, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for r.types() != want {
		if time.Now().After(deadline) {
			t.Fatalf("events = %q, want %q", r.types(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func (r *recorder) waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; events so far: %s", what, r.types())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// snapshots is a scriptable REST stand-in.
type snapshots struct {
	mu     sync.Mutex
	calls  int
	steps  []func(ctx context.Context) (Snapshot, error)
	called chan int
}

func newSnapshots(steps ...func(ctx context.Context) (Snapshot, error)) *snapshots {
	return &snapshots{steps: steps, called: make(chan int, 64)}
}

func (s *snapshots) fetch(ctx context.Context) (Snapshot, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	var step func(ctx context.Context) (Snapshot, error)
	if len(s.steps) > 0 {
		step = s.steps[0]
		if len(s.steps) > 1 {
			s.steps = s.steps[1:]
		}
	}
	s.mu.Unlock()
	s.called <- n
	if step == nil {
		return Snapshot{}, errors.New("no scripted step")
	}
	return step(ctx)
}

func (s *snapshots) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func snap(seq int64, bids, asks []Level) func(context.Context) (Snapshot, error) {
	return func(context.Context) (Snapshot, error) {
		return Snapshot{Sequence: seq, Bids: bids, Asks: asks}, nil
	}
}

func fail(err error) func(context.Context) (Snapshot, error) {
	return func(context.Context) (Snapshot, error) { return Snapshot{}, err }
}

func gate(release <-chan struct{}, then func(context.Context) (Snapshot, error)) func(context.Context) (Snapshot, error) {
	return func(ctx context.Context) (Snapshot, error) {
		select {
		case <-release:
			return then(ctx)
		case <-ctx.Done():
			return Snapshot{}, ctx.Err()
		}
	}
}

func delta(seq int64, changes ...Change) Delta { return Delta{Start: seq, End: seq, Changes: changes} }

func newSyncer(t *testing.T, fetch SnapshotFunc, rec *recorder, mutate ...func(*SyncerConfig)) *Syncer {
	t.Helper()
	cfg := SyncerConfig{Symbol: "XBTUSDTM", Snapshot: fetch, OnEvent: rec.on, RetryMin: time.Millisecond, RetryMax: 4 * time.Millisecond, ResetGrace: 20 * time.Millisecond}
	for _, m := range mutate {
		m(&cfg)
	}
	s := NewSyncer(cfg)
	t.Cleanup(s.Close)
	return s
}

func baseLevels() ([]Level, []Level) {
	return []Level{lv("100", "1"), lv("99", "1")}, []Level{lv("101", "1"), lv("102", "1")}
}

func TestSyncer_BuffersDeltasWhileTheSnapshotIsFetchedThenReplaysOnlyWhatIsNew(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	release := make(chan struct{})
	bids, asks := baseLevels()
	rest := newSnapshots(gate(release, snap(100, bids, asks)))
	s := newSyncer(t, rest.fetch, rec)
	if s.State() != SyncIdle {
		t.Fatalf("state = %v", s.State())
	}
	s.Start()
	<-rest.called
	if s.State() != SyncSyncing {
		t.Fatalf("state = %v", s.State())
	}
	// These arrive while the snapshot (sequence 100) is still in flight.
	for seq := int64(98); seq <= 103; seq++ {
		s.HandleDelta(delta(seq, chg(Bid, "100", fmt.Sprint(seq))))
	}
	select {
	case <-s.Ready():
		t.Fatal("not ready before the snapshot arrived")
	default:
	}
	close(release)
	rec.waitFor(t, func() bool { return s.State() == SyncSynced }, "synced state")
	<-s.Ready()

	rec.waitTypes(t, "snapshot") // replayed history must not be reported as updates
	if seq := s.Book().Sequence(); seq != 103 {
		t.Fatalf("sequence = %d, want 103 (98..100 dropped as stale, 101..103 replayed)", seq)
	}
	if bid, _ := s.Book().BestBid(); bid.Size != "103" {
		t.Fatalf("best bid size = %s, want the last replayed value", bid.Size)
	}
	s.HandleDelta(delta(104, chg(Ask, "101", "7")))
	rec.waitTypes(t, "snapshot,update")
	ev := rec.last()
	if ev.Sequence != 104 || len(ev.Changes) != 1 || ev.Changes[0].Price != "101" || ev.Symbol != "XBTUSDTM" || ev.Book != s.Book() {
		t.Fatalf("update event: %+v", ev)
	}
	if rest.count() != 1 {
		t.Fatalf("snapshot fetched %d times", rest.count())
	}
}

func TestSyncer_StartWithoutAnyDeltaStillSyncsAQuietMarket(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	bids, asks := baseLevels()
	rest := newSnapshots(snap(50, bids, asks))
	s := newSyncer(t, rest.fetch, rec)
	s.Start()
	s.Start() // a second Start is a no-op
	rec.waitFor(t, func() bool { return s.State() == SyncSynced }, "synced")
	if rest.count() != 1 || s.Book().Sequence() != 50 {
		t.Fatalf("fetches=%d seq=%d", rest.count(), s.Book().Sequence())
	}
	s.HandleDelta(delta(51, chg(Bid, "100", "5")))
	rec.waitTypes(t, "snapshot,update")
}

func TestSyncer_SnapshotOlderThanTheBufferIsRefetched(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	release := make(chan struct{})
	bids, asks := baseLevels()
	// The first snapshot (sequence 90) predates the first buffered delta (95), so
	// updates 91..94 would be missing: it must be thrown away and fetched again.
	rest := newSnapshots(gate(release, snap(90, bids, asks)), snap(96, bids, asks))
	s := newSyncer(t, rest.fetch, rec)
	s.HandleDelta(delta(95, chg(Bid, "100", "95")))
	<-rest.called
	s.HandleDelta(delta(96, chg(Bid, "100", "96")))
	s.HandleDelta(delta(97, chg(Bid, "100", "97")))
	close(release)
	rec.waitFor(t, func() bool { return s.State() == SyncSynced }, "synced after a refetch")
	if rest.count() != 2 {
		t.Fatalf("fetches = %d, want 2", rest.count())
	}
	if s.Book().Sequence() != 97 {
		t.Fatalf("sequence = %d, want 97 (96 stale, 97 replayed)", s.Book().Sequence())
	}
	rec.waitTypes(t, "snapshot")
}

func TestSyncer_SequenceGapInTheLiveStreamResyncs(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	bids, asks := baseLevels()
	rest := newSnapshots(snap(10, bids, asks), snap(30, []Level{lv("200", "2")}, []Level{lv("201", "2")}))
	s := newSyncer(t, rest.fetch, rec)
	s.Start()
	rec.waitFor(t, func() bool { return s.State() == SyncSynced }, "first sync")
	s.HandleDelta(delta(11, chg(Bid, "100", "11")))
	s.HandleDelta(delta(12, chg(Bid, "100", "12")))
	s.HandleDelta(delta(20, chg(Bid, "100", "20"))) // 13..19 never arrived

	rec.waitFor(t, func() bool { return s.State() == SyncSynced && rest.count() == 2 }, "second sync")
	rec.waitTypes(t, "snapshot,update,update,stale,snapshot")
	stale := rec.event(3)
	var gap *GapError
	if !errors.Is(stale.Err, ErrSequenceGap) || !errors.As(stale.Err, &gap) || gap.Have != 12 || gap.Start != 20 {
		t.Fatalf("stale cause = %v", stale.Err)
	}
	if bid, _ := s.Book().BestBid(); bid.Price != "200" {
		t.Fatalf("the book must come from the new snapshot, best bid = %v", bid)
	}
	if s.Resyncs() != 1 {
		t.Fatalf("resyncs = %d", s.Resyncs())
	}
	if s.Book().Sequence() != 30 {
		t.Fatalf("sequence = %d (the gap-revealing delta 20 is older than snapshot 30)", s.Book().Sequence())
	}
}

func TestSyncer_ResetClearsTheBookAndResyncsOnTheNextDeltaOrAfterTheGrace(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	bids, asks := baseLevels()
	rest := newSnapshots(snap(10, bids, asks), snap(20, bids, asks), snap(40, bids, asks))
	s := newSyncer(t, rest.fetch, rec, func(c *SyncerConfig) { c.ResetGrace = time.Hour })
	s.Start()
	rec.waitFor(t, func() bool { return s.State() == SyncSynced }, "first sync")

	s.HandleReset()
	if s.State() != SyncIdle || s.Book().Ready() {
		t.Fatalf("after a reset the book must be cleared: state=%v ready=%v", s.State(), s.Book().Ready())
	}
	rec.waitTypes(t, "snapshot,stale")
	if last := rec.last(); !errors.Is(last.Err, ErrReconnected) {
		t.Fatalf("last event: %+v", last)
	}
	time.Sleep(30 * time.Millisecond)
	if rest.count() != 1 {
		t.Fatal("no snapshot may be fetched before the stream resumed")
	}
	s.HandleDelta(delta(21, chg(Bid, "100", "21"))) // the stream is back
	rec.waitFor(t, func() bool { return s.State() == SyncSynced && rest.count() == 2 }, "resync after the first delta")
	if s.Book().Sequence() != 21 {
		t.Fatalf("sequence = %d", s.Book().Sequence())
	}

	// A quiet symbol: nothing arrives, the grace timer starts the resync.
	quiet := newRecorder()
	quietRest := newSnapshots(snap(1, bids, asks), snap(2, bids, asks))
	q := newSyncer(t, quietRest.fetch, quiet, func(c *SyncerConfig) { c.ResetGrace = 15 * time.Millisecond })
	q.Start()
	quiet.waitFor(t, func() bool { return q.State() == SyncSynced }, "quiet first sync")
	q.HandleReset()
	quiet.waitFor(t, func() bool { return q.State() == SyncSynced && quietRest.count() == 2 }, "grace-timer resync")
}

func TestSyncer_DroppedUpdatesResyncImmediately(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	bids, asks := baseLevels()
	rest := newSnapshots(snap(10, bids, asks), snap(50, bids, asks))
	s := newSyncer(t, rest.fetch, rec)
	s.Start()
	rec.waitFor(t, func() bool { return s.State() == SyncSynced }, "first sync")
	s.HandleGap()
	rec.waitFor(t, func() bool { return s.State() == SyncSynced && rest.count() == 2 }, "resync after dropped updates")
	rec.waitTypes(t, "snapshot,stale,snapshot")
	if stale := rec.event(1); !errors.Is(stale.Err, ErrUpdatesDropped) {
		t.Fatalf("events = %s, stale = %+v", rec.types(), stale)
	}
}

func TestSyncer_FetchFailuresAreRetriedThenSucceed(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	bids, asks := baseLevels()
	rest := newSnapshots(fail(errors.New("503")), fail(errors.New("timeout")), snap(10, bids, asks))
	s := newSyncer(t, rest.fetch, rec)
	s.Start()
	rec.waitFor(t, func() bool { return s.State() == SyncSynced }, "sync after retries")
	rec.waitTypes(t, "snapshot")
	if rest.count() != 3 {
		t.Fatalf("fetches=%d", rest.count())
	}
}

func TestSyncer_GivesUpAfterMaxAttemptsWithATypedError(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	boom := errors.New("snapshot endpoint down")
	rest := newSnapshots(fail(boom))
	s := newSyncer(t, rest.fetch, rec, func(c *SyncerConfig) { c.MaxAttempts = 3 })
	s.Start()
	rec.waitFor(t, func() bool { return s.State() == SyncFailed }, "failed state")
	rec.waitTypes(t, "failed")
	ev := rec.last()
	if !errors.Is(ev.Err, stream.ErrResyncFailed) || !errors.Is(ev.Err, boom) {
		t.Fatalf("event: %+v", ev)
	}
	if rest.count() != 3 {
		t.Fatalf("fetches = %d, want exactly 3", rest.count())
	}
	// A failed Syncer ignores further input.
	s.HandleDelta(delta(1))
	s.HandleReset()
	s.HandleGap()
	s.HandleSnapshot(Snapshot{Sequence: 5})
	time.Sleep(20 * time.Millisecond) // anything emitted would have been delivered by now
	if rec.count(EventFailed) != 1 || rec.types() != "failed" || s.Book().Ready() {
		t.Fatalf("a failed syncer must stay failed: %s", rec.types())
	}
}

func TestSyncer_PermanentFetchErrorFailsImmediately(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	rest := newSnapshots(fail(stream.Permanent(errors.New("credentials required"))))
	s := newSyncer(t, rest.fetch, rec)
	s.Start()
	rec.waitFor(t, func() bool { return s.State() == SyncFailed }, "failed state")
	rec.waitTypes(t, "failed")
	if rest.count() != 1 {
		t.Fatalf("a permanent error must not be retried, fetches = %d", rest.count())
	}
	if !stream.IsPermanent(rec.last().Err) {
		t.Fatalf("the permanence of the cause must survive: %v", rec.last().Err)
	}
}

func TestSyncer_BufferOverflowRestartsTheAttempt(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	release1, release2 := make(chan struct{}), make(chan struct{})
	bids, asks := baseLevels()
	rest := newSnapshots(gate(release1, snap(5, bids, asks)), gate(release2, snap(500, bids, asks)))
	s := newSyncer(t, rest.fetch, rec, func(c *SyncerConfig) { c.MaxBuffered = 3 })
	s.Start()
	<-rest.called
	for seq := int64(6); seq <= 9; seq++ { // the 4th buffered delta exceeds the cap
		s.HandleDelta(delta(seq))
	}
	<-rest.called // a fresh fetch started
	close(release1)
	close(release2)
	rec.waitFor(t, func() bool { return s.State() == SyncSynced && s.Book().Sequence() == 500 }, "sync from the second snapshot")
	rec.waitTypes(t, "stale,snapshot")
	if !errors.Is(rec.event(0).Err, ErrBufferOverflow) {
		t.Fatalf("events = %s", rec.types())
	}
}

func TestSyncer_StreamSnapshotMode(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	s := newSyncer(t, nil, rec) // no REST: the stream pushes its own snapshot first
	s.Start()                   // harmless
	s.HandleDelta(delta(1, chg(Bid, "100", "1")))
	if s.Book().Ready() || rec.types() != "" {
		t.Fatal("deltas before the first snapshot cannot be used")
	}
	bids, asks := baseLevels()
	s.HandleSnapshot(Snapshot{Sequence: 100, Bids: bids, Asks: asks})
	if s.State() != SyncSynced {
		t.Fatalf("state=%v", s.State())
	}
	rec.waitTypes(t, "snapshot") // not "update,snapshot": the early delta was not usable
	<-s.Ready()
	s.HandleDelta(Delta{Start: 100, End: 100})                                         // stale
	s.HandleDelta(Delta{Start: 98, End: 105, Changes: []Change{chg(Ask, "101", "9")}}) // overlapping range
	rec.waitTypes(t, "snapshot,update")
	if s.Book().Sequence() != 105 {
		t.Fatalf("seq=%d", s.Book().Sequence())
	}
	s.HandleDelta(delta(200, chg(Ask, "101", "1"))) // gap: wait for the next pushed snapshot, no REST
	rec.waitTypes(t, "snapshot,update,stale")
	if s.State() != SyncIdle || s.Book().Ready() {
		t.Fatalf("state=%v", s.State())
	}
	s.HandleDelta(delta(201)) // ignored while waiting
	s.HandleSnapshot(Snapshot{Sequence: 300, Bids: bids, Asks: asks})
	rec.waitTypes(t, "snapshot,update,stale,snapshot")
	if s.State() != SyncSynced || s.Book().Sequence() != 300 {
		t.Fatalf("state=%v seq=%d events=%s", s.State(), s.Book().Sequence(), rec.types())
	}
	s.HandleReset()
	rec.waitTypes(t, "snapshot,update,stale,snapshot,stale")
	if s.State() != SyncIdle || !errors.Is(rec.last().Err, ErrReconnected) {
		t.Fatalf("state=%v last=%+v", s.State(), rec.last())
	}
	s.HandleSnapshot(Snapshot{Sequence: 400, Bids: bids, Asks: asks}) // the new connection pushes a snapshot first
	if s.State() != SyncSynced || s.Book().Sequence() != 400 {
		t.Fatalf("state=%v seq=%d", s.State(), s.Book().Sequence())
	}
	s.HandleSnapshot(Snapshot{Sequence: 1, Bids: []Level{lv("bad", "1")}}) // invalid data: stale, not synced
	rec.waitTypes(t, "snapshot,update,stale,snapshot,stale,snapshot,stale")
	if s.State() != SyncIdle || !errors.Is(rec.last().Err, ErrInvalidLevel) {
		t.Fatalf("state=%v last=%+v", s.State(), rec.last())
	}
}

func TestSyncer_InvalidDeltaDataResyncs(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	bids, asks := baseLevels()
	rest := newSnapshots(snap(10, bids, asks), snap(20, bids, asks))
	s := newSyncer(t, rest.fetch, rec)
	s.Start()
	rec.waitFor(t, func() bool { return s.State() == SyncSynced }, "first sync")
	s.HandleDelta(delta(11, chg(Bid, "not-a-number", "1")))
	rec.waitFor(t, func() bool { return s.State() == SyncSynced && rest.count() == 2 }, "resync after bad data")
	rec.waitTypes(t, "snapshot,stale,snapshot")
	if !errors.Is(rec.event(1).Err, ErrInvalidLevel) {
		t.Fatalf("cause = %v", rec.event(1).Err)
	}
}

func TestSyncer_MalformedDeltasNeverPoisonTheBuffer(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	release := make(chan struct{})
	bids, asks := baseLevels()
	rest := newSnapshots(gate(release, snap(10, bids, asks)), snap(10, bids, asks))
	s := newSyncer(t, rest.fetch, rec)
	// Idle: a malformed update cannot start a sync.
	s.HandleDelta(delta(11, chg(Bid, "garbage", "1")))
	if s.State() != SyncIdle || rest.count() != 0 {
		t.Fatalf("state=%v fetches=%d", s.State(), rest.count())
	}
	s.HandleDelta(delta(11, chg(Bid, "100", "11")))
	<-rest.called
	// Syncing: a malformed update restarts the attempt instead of being buffered.
	s.HandleDelta(delta(12, chg(Bid, "garbage", "1")))
	<-rest.called
	close(release)
	rec.waitFor(t, func() bool { return s.State() == SyncSynced }, "sync after discarding bad data")
	rec.waitTypes(t, "stale,snapshot")
	if !errors.Is(rec.event(0).Err, ErrInvalidLevel) {
		t.Fatalf("events = %s", rec.types())
	}
}

func TestSyncer_CloseCancelsAnInFlightFetchPromptly(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	started := make(chan struct{})
	rest := newSnapshots(func(ctx context.Context) (Snapshot, error) {
		close(started)
		<-ctx.Done()
		return Snapshot{}, ctx.Err()
	})
	s := NewSyncer(SyncerConfig{Symbol: "S", Snapshot: rest.fetch, OnEvent: rec.on})
	s.Start()
	<-started
	done := make(chan struct{})
	go func() { s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not cancel the pending fetch")
	}
	if s.State() != SyncClosed {
		t.Fatalf("state = %v", s.State())
	}
	s.HandleDelta(delta(1))
	s.HandleReset()
	s.HandleGap()
	s.HandleSnapshot(Snapshot{Sequence: 1})
	s.Start()
	if rec.types() != "" {
		t.Fatalf("a closed syncer must emit nothing: %s", rec.types())
	}
}

func TestSyncer_StaleFetchResultsAreDiscarded(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	slow := make(chan struct{})
	bids, asks := baseLevels()
	rest := newSnapshots(
		gate(slow, snap(1, []Level{lv("1", "1")}, []Level{lv("2", "1")})), // arrives late, after the stream restarted the sync
		snap(100, bids, asks),
	)
	s := newSyncer(t, rest.fetch, rec)
	s.Start()
	<-rest.called
	s.HandleGap() // abandons the first fetch and starts a second one
	rec.waitFor(t, func() bool { return s.State() == SyncSynced && s.Book().Sequence() == 100 }, "second fetch wins")
	close(slow) // the first fetch's result must be ignored
	time.Sleep(30 * time.Millisecond)
	if s.Book().Sequence() != 100 {
		t.Fatalf("a superseded snapshot overwrote the book: sequence %d", s.Book().Sequence())
	}
}

func TestSyncer_ConcurrentReadersWhileStreaming(t *testing.T) {
	wstest.CheckLeaks(t)
	rec := newRecorder()
	bids, asks := baseLevels()
	rest := newSnapshots(snap(10, bids, asks))
	s := newSyncer(t, rest.fetch, rec, func(c *SyncerConfig) { c.EventQueue = 4096 })
	s.Start()
	rec.waitFor(t, func() bool { return s.State() == SyncSynced }, "sync")
	var stop atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				_ = s.State()
				_ = s.Resyncs()
				_, _ = s.Book().Spread()
				_ = s.Book().Snapshot(3)
				runtime.Gosched() // with one CPU a spinning reader would starve the feeder
			}
		}()
	}
	for seq := int64(11); seq < 2000; seq++ {
		s.HandleDelta(delta(seq, chg(Bid, "100", fmt.Sprint(seq))))
	}
	stop.Store(true)
	wg.Wait()
	rec.waitFor(t, func() bool { return rec.count(EventUpdate) == 1989 }, "every update event")
	if s.Book().Sequence() != 1999 {
		t.Fatalf("seq=%d", s.Book().Sequence())
	}
}

func TestSyncerConfigDefaultsAndStrings(t *testing.T) {
	s := NewSyncer(SyncerConfig{Symbol: "S", RetryMin: time.Second})
	if s.cfg.MaxBuffered != 50000 || s.cfg.MaxAttempts != 8 || s.cfg.ResetGrace != time.Second || s.cfg.RetryMax < s.cfg.RetryMin {
		t.Fatalf("defaults: %+v", s.cfg)
	}
	for ty, want := range map[EventType]string{EventSnapshot: "snapshot", EventUpdate: "update", EventStale: "stale", EventFailed: "failed", EventType(0): "unknown"} {
		if ty.String() != want {
			t.Errorf("EventType(%d) = %q", ty, ty.String())
		}
	}
	for st, want := range map[SyncState]string{SyncIdle: "idle", SyncSyncing: "syncing", SyncSynced: "synced", SyncFailed: "failed", SyncClosed: "closed", SyncState(9): "unknown"} {
		if st.String() != want {
			t.Errorf("SyncState(%d) = %q", st, st.String())
		}
	}
	s.Close()
}
