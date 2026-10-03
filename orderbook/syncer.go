package orderbook

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tigusigalpa/kucoin-go/stream"
)

// Causes reported with EventStale.
var (
	// ErrReconnected is the cause when the WebSocket connection was lost and
	// re-established: updates were missed while it was down.
	ErrReconnected = errors.New("kucoin: order book stream reconnected")
	// ErrUpdatesDropped is the cause when the consumer was too slow and queued
	// updates were dropped.
	ErrUpdatesDropped = errors.New("kucoin: order book updates were dropped")
	// ErrBufferOverflow is the cause when too many updates piled up while a
	// snapshot was being fetched.
	ErrBufferOverflow = errors.New("kucoin: order book update buffer overflow")
)

// SnapshotFunc fetches a fresh full snapshot of the book, usually over REST.
type SnapshotFunc func(ctx context.Context) (Snapshot, error)

// EventType identifies a Syncer Event.
type EventType int

// Syncer event types.
const (
	// EventSnapshot: the book was (re)initialised and is ready to use. It is also
	// the reload marker that replaces the queued updates of a consumer that fell
	// behind (see SyncerConfig.EventOverflow): either way, a consumer that mirrors
	// the book reloads its copy from Event.Book with SnapshotIfReady.
	EventSnapshot EventType = iota + 1
	// EventUpdate: a delta was applied; Changes lists it.
	EventUpdate
	// EventStale: synchronisation was lost (Err says why). The book is cleared
	// and is being rebuilt; an EventSnapshot follows when it is ready again.
	EventStale
	// EventFailed: the Syncer stopped for good; Err says why. It wraps
	// stream.ErrResyncFailed when synchronisation could not be restored and
	// stream.ErrSlowConsumer when the event consumer fell too far behind with
	// SyncerConfig.EventOverflow set to stream.FailSubscription.
	EventFailed
)

// String returns the event name.
func (t EventType) String() string {
	switch t {
	case EventSnapshot:
		return "snapshot"
	case EventUpdate:
		return "update"
	case EventStale:
		return "stale"
	case EventFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Event is something that happened to the book. Events are delivered in the
// order they occurred.
type Event struct {
	Type   EventType
	Symbol string
	// Sequence is the book's sequence after the event.
	Sequence int64
	// Changes are the price-level changes of an EventUpdate.
	Changes []Change
	// Err is the cause of EventStale and EventFailed.
	Err error
	// Book is the live book. It always reflects the newest applied update, which
	// may be newer than the event being handled when the consumer is slow, and it
	// may even be cleared and being rebuilt by the time the event is read. A
	// consumer that mirrors the book therefore: on EventSnapshot, copies the book
	// with SnapshotIfReady and, when that reports false, ignores the event (the
	// rebuild ends in another EventSnapshot); on EventStale, regards its copy as
	// invalid; on EventUpdate, applies Changes only when its copy is valid and
	// Sequence is greater than the copy's sequence.
	Book *Book
}

// SyncState is the state of a Syncer.
type SyncState int

// Syncer states.
const (
	// SyncIdle: no snapshot yet; waiting for the stream to start (or restart).
	SyncIdle SyncState = iota
	// SyncSyncing: a snapshot is being obtained while updates are buffered.
	SyncSyncing
	// SyncSynced: the book is current and follows the stream.
	SyncSynced
	// SyncFailed: synchronisation failed permanently.
	SyncFailed
	// SyncClosed: the Syncer was closed.
	SyncClosed
)

// String returns the state name.
func (s SyncState) String() string {
	switch s {
	case SyncIdle:
		return "idle"
	case SyncSyncing:
		return "syncing"
	case SyncSynced:
		return "synced"
	case SyncFailed:
		return "failed"
	case SyncClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// SyncerConfig configures a Syncer.
type SyncerConfig struct {
	// Symbol is the book's symbol.
	Symbol string
	// Snapshot fetches full snapshots (REST). Leave it nil for feeds that push
	// their own snapshots, such as UTA's increment@10ms order book; those are fed
	// with HandleSnapshot.
	Snapshot SnapshotFunc
	// OnEvent receives the events in order. It is called from a goroutine of the
	// Syncer's own, with no lock held, and may block: a slow OnEvent never stalls
	// the book or the goroutine that feeds the Syncer (see EventQueue). It must
	// return promptly once its consumer is gone, because Close waits for it.
	OnEvent func(Event)
	// OnFail, if set, is called once, from a goroutine of its own, when the Syncer
	// stops for good (the cause is that of its EventFailed). Unlike OnEvent it does
	// not wait behind events a consumer has not read, so it is the place to end a
	// subscription whose consumer may be absent. Close waits for it to return.
	OnFail func(err error)
	// EventQueue is how many events may wait for a slow OnEvent before
	// EventOverflow applies. Default 1024.
	EventQueue int
	// EventOverflow decides what happens when the queue of undelivered events is
	// full. stream.DropOldest and stream.DropNewest (the zero value is DropOldest)
	// collapse the queued updates into one EventSnapshot marker that tells the
	// consumer to reload from the live book: the book itself keeps following the
	// stream, and the discarded updates are counted by DroppedEvents.
	// stream.FailSubscription stops the Syncer with an EventFailed wrapping
	// stream.ErrSlowConsumer instead, for consumers that must see every update.
	EventOverflow stream.OverflowPolicy
	// MaxBuffered caps the updates buffered while a snapshot is fetched; beyond it
	// the attempt is restarted. Default 50000.
	MaxBuffered int
	// RetryMin and RetryMax bound the backoff between failed snapshot fetches.
	// Defaults 250ms and 5s.
	RetryMin, RetryMax time.Duration
	// MaxAttempts is how many consecutive failed fetch/alignment attempts end the
	// Syncer with EventFailed. Default 8. A permanent error (stream.Permanent)
	// ends it at once.
	MaxAttempts int
	// ResetGrace is how long to wait for the stream to resume after a reconnect
	// before fetching a snapshot anyway (for quiet symbols). Default 1s.
	ResetGrace time.Duration
}

// Tuning adjusts how a Syncer recovers; zero fields keep the defaults.
type Tuning struct {
	// MaxBuffered caps the updates buffered while a snapshot is fetched.
	MaxBuffered int
	// RetryMin and RetryMax bound the backoff between failed snapshot fetches.
	RetryMin, RetryMax time.Duration
	// MaxAttempts is how many consecutive failed snapshot attempts end the
	// Syncer with EventFailed.
	MaxAttempts int
	// ResetGrace is how long to wait for the stream to resume after a reconnect
	// before fetching a snapshot anyway.
	ResetGrace time.Duration
}

// Apply copies the non-zero fields of t into cfg.
func (t Tuning) Apply(cfg *SyncerConfig) {
	if t.MaxBuffered > 0 {
		cfg.MaxBuffered = t.MaxBuffered
	}
	if t.RetryMin > 0 {
		cfg.RetryMin = t.RetryMin
	}
	if t.RetryMax > 0 {
		cfg.RetryMax = t.RetryMax
	}
	if t.MaxAttempts > 0 {
		cfg.MaxAttempts = t.MaxAttempts
	}
	if t.ResetGrace > 0 {
		cfg.ResetGrace = t.ResetGrace
	}
}

// Syncer keeps a Book synchronised with a sequenced update stream using KuCoin's
// documented procedure: buffer the stream, obtain a snapshot, drop buffered
// updates the snapshot already contains, replay the rest, then apply updates as
// they arrive, and start over whenever the sequence breaks, the connection was
// lost or updates were dropped.
//
// It is driven from one goroutine (the subscription's delivery goroutine) that
// calls Start, HandleDelta, HandleSnapshot, HandleReset and HandleGap in stream
// order; snapshot fetches run on their own goroutine. All of its methods are safe
// for concurrent use.
type Syncer struct {
	cfg  SyncerConfig
	book *Book

	mu      sync.Mutex
	state   SyncState
	buf     []Delta
	gen     uint64
	cancel  context.CancelFunc
	timer   *time.Timer
	wg      sync.WaitGroup
	resyncs atomic.Uint64

	// The event queue and its delivery goroutine. Events are produced under mu,
	// which fixes their order, and delivered outside it, so that a consumer that
	// is slow, or that calls back into the Syncer, can never deadlock it.
	evq       []queuedEvent
	evCond    *sync.Cond // on mu; wakes the delivery goroutine
	evStarted bool
	evClosed  bool
	evDropped atomic.Uint64

	ready     chan struct{}
	readyOnce sync.Once
}

// queuedEvent is an event waiting for delivery. reload marks the synthetic
// EventSnapshot that stands in for updates dropped from a full queue.
type queuedEvent struct {
	ev     Event
	reload bool
}

// NewSyncer creates a Syncer with its own empty Book.
func NewSyncer(cfg SyncerConfig) *Syncer {
	if cfg.MaxBuffered <= 0 {
		cfg.MaxBuffered = 50000
	}
	if cfg.RetryMin <= 0 {
		cfg.RetryMin = 250 * time.Millisecond
	}
	if cfg.RetryMax < cfg.RetryMin {
		cfg.RetryMax = 5 * time.Second
		if cfg.RetryMax < cfg.RetryMin {
			cfg.RetryMax = cfg.RetryMin
		}
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 8
	}
	if cfg.ResetGrace <= 0 {
		cfg.ResetGrace = time.Second
	}
	if cfg.EventQueue <= 0 {
		cfg.EventQueue = 1024
	}
	s := &Syncer{cfg: cfg, book: New(cfg.Symbol), ready: make(chan struct{})}
	s.evCond = sync.NewCond(&s.mu)
	return s
}

// Book returns the live book.
func (s *Syncer) Book() *Book { return s.book }

// Ready is closed when the book was synchronised for the first time.
func (s *Syncer) Ready() <-chan struct{} { return s.ready }

// State returns the current state.
func (s *Syncer) State() SyncState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Resyncs returns how many times synchronisation was lost and restarted.
func (s *Syncer) Resyncs() uint64 { return s.resyncs.Load() }

// DroppedEvents returns how many update events were discarded, and replaced by a
// reload marker, because the consumer of OnEvent fell more than
// SyncerConfig.EventQueue events behind. The book itself is never affected.
func (s *Syncer) DroppedEvents() uint64 { return s.evDropped.Load() }

// Start begins the first synchronisation. Call it once the stream subscription
// is live, so every update from the snapshot onwards is buffered. Feeds that push
// their own snapshots do not need it.
func (s *Syncer) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.Snapshot == nil || s.state != SyncIdle {
		return
	}
	s.beginLocked()
}

// HandleDelta feeds one sequenced update from the stream.
func (s *Syncer) HandleDelta(d Delta) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case SyncClosed, SyncFailed:
	case SyncIdle:
		if s.cfg.Snapshot == nil {
			return // the stream will push a snapshot first; updates before it are useless
		}
		if err := d.Validate(); err != nil {
			return // malformed data cannot start a sync; wait for the next update
		}
		s.buf = append(s.buf[:0], d)
		s.beginLocked()
	case SyncSyncing:
		if s.cfg.Snapshot == nil {
			return
		}
		if err := d.Validate(); err != nil {
			s.loseLocked(err, true)
			return
		}
		s.buf = append(s.buf, d)
		if len(s.buf) > s.cfg.MaxBuffered {
			s.loseLocked(ErrBufferOverflow, true)
		}
	case SyncSynced:
		applied, err := s.book.Apply(d)
		if err != nil {
			// Only a sequence gap leaves a valid update that belongs in the new
			// buffer; malformed data is discarded.
			valid := errors.Is(err, ErrSequenceGap)
			s.loseLocked(err, true)
			if valid && s.cfg.Snapshot != nil && s.state == SyncSyncing {
				s.buf = append(s.buf[:0], d)
			}
			return
		}
		if applied {
			s.emitLocked(Event{Type: EventUpdate, Changes: d.Changes})
		}
	}
}

// HandleSnapshot feeds a snapshot pushed by the stream itself, replacing the
// book.
func (s *Syncer) HandleSnapshot(snap Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == SyncClosed || s.state == SyncFailed {
		return
	}
	s.stopFetchLocked()
	if err := s.book.Reset(snap); err != nil {
		s.loseLocked(err, false)
		return
	}
	s.buf = nil
	s.state = SyncSynced
	s.markReadyLocked()
	s.emitLocked(Event{Type: EventSnapshot})
}

// HandleReset reports that the stream's connection was lost and re-established:
// updates were missed and the book must be rebuilt.
func (s *Syncer) HandleReset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == SyncClosed || s.state == SyncFailed {
		return
	}
	s.loseLocked(ErrReconnected, false)
	if s.cfg.Snapshot != nil {
		// The first update after the reset proves the stream is live again and
		// starts the resync; for a quiet symbol, start it after a grace period.
		s.timer = time.AfterFunc(s.cfg.ResetGrace, s.kick)
	}
}

// HandleGap reports that updates were dropped because the consumer was too slow;
// the book must be rebuilt.
func (s *Syncer) HandleGap() { s.HandleLoss(ErrUpdatesDropped) }

// HandleLoss reports that an update was lost or could not be read (cause says
// why); the book must be rebuilt immediately.
func (s *Syncer) HandleLoss(cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == SyncClosed || s.state == SyncFailed {
		return
	}
	s.loseLocked(cause, true)
}

// Close stops the Syncer, discards the events still queued and waits for its
// background snapshot fetch and its event delivery to finish. It must not be
// called from OnEvent.
func (s *Syncer) Close() {
	s.mu.Lock()
	s.state = SyncClosed
	s.stopFetchLocked()
	s.buf = nil
	s.evClosed = true
	s.evq = nil
	s.evCond.Broadcast()
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *Syncer) kick() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == SyncIdle && s.cfg.Snapshot != nil {
		s.beginLocked()
	}
}

// loseLocked abandons the current synchronisation. With restart (and a snapshot
// source) a new one begins immediately.
func (s *Syncer) loseLocked(cause error, restart bool) {
	wasLive := s.state == SyncSynced || s.state == SyncSyncing
	s.stopFetchLocked()
	s.book.Clear()
	s.buf = nil
	s.state = SyncIdle
	if wasLive {
		s.resyncs.Add(1)
		s.emitLocked(Event{Type: EventStale, Err: cause})
	}
	if restart && s.cfg.Snapshot != nil && s.state == SyncIdle {
		s.beginLocked()
	}
}

// beginLocked starts a snapshot fetch with the current buffer.
func (s *Syncer) beginLocked() {
	s.stopFetchLocked()
	s.state = SyncSyncing
	s.gen++
	gen := s.gen
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Add(1)
	go s.fetch(ctx, gen)
}

func (s *Syncer) stopFetchLocked() {
	s.gen++
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
}

// fetch obtains a snapshot, retrying with backoff, and aligns the buffered
// updates with it.
func (s *Syncer) fetch(ctx context.Context, gen uint64) {
	defer s.wg.Done()
	delay := s.cfg.RetryMin
	for attempt := 1; ; attempt++ {
		snap, err := s.cfg.Snapshot(ctx)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			s.mu.Lock()
			if s.gen != gen || s.state != SyncSyncing {
				s.mu.Unlock()
				return
			}
			if err = s.alignLocked(snap); err == nil {
				s.state = SyncSynced
				s.markReadyLocked()
				s.emitLocked(Event{Type: EventSnapshot})
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
		}
		if stream.IsPermanent(err) || attempt >= s.cfg.MaxAttempts {
			s.mu.Lock()
			if s.gen == gen && s.state == SyncSyncing {
				s.failLocked(fmt.Errorf("%w: %w", stream.ErrResyncFailed, err))
			}
			s.mu.Unlock()
			return
		}
		jitter := time.Duration(rand.Int63n(int64(delay)/2 + 1)) //nolint:gosec // retry pacing only
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay - jitter):
		}
		if delay *= 2; delay > s.cfg.RetryMax {
			delay = s.cfg.RetryMax
		}
	}
}

// alignLocked loads the snapshot and replays the buffered updates on top of it.
// Updates the snapshot already contains are skipped by the sequence rule; a gap
// between the snapshot and the first buffered update means the snapshot is older
// than the buffer and must be fetched again.
func (s *Syncer) alignLocked(snap Snapshot) error {
	if err := s.book.Reset(snap); err != nil {
		return err
	}
	for _, d := range s.buf {
		if _, err := s.book.Apply(d); err != nil {
			s.book.Clear()
			return err
		}
	}
	s.buf = nil
	return nil
}

func (s *Syncer) markReadyLocked() { s.readyOnce.Do(func() { close(s.ready) }) }

// emitLocked queues an event for delivery. It never blocks; the caller holds mu.
func (s *Syncer) emitLocked(ev Event) {
	if s.cfg.OnEvent == nil || s.evClosed {
		return
	}
	ev.Symbol = s.cfg.Symbol
	ev.Book = s.book
	ev.Sequence = s.book.Sequence()
	switch {
	case ev.Type == EventUpdate && len(s.evq) >= s.cfg.EventQueue:
		if s.cfg.EventOverflow == stream.FailSubscription {
			s.failLocked(fmt.Errorf("%w: the %s order book events were not consumed fast enough", stream.ErrSlowConsumer, s.cfg.Symbol))
			return
		}
		s.collapseLocked(ev)
		return
	case ev.Type != EventUpdate && ev.Type != EventFailed && len(s.evq) >= 2*s.cfg.EventQueue:
		// A consumer that has not read for ages while the book kept resynchronising:
		// the new event describes the state of the world, so the older ones go.
		s.dropQueuedLocked(func(q queuedEvent) bool { return q.ev.Type != EventFailed })
	}
	s.pushEventLocked(queuedEvent{ev: ev})
}

// collapseLocked replaces every queued update, and the markers of earlier
// collapses, with one reload marker positioned after the queued state changes.
// ev is the update that did not fit; the marker covers it, because the book
// already includes it.
func (s *Syncer) collapseLocked(ev Event) {
	dropped := uint64(1) // ev itself
	s.dropQueuedLocked(func(q queuedEvent) bool {
		if q.ev.Type == EventUpdate {
			dropped++
			return true
		}
		return q.reload
	})
	s.evDropped.Add(dropped)
	marker := ev
	marker.Type = EventSnapshot
	marker.Changes = nil
	marker.Err = nil
	s.pushEventLocked(queuedEvent{ev: marker, reload: true})
}

// dropQueuedLocked removes the queued events for which drop returns true,
// keeping the order of the rest.
func (s *Syncer) dropQueuedLocked(drop func(queuedEvent) bool) {
	kept := s.evq[:0]
	for _, q := range s.evq {
		if !drop(q) {
			kept = append(kept, q)
		}
	}
	for i := len(kept); i < len(s.evq); i++ {
		s.evq[i] = queuedEvent{} // let the dropped events be collected
	}
	s.evq = kept
}

// failLocked stops the Syncer with err: the book is cleared, EventFailed is
// queued and OnFail is started. It does nothing once the Syncer has failed or was
// closed.
func (s *Syncer) failLocked(err error) {
	if s.state == SyncFailed || s.state == SyncClosed {
		return
	}
	s.stopFetchLocked()
	s.state = SyncFailed
	s.book.Clear()
	s.buf = nil
	s.emitLocked(Event{Type: EventFailed, Err: err})
	if s.cfg.OnFail != nil && !s.evClosed {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.cfg.OnFail(err)
		}()
	}
}

func (s *Syncer) pushEventLocked(q queuedEvent) {
	s.evq = append(s.evq, q)
	if !s.evStarted {
		s.evStarted = true
		s.wg.Add(1)
		go s.deliverLoop()
	}
	s.evCond.Signal()
}

// deliverLoop hands the queued events to OnEvent, in order, with no lock held.
// It is the only goroutine that ever calls OnEvent.
func (s *Syncer) deliverLoop() {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		for len(s.evq) == 0 && !s.evClosed {
			s.evCond.Wait()
		}
		if s.evClosed {
			s.mu.Unlock()
			return
		}
		q := s.evq[0]
		s.evq[0] = queuedEvent{}
		s.evq = s.evq[1:]
		s.mu.Unlock()
		s.cfg.OnEvent(q.ev)
	}
}
