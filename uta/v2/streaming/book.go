package streaming

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/websocket/uta"
)

// Defaults of the managed order book of the 10ms feed. KuCoin takes a few seconds
// (measured: one to five) to push the first snapshot of an increment@10ms
// subscription, hence the generous wait before subscribing again.
const (
	defaultRetryMin         = 250 * time.Millisecond
	defaultRetryMax         = 5 * time.Second
	defaultMaxAttempts      = 8
	defaultSnapshotWait     = 20 * time.Second
	resubscribeJitterDivide = 4
)

// BookStream is a managed local order book of one symbol.
//
// It embeds the Subscription of orderbook.Event values: every applied update, every
// (re)initialisation from a snapshot and every loss of synchronisation is delivered
// in order on C(). The live book is available at any time through Book(); read it
// after Ready() is closed. Events carry the live book too, which may already be
// newer than the event being handled.
//
//	book, err := session.SubscribeOrderBook(ctx, "XBTUSDTM")
//	if err != nil { ... }
//	defer book.Close()
//	<-book.Ready()
//	bid, _ := book.Book().BestBid()
//	for ev := range book.C() { ... }
//
// The book follows the stream whether or not anybody reads C(): an application
// that only polls Book() may ignore the events. A consumer that falls more than
// the subscription buffer (stream.WithBuffer, 1024 by default) events behind has
// the backlog of updates replaced by a single orderbook.EventSnapshot, a marker
// that says "reload from ev.Book"; the discarded updates are counted by Dropped. A
// consumer that keeps a copy of the book reloads it with ev.Book.SnapshotIfReady on
// every EventSnapshot (one the book is not ready for is ignored: another follows),
// treats EventStale as "my copy is invalid" and applies an update only when its
// copy is valid and ev.Sequence is newer than the copy (see orderbook.Event). With
// stream.WithOverflow(stream.FailSubscription) the subscription ends with
// stream.ErrSlowConsumer instead.
type BookStream struct {
	*stream.Subscription[orderbook.Event]
	core *bookCore
}

// Book returns the live order book. It is safe to read from any goroutine and
// always reflects the newest applied update; its Ready method is false while the
// book is (re)synchronising.
func (b *BookStream) Book() *orderbook.Book { return b.core.syncer.Book() }

// Ready is closed once the book was synchronised for the first time.
func (b *BookStream) Ready() <-chan struct{} { return b.core.syncer.Ready() }

// State returns the synchronisation state.
func (b *BookStream) State() orderbook.SyncState { return b.core.syncer.State() }

// Resyncs returns how many times synchronisation was lost and restarted.
func (b *BookStream) Resyncs() uint64 { return b.core.syncer.Resyncs() }

// SubscribeOrderBook maintains a local order book of symbol from KuCoin's 10ms
// incremental feed (obu, depth increment@10ms): the server pushes a snapshot first
// and deltas afterwards, so no REST call is needed. The book follows the sequence
// rule of the feed: a delta applies when it starts at or before the book's
// sequence plus one and ends after it; one that ends at or before it is stale and
// ignored.
//
// When synchronisation is lost — a sequence gap, an update that cannot be decoded,
// updates the connection had to drop — the book is cleared, an orderbook.EventStale
// is delivered and the book subscribes again, which makes KuCoin push a fresh
// snapshot (it arrives a few seconds later). After a reconnect the
// new connection's snapshot restores the book. If the snapshot does not arrive or
// subscribing keeps failing, the subscription ends with an error matching
// stream.ErrResyncFailed; no orderbook.EventFailed is delivered for this feed, the
// subscription simply ends.
//
// The book subscribes to the symbol's increment@10ms feed exclusively: a plain
// SubscribeOrderBookUpdates of the same symbol and depth cannot coexist with it on
// one session.
//
// Docs: https://www.kucoin.com/docs-new/3470354w0
func (s *Session) SubscribeOrderBook(ctx context.Context, symbol string, opts ...stream.SubscribeOption) (*BookStream, error) {
	return s.subscribeBook(ctx, "SubscribeOrderBook", symbol, DepthIncrement10ms, orderbook.Tuning{}, opts)
}

// SubscribeOrderBookTuned is SubscribeOrderBook with explicit control over how the
// book recovers. For this feed, which pushes its own snapshots, RetryMin and
// RetryMax pace the resubscriptions (250ms to 5s by default, doubling per
// consecutive failure), MaxAttempts is how many consecutive resubscriptions may
// fail before the book gives up (8 by default) and ResetGrace is how long to wait
// for a snapshot after subscribing before subscribing again (20s by default).
// MaxBuffered has no use here. Zero fields keep the defaults.
//
// Docs: https://www.kucoin.com/docs-new/3470354w0
func (s *Session) SubscribeOrderBookTuned(ctx context.Context, symbol string, tuning orderbook.Tuning, opts ...stream.SubscribeOption) (*BookStream, error) {
	return s.subscribeBook(ctx, "SubscribeOrderBookTuned", symbol, DepthIncrement10ms, tuning, opts)
}

// SubscribeOrderBookIncrement maintains a local order book of symbol from the
// real-time incremental feed (obu, depth increment) and a REST snapshot, using
// KuCoin's documented procedure: subscribe, buffer the deltas, fetch the snapshot
// through the SnapshotFunc the Service was built with, drop the deltas the
// snapshot already contains and replay the rest. On a sequence gap, a reconnect
// or dropped updates the book is cleared, an orderbook.EventStale is delivered and
// the procedure restarts on its own. If the snapshot cannot be obtained
// repeatedly the subscription ends with an error matching stream.ErrResyncFailed,
// after an orderbook.EventFailed.
//
// It returns ErrNoSnapshotSource when the Service has no SnapshotFunc.
//
// Deprecated: KuCoin announced the removal of the increment depth on 2026-07-15
// and asks for SubscribeOrderBook (increment@10ms) instead. It still works at the
// time of writing.
//
// Docs: https://www.kucoin.com/docs-new/3470354w0
func (s *Session) SubscribeOrderBookIncrement(ctx context.Context, symbol string, opts ...stream.SubscribeOption) (*BookStream, error) {
	return s.subscribeBook(ctx, "SubscribeOrderBookIncrement", symbol, DepthIncrement, orderbook.Tuning{}, opts)
}

// SubscribeOrderBookIncrementTuned is SubscribeOrderBookIncrement with explicit
// control over snapshot retries, the update buffer size and the grace period after
// a reconnect, as orderbook.Tuning describes them. Zero fields keep the defaults.
//
// Deprecated: see SubscribeOrderBookIncrement.
//
// Docs: https://www.kucoin.com/docs-new/3470354w0
func (s *Session) SubscribeOrderBookIncrementTuned(ctx context.Context, symbol string, tuning orderbook.Tuning, opts ...stream.SubscribeOption) (*BookStream, error) {
	return s.subscribeBook(ctx, "SubscribeOrderBookIncrementTuned", symbol, DepthIncrement, tuning, opts)
}

func (s *Session) subscribeBook(ctx context.Context, method, symbol string, depth Depth, tuning orderbook.Tuning, opts []stream.SubscribeOption) (*BookStream, error) {
	if err := s.market(method); err != nil {
		return nil, err
	}
	if err := checkSymbol(symbol); err != nil {
		return nil, err
	}
	rest := depth == DepthIncrement
	if rest && s.snapshot == nil {
		return nil, ErrNoSnapshotSource
	}
	spec := uta.SubscribeSpec{Channel: channelOrderBook, TradeType: s.tradeType, Symbols: []string{symbol}, Depth: string(depth)}
	cfg := orderbook.SyncerConfig{Symbol: symbol}
	if rest {
		tradeType := s.tradeType
		cfg.Snapshot = func(ctx context.Context) (orderbook.Snapshot, error) {
			return s.snapshot(ctx, tradeType, symbol)
		}
	}
	tuning.Apply(&cfg)

	core := newBookCore(s.client, spec, stream.NewSubscribeConfig(opts...), cfg, rest)
	handler := newBookHandler(core)
	handle, err := s.client.SubscribeHandler(ctx, spec, handler, core.opts...)
	if err != nil {
		handler.retired.Store(true) // the engine has already told it that the subscription failed
		core.abandon()
		return nil, err
	}
	core.attach(handle, handler)
	return &BookStream{Subscription: core.sub, core: core}, nil
}

// bookCore is the state shared by the successive subscriptions of one managed
// book. The book of the deprecated increment depth (rest) has a single
// subscription; the book of the 10ms feed is resubscribed by a worker goroutine
// whenever it needs a fresh snapshot.
type bookCore struct {
	client *uta.Client
	spec   uta.SubscribeSpec
	opts   []stream.SubscribeOption // of the connection-level subscription
	rest   bool

	sub    *stream.Subscription[orderbook.Event]
	syncer *orderbook.Syncer

	retryMin, retryMax time.Duration
	maxAttempts        int
	snapshotWait       time.Duration

	ctx    context.Context
	cancel context.CancelFunc

	mu          sync.Mutex
	handle      *uta.Handle
	handler     *bookHandler
	droppedBase uint64
	ended       bool
	watchdog    *time.Timer

	// awaiting is true from a (re)subscription or a reset until the stream's
	// snapshot has been applied; epoch identifies the current wait for the
	// watchdog; failures counts consecutive resubscriptions that brought no
	// snapshot; forced asks the worker to resubscribe even while a snapshot is
	// awaited.
	awaiting atomic.Bool
	epoch    atomic.Uint64
	failures atomic.Int64
	forced   atomic.Bool
	kick     chan struct{}

	// attached is closed when the first subscription has been recorded; the worker
	// does not touch the book's subscriptions before that.
	attached chan struct{}

	finalized sync.Once
}

func newBookCore(client *uta.Client, spec uta.SubscribeSpec, sc stream.SubscribeConfig, cfg orderbook.SyncerConfig, rest bool) *bookCore {
	c := &bookCore{
		client:       client,
		spec:         spec,
		rest:         rest,
		retryMin:     positiveDuration(cfg.RetryMin, defaultRetryMin),
		retryMax:     positiveDuration(cfg.RetryMax, defaultRetryMax),
		maxAttempts:  defaultMaxAttempts,
		snapshotWait: positiveDuration(cfg.ResetGrace, defaultSnapshotWait),
		kick:         make(chan struct{}, 1),
		attached:     make(chan struct{}),
	}
	if cfg.MaxAttempts > 0 {
		c.maxAttempts = cfg.MaxAttempts
	}
	if c.retryMax < c.retryMin {
		c.retryMax = c.retryMin
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())

	c.sub = stream.NewSubscription[orderbook.Event](spec.Name(), 0, c.close)
	cfg.DeliverTo(c.sub, c.end)
	// The consumer's buffer and overflow policy apply to the queue of undelivered
	// events: a consumer that falls further behind either gets a reload marker (the
	// default) or, with stream.FailSubscription, loses the subscription. The
	// connection's own frame queue keeps its default size, because the book takes
	// frames off it without delay and a dropped frame would cost a new snapshot,
	// which takes seconds to arrive; only the overflow policy applies to it too.
	if sc.Buffer > 0 {
		cfg.EventQueue = sc.Buffer
	}
	if sc.OverflowSet {
		cfg.EventOverflow = sc.Overflow
		c.opts = []stream.SubscribeOption{stream.WithOverflow(sc.Overflow)}
	}
	c.syncer = orderbook.NewSyncer(cfg)
	if !rest {
		// Wait for the stream's first snapshot from now on: it may arrive before the
		// subscription call returns.
		c.armWatchdog()
		go c.run()
	}
	return c
}

func positiveDuration(d, fallback time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return fallback
}

// attach records the first subscription once it exists.
func (c *bookCore) attach(h *uta.Handle, handler *bookHandler) {
	c.mu.Lock()
	c.handle, c.handler = h, handler
	c.mu.Unlock()
	c.sub.BindDropped(c.droppedTotal)
	handler.arm()
	close(c.attached)
	if c.rest {
		c.syncer.Start()
	}
}

// abandon releases everything of a book whose first subscription failed.
func (c *bookCore) abandon() {
	c.finish(nil)
	c.finalize()
}

// droppedTotal is the number of updates discarded for this book: by the
// connection's queues (all subscriptions so far) and by the event queue.
func (c *bookCore) droppedTotal() uint64 {
	c.mu.Lock()
	n := c.droppedBase
	if c.handle != nil {
		n += c.handle.Dropped()
	}
	c.mu.Unlock()
	return n + c.syncer.DroppedEvents()
}

// synced records that the stream's snapshot has been applied.
func (c *bookCore) synced() {
	c.awaiting.Store(false)
	c.failures.Store(0)
	c.mu.Lock()
	if c.watchdog != nil {
		c.watchdog.Stop()
		c.watchdog = nil
	}
	c.mu.Unlock()
}

// armWatchdog starts waiting for the stream's snapshot: if it has not arrived
// when snapshotWait elapses, the book subscribes again.
func (c *bookCore) armWatchdog() {
	c.awaiting.Store(true)
	epoch := c.epoch.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ended {
		return
	}
	if c.watchdog != nil {
		c.watchdog.Stop()
	}
	c.watchdog = time.AfterFunc(c.snapshotWait, func() {
		if c.awaiting.Load() && c.epoch.Load() == epoch && c.syncer.State() != orderbook.SyncSynced {
			c.failures.Add(1)
			c.requestResubscribe(true)
		}
	})
}

// requestResubscribe asks the worker for a fresh subscription. A forced request is
// honoured even while a snapshot is awaited; an ordinary one only when the book
// lost its place in the stream.
func (c *bookCore) requestResubscribe(force bool) {
	if c.rest {
		return
	}
	if force {
		c.forced.Store(true)
	}
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// afterSnapshot is called by the handler after it fed a snapshot to the syncer.
func (c *bookCore) afterSnapshot() {
	if c.rest {
		return
	}
	if c.syncer.State() == orderbook.SyncSynced {
		c.synced()
		return
	}
	c.requestResubscribe(true) // the snapshot could not be applied
}

// afterLoss is called by the handler after an undecodable update or a gap: a book
// that is not synchronised then has to subscribe again, even if it was waiting for
// a snapshot, as the lost frame may have been that snapshot.
func (c *bookCore) afterLoss() {
	if c.rest {
		return
	}
	if c.syncer.State() != orderbook.SyncSynced {
		c.requestResubscribe(true)
	}
}

// run is the worker of a book of the 10ms feed: it subscribes again whenever the
// book needs a fresh snapshot.
func (c *bookCore) run() {
	defer func() {
		c.mu.Lock()
		live := c.handle != nil
		c.mu.Unlock()
		if !live {
			c.finalize()
		}
	}()
	select {
	case <-c.attached:
	case <-c.ctx.Done():
		return
	}
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.kick:
		}
		force := c.forced.Swap(false)
		if !force && c.recovered() {
			continue
		}
		if !c.resubscribe(force) {
			return
		}
	}
}

// recovered reports that nothing needs to be done: a snapshot is on its way or the
// book is synchronised again.
func (c *bookCore) recovered() bool {
	return c.awaiting.Load() || c.syncer.State() == orderbook.SyncSynced
}

// resubscribe replaces the subscription with a new one, which makes KuCoin push a
// new snapshot, unless the book recovered while it waited (and force is not set).
// It reports false when the book has ended.
func (c *bookCore) resubscribe(force bool) bool {
	for attempt := 0; ; attempt++ {
		if int(c.failures.Load()) >= c.maxAttempts {
			c.failBook(errors.New("no snapshot of the stream"))
			return false
		}
		if !sleepCtx(c.ctx, c.backoff(int(c.failures.Load()))) {
			return false
		}
		if attempt == 0 && !force && c.recovered() {
			return true // the stream pushed a snapshot on its own while we waited
		}
		c.mu.Lock()
		if c.ended {
			c.mu.Unlock()
			return false
		}
		old, oldHandler := c.handle, c.handler
		c.handle, c.handler = nil, nil
		if old != nil {
			c.droppedBase += old.Dropped()
		}
		c.mu.Unlock()
		if oldHandler != nil {
			oldHandler.retired.Store(true)
		}
		if old != nil {
			_ = old.Close()
		}

		handler := newBookHandler(c)
		c.armWatchdog()
		handle, err := c.client.SubscribeHandler(c.ctx, c.spec, handler, c.opts...)
		if err != nil {
			handler.retired.Store(true) // the engine has already told it that the subscription failed
			if c.ctx.Err() != nil || c.isEnded() {
				return false
			}
			if errors.Is(err, stream.ErrClosed) {
				c.finish(c.client.Err())
				return false
			}
			if int(c.failures.Add(1)) >= c.maxAttempts {
				c.failBook(err)
				return false
			}
			continue
		}
		c.mu.Lock()
		if c.ended {
			c.mu.Unlock()
			handler.retired.Store(true)
			_ = handle.Close()
			return false // the worker's exit releases the book
		}
		c.handle, c.handler = handle, handler
		c.mu.Unlock()
		handler.arm()
		return true
	}
}

// backoff returns the pause before the next resubscription after the given
// number of consecutive failures.
func (c *bookCore) backoff(failures int) time.Duration {
	d := c.retryMin
	for i := 0; i < failures && d < c.retryMax; i++ {
		d *= 2
	}
	if d > c.retryMax {
		d = c.retryMax
	}
	return d - time.Duration(rand.Int63n(int64(d)/resubscribeJitterDivide+1)) //nolint:gosec // retry pacing only
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// failBook ends the book because subscribing again keeps failing: the
// subscription ends with an error matching stream.ErrResyncFailed.
func (c *bookCore) failBook(cause error) {
	c.finish(fmt.Errorf("%w: %w", stream.ErrResyncFailed, cause))
	_ = c.closeCurrent()
}

func (c *bookCore) isEnded() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ended
}

// markEnded records that the book is over and stops the watchdog.
func (c *bookCore) markEnded() {
	c.mu.Lock()
	c.ended = true
	if c.watchdog != nil {
		c.watchdog.Stop()
		c.watchdog = nil
	}
	c.mu.Unlock()
}

// finish ends the book's subscription with err (nil for a requested end) and
// stops the worker.
func (c *bookCore) finish(err error) {
	c.markEnded()
	c.sub.Finish(err)
	c.cancel()
}

// end releases what sits under the subscription of a book whose syncer failed; the
// syncer's delivery has already finished the subscription. It may be called more
// than once.
func (c *bookCore) end() {
	c.markEnded()
	c.cancel()
	_ = c.closeCurrent()
}

// finalize releases the syncer, which waits for its event delivery, and then seals
// the subscription; it runs once, when the last subscription has ended.
func (c *bookCore) finalize() {
	c.finalized.Do(func() {
		c.cancel()
		c.syncer.Close()
		c.sub.Seal()
	})
}

// closeCurrent closes the live subscription, if any.
func (c *bookCore) closeCurrent() error {
	c.mu.Lock()
	h := c.handle
	c.mu.Unlock()
	if h == nil {
		return nil
	}
	return h.Close()
}

// close is the Close function of the book's Subscription.
func (c *bookCore) close() error {
	c.finish(nil)
	return c.closeCurrent()
}

// bookHandler feeds the syncer from one subscription of the obu channel.
//
// The engine reports the end of a subscription, also one that failed while it was
// being created, to its handler: OnAbort, and later OnClosed. While the
// subscription call has not returned, the handler only records them; arm replays
// what it recorded, and retired (set when the call failed, or when the book has
// replaced the subscription) makes it ignore everything.
type bookHandler struct {
	core    *bookCore
	retired atomic.Bool

	mu       sync.Mutex
	pending  bool // the subscription call has not returned yet
	aborted  bool
	abortErr error
	closed   bool
}

func newBookHandler(c *bookCore) *bookHandler { return &bookHandler{core: c, pending: true} }

// arm is called when the subscription call returned successfully: an end that
// happened in the meantime is acted upon now.
func (h *bookHandler) arm() {
	h.mu.Lock()
	h.pending = false
	aborted, err, closed := h.aborted, h.abortErr, h.closed
	h.mu.Unlock()
	if h.retired.Load() {
		return
	}
	if aborted {
		h.core.finish(err)
	}
	if closed {
		h.core.finalize()
	}
}

func (h *bookHandler) OnFrame(f stream.Frame) {
	if h.retired.Load() {
		return
	}
	push, _ := f.Msg.(*uta.Push)
	if push == nil {
		return
	}
	c := h.core
	update, err := decodeBookUpdate(push)
	if err != nil {
		// An update that cannot be read is an update that is missing.
		c.client.ReportDecodeError(&stream.DecodeError{Channel: f.Route, Raw: f.Raw, Err: err})
		c.syncer.HandleLoss(err)
		c.afterLoss()
		return
	}
	if update.Kind == UpdateSnapshot {
		c.syncer.HandleSnapshot(update.ToSnapshot())
		c.afterSnapshot()
		return
	}
	resyncs := c.syncer.Resyncs()
	c.syncer.HandleDelta(update.ToDelta())
	if !c.rest && c.syncer.Resyncs() != resyncs {
		// The delta did not continue the book (a gap, an invalid level): the book
		// was cleared and only a new snapshot can restore it.
		c.requestResubscribe(false)
	}
}

func (h *bookHandler) OnReset(uint64) {
	if h.retired.Load() {
		return
	}
	// The engine subscribes again after a reconnect, which makes KuCoin push a new
	// snapshot; the book is stale until it arrives.
	if !h.core.rest {
		h.core.armWatchdog()
	}
	h.core.syncer.HandleReset()
}

func (h *bookHandler) OnGap(uint64) {
	if h.retired.Load() {
		return
	}
	h.core.syncer.HandleGap()
	h.core.afterLoss()
}

func (h *bookHandler) OnAbort(err error) {
	if h.retired.Load() {
		return
	}
	h.mu.Lock()
	if h.pending {
		h.aborted, h.abortErr = true, err
		h.mu.Unlock()
		return
	}
	h.mu.Unlock()
	h.core.finish(err)
}

func (h *bookHandler) OnClosed() {
	if h.retired.Load() {
		return
	}
	h.mu.Lock()
	if h.pending {
		h.closed = true
		h.mu.Unlock()
		return
	}
	h.mu.Unlock()
	h.core.finalize()
}
