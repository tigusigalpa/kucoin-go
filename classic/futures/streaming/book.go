package streaming

import (
	"context"
	"sync/atomic"

	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/websocket/classic"
)

// BookStream is a managed local order book of one Futures symbol.
//
// It embeds the Subscription of orderbook.Event values: every applied update,
// every (re)initialisation from a snapshot and every loss of synchronisation is
// delivered in order on C(). The live book is available at any time through
// Book(); read it after Ready() is closed.
//
//	book, err := session.SubscribeOrderBook(ctx, "XBTUSDTM")
//	if err != nil { ... }
//	defer book.Close()
//	<-book.Ready()
//	bid, _ := book.Book().BestBid()
//	for ev := range book.C() { ... }
//
// Reading C() is optional: the book follows the stream whether or not anybody
// reads the events, so an application that only polls Book() may ignore them. A
// consumer that falls more than the subscription buffer (stream.WithBuffer,
// default 1024) events behind has the backlog of updates replaced by a single
// orderbook.EventSnapshot; such events are counted by Dropped. A consumer that
// keeps a copy of the book reloads it with ev.Book.SnapshotIfReady on every
// EventSnapshot, treats EventStale as "my copy is invalid" and applies an update
// only when ev.Sequence is newer than its copy (see orderbook.Event). With
// stream.WithOverflow(stream.FailSubscription) the subscription ends with
// stream.ErrSlowConsumer instead.
type BookStream struct {
	*stream.Subscription[orderbook.Event]
	syncer *orderbook.Syncer
}

// Book returns the live order book. It is safe to read from any goroutine and
// always reflects the newest applied update; its Ready method is false while the
// book is (re)synchronising.
func (b *BookStream) Book() *orderbook.Book { return b.syncer.Book() }

// Ready is closed once the book was synchronised for the first time.
func (b *BookStream) Ready() <-chan struct{} { return b.syncer.Ready() }

// State returns the synchronisation state.
func (b *BookStream) State() orderbook.SyncState { return b.syncer.State() }

// Resyncs returns how many times synchronisation was lost and restarted.
func (b *BookStream) Resyncs() uint64 { return b.syncer.Resyncs() }

// SubscribeOrderBook maintains a local order book of symbol using KuCoin's
// documented procedure: subscribe to the level-2 incremental feed
// (/contractMarket/level2:{symbol}), buffer it, fetch the REST snapshot, drop
// updates the snapshot already contains and replay the rest. Every update must
// follow the previous sequence number by exactly one (the live feed does, and the
// documentation requires it); on a gap, after a reconnect, or when updates were
// dropped before they reached the book (this process could not keep up with the
// feed), the book is cleared, an orderbook.EventStale is delivered and the
// procedure restarts on its own.
//
// If the snapshot cannot be obtained repeatedly the subscription ends with an
// error matching stream.ErrResyncFailed.
func (s *Session) SubscribeOrderBook(ctx context.Context, symbol string, opts ...stream.SubscribeOption) (*BookStream, error) {
	return s.SubscribeOrderBookTuned(ctx, symbol, orderbook.Tuning{}, opts...)
}

// SubscribeOrderBookTuned is SubscribeOrderBook with explicit control over how the
// book recovers: snapshot retry backoff and attempt limit, the update buffer size
// and the grace period after a reconnect. Zero fields keep the defaults.
func (s *Session) SubscribeOrderBookTuned(ctx context.Context, symbol string, tuning orderbook.Tuning, opts ...stream.SubscribeOption) (*BookStream, error) {
	if s.snapshot == nil {
		return nil, ErrNoSnapshotSource
	}
	if _, err := joinSymbols([]string{symbol}, identity); err != nil {
		return nil, err
	}
	topic := "/contractMarket/level2:" + symbol

	var handle atomic.Pointer[classic.Handle]
	closeHandle := func() error {
		if h := handle.Load(); h != nil {
			return h.Close()
		}
		return nil
	}
	sub := stream.NewSubscription[orderbook.Event](topic, 0, closeHandle)
	cfg := orderbook.SyncerConfig{
		Symbol: symbol,
		Snapshot: func(ctx context.Context) (orderbook.Snapshot, error) {
			return s.snapshot(ctx, symbol)
		},
	}
	cfg.DeliverTo(sub, func() { _ = closeHandle() })
	// The subscription options also govern the events: a consumer that falls
	// further behind than the buffer either gets a reload marker (the default) or,
	// with stream.FailSubscription, loses the subscription.
	sc := stream.NewSubscribeConfig(opts...)
	if sc.Buffer > 0 {
		cfg.EventQueue = sc.Buffer
	}
	if sc.OverflowSet {
		cfg.EventOverflow = sc.Overflow
	}
	tuning.Apply(&cfg)
	syncer := orderbook.NewSyncer(cfg)
	h, err := s.client.SubscribeHandler(ctx, topic, false, &bookHandler{sub: sub, syncer: syncer, report: s.client.ReportDecodeError}, opts...)
	if err != nil {
		syncer.Close()
		return nil, err
	}
	handle.Store(h)
	sub.BindDropped(func() uint64 { return h.Dropped() + syncer.DroppedEvents() })
	select {
	case <-sub.Done(): // the book already failed while the handle was being created
		_ = h.Close()
	default:
		syncer.Start()
	}
	return &BookStream{Subscription: sub, syncer: syncer}, nil
}

// bookHandler feeds a Syncer from the level-2 topic.
type bookHandler struct {
	sub    *stream.Subscription[orderbook.Event]
	syncer *orderbook.Syncer
	report func(error)
}

func (h *bookHandler) OnFrame(f stream.Frame) {
	m, _ := f.Msg.(*classic.Message)
	if m == nil {
		return
	}
	change, ok, err := decodeOrderBookChange(m)
	if err != nil {
		// An update that cannot be read is an update that is missing.
		h.report(&stream.DecodeError{Channel: m.Topic, Raw: f.Raw, Err: err})
		h.syncer.HandleLoss(err)
		return
	}
	if !ok {
		return
	}
	seq := int64(change.Sequence)
	h.syncer.HandleDelta(orderbook.Delta{
		Symbol:  change.Symbol,
		Start:   seq,
		End:     seq,
		Changes: []orderbook.Change{{Side: change.Side, Price: change.Price, Size: change.Size}},
	})
}

func (h *bookHandler) OnReset(uint64) { h.syncer.HandleReset() }

func (h *bookHandler) OnGap(dropped uint64) {
	h.sub.AddDropped(dropped)
	h.syncer.HandleGap()
}

func (h *bookHandler) OnAbort(err error) { h.sub.Finish(err) }

func (h *bookHandler) OnClosed() {
	h.syncer.Close()
	h.sub.Seal()
}
