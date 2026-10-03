package streaming

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
	"github.com/tigusigalpa/kucoin-go/types"
	"github.com/tigusigalpa/kucoin-go/websocket/classic"
)

const l2Topic = "/contractMarket/level2:XBTUSDTM"

func l2(seq int64, change string) string {
	return wstest.Message(l2Topic, "level2", fmt.Sprintf(`{"sequence":%d,"change":%q,"timestamp":1731897467182}`, seq, change))
}

func lvl(price, size string) orderbook.Level {
	return orderbook.Level{Price: types.Decimal(price), Size: types.Decimal(size)}
}

// docSnapshot is the REST snapshot of KuCoin's worked order-book example
// (sequence 16), in the number spelling of the Futures REST API.
func docSnapshot() orderbook.Snapshot {
	return orderbook.Snapshot{
		Symbol:   "XBTUSDTM",
		Sequence: 16,
		Asks:     []orderbook.Level{lvl("3988.59", "3"), lvl("3988.60", "47"), lvl("3988.61", "32"), lvl("3988.62", "8")},
		Bids:     []orderbook.Level{lvl("3988.51", "56"), lvl("3988.50", "15"), lvl("3988.49", "100"), lvl("3988.48", "10")},
	}
}

func nextEvent(t *testing.T, b *BookStream) orderbook.Event {
	t.Helper()
	return next(t, b.Subscription)
}

func expectEvents(t *testing.T, b *BookStream, want ...orderbook.EventType) []orderbook.Event {
	t.Helper()
	var out []orderbook.Event
	for _, w := range want {
		ev := nextEvent(t, b)
		if ev.Type != w {
			t.Fatalf("event %d = %v (%+v), want %v", len(out), ev.Type, ev, w)
		}
		out = append(out, ev)
	}
	return out
}

func levels(ls []orderbook.Level) string {
	s := ""
	for _, l := range ls {
		s += fmt.Sprintf("%s:%s ", l.Price, l.Size)
	}
	return s
}

func TestOrderBook_FollowsKuCoinsDocumentedProcedure(t *testing.T) {
	h := newHarness(t)
	release := make(chan struct{})
	var calls atomic.Int32
	h.snapshots = func(ctx context.Context, symbol string) (orderbook.Snapshot, error) {
		calls.Add(1)
		if symbol != "XBTUSDTM" {
			t.Errorf("snapshot requested for %q", symbol)
		}
		select {
		case <-release:
			return docSnapshot(), nil
		case <-ctx.Done():
			return orderbook.Snapshot{}, ctx.Err()
		}
	}
	// 15 and 16 are already contained in the snapshot (sequence 16) and must be
	// discarded; 17 and 18 are the documented updates.
	h.fake.OnSubscribe(l2Topic, l2(15, "3988.62,sell,8"), l2(16, "3988.59,sell,3"), l2(17, "3988.50,buy,44"), l2(18, "3988.61,sell,0"))
	book, err := h.public(t).SubscribeOrderBook(ctx5(t), "XBTUSDTM")
	if err != nil {
		t.Fatal(err)
	}
	if book.State() == orderbook.SyncSynced {
		t.Fatal("the book cannot be synchronised before the snapshot arrived")
	}
	select {
	case <-book.Ready():
		t.Fatal("not ready yet")
	default:
	}
	close(release)
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	// Updates 17 and 18 either were replayed from the buffer or arrived right
	// after the snapshot; either way they must end up in the book, and the stale
	// 15 and 16 must not.
	for book.Book().Sequence() < 18 {
		if ev := nextEvent(t, book); ev.Type != orderbook.EventUpdate {
			t.Fatalf("unexpected %v event while catching up", ev.Type)
		}
	}
	snap := book.Book().Snapshot(0)
	if snap.Sequence != 18 {
		t.Fatalf("sequence = %d, want 18", snap.Sequence)
	}
	if got := levels(snap.Bids); got != "3988.51:56 3988.5:44 3988.49:100 3988.48:10 " {
		t.Fatalf("bids = %s", got)
	}
	if got := levels(snap.Asks); got != "3988.59:3 3988.6:47 3988.62:8 " {
		t.Fatalf("asks = %s", got)
	}

	// Live updates after synchronisation arrive as Update events.
	if err := h.fake.Push(l2(19, "3988.52,buy,7")); err != nil {
		t.Fatal(err)
	}
	var ev orderbook.Event
	for { // events of updates 17/18 may still be queued ahead of this one
		ev = nextEvent(t, book)
		if ev.Type != orderbook.EventUpdate {
			t.Fatalf("unexpected %v event", ev.Type)
		}
		if ev.Sequence >= 19 {
			break
		}
	}
	if ev.Sequence != 19 || len(ev.Changes) != 1 || ev.Changes[0].Side != orderbook.Bid || ev.Changes[0].Price != "3988.52" || ev.Changes[0].Size != "7" || ev.Symbol != "XBTUSDTM" || ev.Book != book.Book() {
		t.Fatalf("update event: %+v", ev)
	}
	if bid, _ := book.Book().BestBid(); bid.Price != "3988.52" || bid.Size != "7" {
		t.Fatalf("best bid = %v", bid)
	}
	if spread, ok := book.Book().Spread(); !ok || spread != "0.07" {
		t.Fatalf("spread = %q", spread)
	}
	if calls.Load() != 1 || book.State() != orderbook.SyncSynced || book.Resyncs() != 0 {
		t.Fatalf("snapshot calls=%d state=%v resyncs=%d", calls.Load(), book.State(), book.Resyncs())
	}
}

func TestOrderBook_ASequenceGapTriggersAnAutomaticResync(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		if calls.Add(1) == 1 {
			return docSnapshot(), nil
		}
		return orderbook.Snapshot{Symbol: "XBTUSDTM", Sequence: 40, Bids: []orderbook.Level{lvl("500", "5")}, Asks: []orderbook.Level{lvl("501", "5")}}, nil
	}
	book, err := h.public(t).SubscribeOrderBook(ctx5(t), "XBTUSDTM")
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	if err := h.fake.Push(l2(17, "3988.50,buy,44")); err != nil {
		t.Fatal(err)
	}
	if err := h.fake.Push(l2(18, "3988.61,sell,0")); err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventUpdate, orderbook.EventUpdate)
	if err := h.fake.Push(l2(25, "3988.62,sell,1")); err != nil { // 19..24 were lost
		t.Fatal(err)
	}
	evs := expectEvents(t, book, orderbook.EventStale, orderbook.EventSnapshot)
	var gap *orderbook.GapError
	if !errors.Is(evs[0].Err, orderbook.ErrSequenceGap) || !errors.As(evs[0].Err, &gap) || gap.Have != 18 || gap.Start != 25 {
		t.Fatalf("stale cause: %v", evs[0].Err)
	}
	if bid, _ := book.Book().BestBid(); bid.Price != "500" || book.Book().Sequence() != 40 {
		t.Fatalf("the book must come from the second snapshot: %v seq=%d", bid, book.Book().Sequence())
	}
	if book.Resyncs() != 1 || calls.Load() != 2 {
		t.Fatalf("resyncs=%d snapshot calls=%d", book.Resyncs(), calls.Load())
	}
	// Streaming continues from the new baseline.
	if err := h.fake.Push(l2(41, "500,buy,9")); err != nil {
		t.Fatal(err)
	}
	if ev := expectEvents(t, book, orderbook.EventUpdate)[0]; ev.Sequence != 41 {
		t.Fatalf("update: %+v", ev)
	}
}

func TestOrderBook_ReconnectDiscardsTheBookAndRebuildsIt(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		if calls.Add(1) == 1 {
			return docSnapshot(), nil
		}
		return orderbook.Snapshot{Symbol: "XBTUSDTM", Sequence: 100, Bids: []orderbook.Level{lvl("700", "1")}, Asks: []orderbook.Level{lvl("701", "1")}}, nil
	}
	s := h.public(t)
	book, err := s.SubscribeOrderBook(ctx5(t), "XBTUSDTM")
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)

	h.fake.Server.DropAll()
	ev := expectEvents(t, book, orderbook.EventStale)[0]
	if !errors.Is(ev.Err, orderbook.ErrReconnected) {
		t.Fatalf("stale cause = %v", ev.Err)
	}
	if book.Book().Ready() {
		t.Fatal("a stale book must not be presented as ready")
	}
	eventually(t, func() bool { return s.State() == stream.StateConnected && h.fake.Server.Connections() == 2 }, "reconnected")
	// The stream is back: its first update proves it and restarts the sync.
	if err := h.fake.Push(l2(101, "700,buy,2")); err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventSnapshot)
	if book.Book().Sequence() != 101 || calls.Load() != 2 {
		t.Fatalf("sequence=%d snapshot calls=%d", book.Book().Sequence(), calls.Load())
	}
	if bid, _ := book.Book().BestBid(); bid.Price != "700" || bid.Size != "2" {
		t.Fatalf("best bid = %v", bid)
	}
}

func TestOrderBook_QuietSymbolStillResyncsAfterAReconnect(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		n := calls.Add(1)
		return orderbook.Snapshot{Symbol: "XBTUSDTM", Sequence: int64(n) * 10, Bids: []orderbook.Level{lvl("1", "1")}}, nil
	}
	s := h.public(t)
	book, err := s.SubscribeOrderBookTuned(ctx5(t), "XBTUSDTM", orderbook.Tuning{ResetGrace: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready() // Start fetches immediately, no update needed
	expectEvents(t, book, orderbook.EventSnapshot)
	h.fake.Server.DropAll()
	expectEvents(t, book, orderbook.EventStale, orderbook.EventSnapshot) // the grace timer restarts the sync
	if book.Book().Sequence() != 20 {
		t.Fatalf("sequence = %d", book.Book().Sequence())
	}
}

func TestOrderBook_GivesUpWithATypedErrorWhenSnapshotsKeepFailing(t *testing.T) {
	h := newHarness(t)
	boom := errors.New("snapshot endpoint down")
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) { return orderbook.Snapshot{}, boom }
	book, err := h.public(t).SubscribeOrderBookTuned(ctx5(t), "XBTUSDTM", orderbook.Tuning{MaxAttempts: 2, RetryMin: time.Millisecond, RetryMax: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ev := expectEvents(t, book, orderbook.EventFailed)[0]
	if !errors.Is(ev.Err, stream.ErrResyncFailed) || !errors.Is(ev.Err, boom) {
		t.Fatalf("failure: %v", ev.Err)
	}
	select {
	case <-book.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the subscription did not end")
	}
	if !errors.Is(book.Err(), stream.ErrResyncFailed) {
		t.Fatalf("Err = %v", book.Err())
	}
	if _, ok := <-book.C(); ok {
		t.Fatal("C must be closed after the failure")
	}
	eventually(t, func() bool { return h.fake.Count("unsubscribe", l2Topic) == 1 }, "the failed book unsubscribes from the stream")
}

func TestOrderBook_PermanentSnapshotErrorEndsImmediately(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		calls.Add(1)
		return orderbook.Snapshot{}, stream.Permanent(fmt.Errorf("snapshot: %w", transport.ErrCredentialsRequired))
	}
	book, err := h.public(t).SubscribeOrderBook(ctx5(t), "XBTUSDTM")
	if err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventFailed)
	<-book.Done()
	if !errors.Is(book.Err(), transport.ErrCredentialsRequired) || !errors.Is(book.Err(), stream.ErrResyncFailed) || calls.Load() != 1 {
		t.Fatalf("Err=%v calls=%d", book.Err(), calls.Load())
	}
}

// Too many updates pile up while a snapshot is being fetched: the attempt is
// abandoned, the consumer is told, and the book rebuilds itself from a fresh
// snapshot. The stalled first fetch makes this independent of machine speed.
func TestOrderBook_TooManyBufferedUpdatesRestartTheSyncAndTheBookRecovers(t *testing.T) {
	h := newHarness(t)
	const last = 50
	release := make(chan struct{})
	var calls atomic.Int32
	h.snapshots = func(ctx context.Context, symbol string) (orderbook.Snapshot, error) {
		if calls.Add(1) == 1 {
			select { // the first fetch is slow: updates pile up behind it
			case <-release:
			case <-ctx.Done():
				return orderbook.Snapshot{}, ctx.Err()
			}
		}
		return orderbook.Snapshot{Symbol: symbol, Sequence: last, Bids: []orderbook.Level{lvl("100", "9")}, Asks: []orderbook.Level{lvl("101", "9")}}, nil
	}
	frames := make([]string, 0, last)
	for seq := 1; seq <= last; seq++ {
		frames = append(frames, l2(int64(seq), "100,buy,1"))
	}
	h.fake.OnSubscribe(l2Topic, frames...)
	book, err := h.public(t).SubscribeOrderBookTuned(ctx5(t), "XBTUSDTM", orderbook.Tuning{MaxBuffered: 3, RetryMin: time.Millisecond, RetryMax: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ev := nextEvent(t, book)
	if ev.Type != orderbook.EventStale || !errors.Is(ev.Err, orderbook.ErrBufferOverflow) {
		t.Fatalf("first event = %v (%v), want a stale event caused by the buffer overflow", ev.Type, ev.Err)
	}
	close(release)
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev := <-book.C():
			if ev.Type == orderbook.EventSnapshot && book.Book().Sequence() >= last {
				if book.Resyncs() == 0 || calls.Load() < 2 {
					t.Fatalf("resyncs=%d fetches=%d", book.Resyncs(), calls.Load())
				}
				return
			}
		case <-deadline:
			t.Fatalf("book never recovered: state=%v seq=%d resyncs=%d", book.State(), book.Book().Sequence(), book.Resyncs())
		}
	}
}

// The connection layer reports frames it dropped (a gap marker) and a restored
// connection (a reset marker) to the book's handler in stream order; either one
// means the book may have a hole in it and must be rebuilt.
func TestBookHandler_GapAndResetMarkersForceAResync(t *testing.T) {
	wstest.CheckLeaks(t)
	var calls atomic.Int32
	cfg := orderbook.SyncerConfig{
		Symbol: "XBTUSDTM", RetryMin: time.Millisecond,
		Snapshot: func(context.Context) (orderbook.Snapshot, error) {
			n := int64(calls.Add(1))
			return orderbook.Snapshot{Symbol: "XBTUSDTM", Sequence: 10 * n, Bids: []orderbook.Level{lvl("100", "9")}, Asks: []orderbook.Level{lvl("101", "9")}}, nil
		},
	}
	sub := stream.NewSubscription[orderbook.Event]("t", 0, nil)
	cfg.DeliverTo(sub, nil)
	syncer := orderbook.NewSyncer(cfg)
	t.Cleanup(func() { syncer.Close(); sub.Seal() })
	go func() { // the events are not the subject here; keep them flowing
		for range sub.C() {
		}
	}()
	h := &bookHandler{sub: sub, syncer: syncer, report: func(error) {}}
	syncer.Start()
	<-syncer.Ready()

	h.OnGap(7)
	eventually(t, func() bool {
		return syncer.Resyncs() == 1 && syncer.State() == orderbook.SyncSynced && calls.Load() == 2
	}, "the resync after a gap marker")
	if got := sub.Dropped(); got != 7 {
		t.Fatalf("dropped = %d, want the 7 frames the gap marker reported", got)
	}
	h.OnReset(2)
	// After a reset the stream must prove it is alive again: the first update starts the resync.
	eventually(t, func() bool { return syncer.State() == orderbook.SyncIdle }, "the book to be cleared by the reset marker")
	h.OnFrame(stream.Frame{Msg: &classic.Message{Topic: l2Topic, Subject: "level2", Data: []byte(`{"sequence":25,"change":"100,buy,3","timestamp":1}`)}})
	eventually(t, func() bool {
		return syncer.Resyncs() == 2 && syncer.State() == orderbook.SyncSynced && calls.Load() == 3
	}, "the resync after a reset marker")
}

// A consumer that does not read the events at all must not freeze the book: the
// book follows the stream whether or not anybody listens, and the events that pile
// up are collapsed into reload markers instead of stalling the connection.
func TestOrderBook_ConsumerThatNeverReadsDoesNotFreezeTheBook(t *testing.T) {
	h := newHarness(t)
	const last = 1500
	var calls atomic.Int32
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		seq := int64(last)
		if calls.Add(1) == 1 {
			seq = 0
		}
		return orderbook.Snapshot{Symbol: "XBTUSDTM", Sequence: seq, Bids: []orderbook.Level{lvl("100", "9")}, Asks: []orderbook.Level{lvl("101", "9")}}, nil
	}
	frames := make([]string, 0, last)
	for seq := 1; seq <= last; seq++ {
		frames = append(frames, l2(int64(seq), fmt.Sprintf("100,buy,%d", seq)))
	}
	h.fake.OnSubscribe(l2Topic, frames...)
	book, err := h.public(t).SubscribeOrderBook(ctx5(t), "XBTUSDTM", stream.WithBuffer(32))
	if err != nil {
		t.Fatal(err)
	}
	// Not a single event is read. Whether the burst was absorbed or forced a resync,
	// the book must reach the last sequence by itself.
	eventually(t, func() bool { return book.State() == orderbook.SyncSynced && book.Book().Sequence() == last }, "the book to reach sequence "+fmt.Sprint(last)+" without anybody reading its events")

	// A consumer that follows the documented protocol ends up with the live book:
	// reload from the book on an EventSnapshot (when the book is ready), treat an
	// EventStale as "my copy is invalid", and skip the updates a reload already
	// contains. The book may still be resynchronising while it catches up, so it
	// keeps following until everything is quiet.
	mirror := orderbook.New("XBTUSDTM")
	valid := false
	follow := func(ev orderbook.Event) {
		switch ev.Type {
		case orderbook.EventSnapshot:
			snap, ok := ev.Book.SnapshotIfReady(0)
			if valid = ok; ok {
				if err := mirror.Reset(snap); err != nil {
					t.Fatal(err)
				}
			}
		case orderbook.EventStale:
			valid = false
		case orderbook.EventUpdate:
			if !valid || ev.Sequence <= mirror.Sequence() {
				return
			}
			if _, err := mirror.Apply(orderbook.Delta{Start: mirror.Sequence() + 1, End: ev.Sequence, Changes: ev.Changes}); err != nil {
				t.Fatalf("applying update %d: %v", ev.Sequence, err)
			}
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
	drain:
		for {
			select {
			case ev := <-book.C():
				follow(ev)
			default:
				break drain
			}
		}
		if bids := book.Book().Snapshot(0).Bids; book.State() == orderbook.SyncSynced && valid && mirror.Sequence() == book.Book().Sequence() &&
			levels(mirror.Snapshot(0).Bids) == levels(bids) && len(bids) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the mirror (valid=%v, sequence %d) never caught up with the book (state %v, sequence %d)", valid, mirror.Sequence(), book.State(), book.Book().Sequence())
		}
		time.Sleep(time.Millisecond)
	}
}

// With stream.FailSubscription a consumer that falls behind loses the
// subscription instead of being resynchronised silently.
func TestOrderBook_FailSubscriptionPolicyEndsASlowConsumersBook(t *testing.T) {
	h := newHarness(t)
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		return orderbook.Snapshot{Symbol: "XBTUSDTM", Sequence: 0, Bids: []orderbook.Level{lvl("100", "9")}, Asks: []orderbook.Level{lvl("101", "9")}}, nil
	}
	frames := make([]string, 0, 300)
	for seq := 1; seq <= 300; seq++ {
		frames = append(frames, l2(int64(seq), "100,buy,1"))
	}
	h.fake.OnSubscribe(l2Topic, frames...)
	book, err := h.public(t).SubscribeOrderBook(ctx5(t), "XBTUSDTM", stream.WithBuffer(8), stream.WithOverflow(stream.FailSubscription))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-book.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("the subscription must end when its consumer cannot keep up: state=%v", book.State())
	}
	if !errors.Is(book.Err(), stream.ErrSlowConsumer) {
		t.Fatalf("Err = %v, want stream.ErrSlowConsumer", book.Err())
	}
}

func TestOrderBook_UndecodableUpdateIsTreatedAsALostUpdate(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		n := calls.Add(1)
		return orderbook.Snapshot{Symbol: "XBTUSDTM", Sequence: int64(n) * 100, Bids: []orderbook.Level{lvl("1", "1")}}, nil
	}
	s := h.public(t)
	book, err := s.SubscribeOrderBook(ctx5(t), "XBTUSDTM")
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	if err := h.fake.Push(wstest.Message(l2Topic, "level2", `{"sequence":101,"change":"garbage","timestamp":1}`)); err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventStale, orderbook.EventSnapshot)
	if s.Stats().DecodeErrors != 1 || calls.Load() != 2 {
		t.Fatalf("decode errors=%d snapshot calls=%d", s.Stats().DecodeErrors, calls.Load())
	}
}

func TestOrderBook_CloseEndsTheStreamAndReleasesTheSyncer(t *testing.T) {
	h := newHarness(t)
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) { return docSnapshot(), nil }
	book, err := h.public(t).SubscribeOrderBook(ctx5(t), "XBTUSDTM")
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	if err := book.Close(); err != nil {
		t.Fatal(err)
	}
	for range book.C() { // drain until closed
	}
	if book.Err() != nil {
		t.Fatalf("a requested close is not an error: %v", book.Err())
	}
	if h.fake.Count("unsubscribe", l2Topic) != 1 {
		t.Fatal("the server never saw the unsubscribe")
	}
	if book.State() != orderbook.SyncClosed {
		t.Fatalf("syncer state = %v", book.State())
	}
}

func TestOrderBook_RejectedSubscriptionLeavesNothingBehind(t *testing.T) {
	h := newHarness(t)
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) { return docSnapshot(), nil }
	h.fake.Reject(l2Topic, 404)
	_, err := h.public(t).SubscribeOrderBook(ctx5(t), "XBTUSDTM")
	if !errors.Is(err, stream.ErrTopicNotFound) {
		t.Fatalf("error = %v", err)
	}
}

func TestOrderBook_ConcurrentReadersWhileStreaming(t *testing.T) {
	h := newHarness(t)
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) { return docSnapshot(), nil }
	s := h.public(t)
	book, err := s.SubscribeOrderBook(ctx5(t), "XBTUSDTM", stream.WithBuffer(10000))
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)

	var stop atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				snap := book.Book().Snapshot(5)
				for i := 1; i < len(snap.Bids); i++ {
					if c, _ := snap.Bids[i-1].Price.Cmp(snap.Bids[i].Price); c <= 0 {
						t.Errorf("bids out of order: %v", levels(snap.Bids))
						return
					}
				}
				_, _ = book.Book().Spread()
				_ = book.State()
				runtime.Gosched() // with one CPU a spinning reader would starve the feeder
			}
		}()
	}
	for seq := int64(17); seq < 600; seq++ {
		if err := h.fake.Push(l2(seq, fmt.Sprintf("3988.%02d,buy,%d", 10+seq%30, seq))); err != nil {
			t.Fatal(err)
		}
	}
	got := 0
	for got < 583 {
		ev := nextEvent(t, book)
		if ev.Type == orderbook.EventUpdate {
			got++
		}
	}
	stop.Store(true)
	wg.Wait()
	if book.Book().Sequence() != 599 {
		t.Fatalf("sequence = %d", book.Book().Sequence())
	}
}
