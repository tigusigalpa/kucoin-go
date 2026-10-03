package streaming

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
	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
	"github.com/tigusigalpa/kucoin-go/types"
	"github.com/tigusigalpa/kucoin-go/websocket/classic"
)

const l2Topic = "/market/level2:BTC-USDT"

// l2 builds a level-2 push that covers the sequence numbers start..end; asks and
// bids are JSON arrays of [price, size, sequence] rows ("" for none).
func l2(start, end int64, asks, bids string) string {
	if asks == "" {
		asks = "[]"
	}
	if bids == "" {
		bids = "[]"
	}
	return wstest.Message(l2Topic, "trade.l2update",
		fmt.Sprintf(`{"changes":{"asks":%s,"bids":%s},"sequenceEnd":%d,"sequenceStart":%d,"symbol":"BTC-USDT","time":1729816425625}`, asks, bids, end, start))
}

func lvl(price, size string) orderbook.Level {
	return orderbook.Level{Price: types.Decimal(price), Size: types.Decimal(size)}
}

// docSnapshot is the REST snapshot of KuCoin's worked order-book example
// (sequence 16), in the spelling of the Spot REST API.
func docSnapshot() orderbook.Snapshot {
	return orderbook.Snapshot{
		Symbol:   "BTC-USDT",
		Sequence: 16,
		Asks:     []orderbook.Level{lvl("3988.62", "8"), lvl("3988.61", "32"), lvl("3988.60", "47"), lvl("3988.59", "3")},
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

// catchUp reads events until one shows the book at sequence n or beyond. Only
// snapshot and update events may occur while it does.
func catchUp(t *testing.T, b *BookStream, n int64) orderbook.Event {
	t.Helper()
	for {
		ev := nextEvent(t, b)
		if ev.Type != orderbook.EventSnapshot && ev.Type != orderbook.EventUpdate {
			t.Fatalf("unexpected %v event (%v) while catching up to sequence %d", ev.Type, ev.Err, n)
		}
		if ev.Sequence >= n {
			return ev
		}
	}
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
		if symbol != "BTC-USDT" {
			t.Errorf("snapshot requested for %q", symbol)
		}
		select {
		case <-release:
			return docSnapshot(), nil
		case <-ctx.Done():
			return orderbook.Snapshot{}, ctx.Err()
		}
	}
	// 14..16 is entirely contained in the snapshot (sequence 16) and must be
	// discarded: applying it would set 3988.59 to 999. 15..19 is the
	// documentation's own example; it starts before and ends after the snapshot,
	// an overlapping range that must be applied.
	h.fake.OnSubscribe(l2Topic,
		l2(14, 16, `[["3988.59","999","16"]]`, ""),
		l2(15, 19, `[["3988.59","3","16"],["3988.61","0","19"],["3988.62","8","15"]]`, `[["3988.50","44","18"]]`))
	book, err := h.public(t).SubscribeOrderBook(ctx5(t), "BTC-USDT")
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
	// The two buffered updates were either replayed on the snapshot or arrived right
	// after it; either way the stale one must be skipped and the overlapping one
	// applied.
	first := nextEvent(t, book)
	if first.Type != orderbook.EventSnapshot {
		t.Fatalf("first event = %v", first.Type)
	}
	if first.Sequence < 19 {
		catchUp(t, book, 19)
	}
	snap := book.Book().Snapshot(0)
	if snap.Sequence != 19 {
		t.Fatalf("sequence = %d, want 19", snap.Sequence)
	}
	// The documentation's final order book.
	if got := levels(snap.Bids); got != "3988.51:56 3988.5:44 3988.49:100 3988.48:10 " {
		t.Fatalf("bids = %s", got)
	}
	if got := levels(snap.Asks); got != "3988.59:3 3988.6:47 3988.62:8 " {
		t.Fatalf("asks = %s", got)
	}

	// Live update continuing the book.
	if err := h.fake.Push(l2(20, 20, `[["3988.60","40","20"]]`, "")); err != nil {
		t.Fatal(err)
	}
	ev := expectEvents(t, book, orderbook.EventUpdate)[0]
	if ev.Sequence != 20 || len(ev.Changes) != 1 || ev.Changes[0].Side != orderbook.Ask || ev.Changes[0].Price != "3988.60" || ev.Changes[0].Size != "40" || ev.Symbol != "BTC-USDT" || ev.Book != book.Book() {
		t.Fatalf("update event: %+v", ev)
	}
	// A live range that starts at an already applied sequence number but ends
	// beyond it is applied, asks and bids alike.
	if err := h.fake.Push(l2(20, 22, `[["3988.60","41","22"]]`, `[["3988.52","7","21"]]`)); err != nil {
		t.Fatal(err)
	}
	ev = expectEvents(t, book, orderbook.EventUpdate)[0]
	if ev.Sequence != 22 || len(ev.Changes) != 2 || ev.Changes[0].Side != orderbook.Ask || ev.Changes[1].Side != orderbook.Bid || ev.Changes[1].Price != "3988.52" {
		t.Fatalf("range update event: %+v", ev)
	}
	// A range the book already contains is ignored without an event, and a row with
	// a zero price is skipped while the sequence still advances.
	if err := h.fake.Push(l2(21, 22, `[["3988.60","999","22"]]`, "")); err != nil {
		t.Fatal(err)
	}
	if err := h.fake.Push(l2(23, 23, "", `[["0","5","23"]]`)); err != nil {
		t.Fatal(err)
	}
	ev = expectEvents(t, book, orderbook.EventUpdate)[0]
	if ev.Sequence != 23 || len(ev.Changes) != 0 {
		t.Fatalf("zero-price update event: sequence=%d changes=%+v", ev.Sequence, ev.Changes)
	}
	if bid, _ := book.Book().BestBid(); bid.Price != "3988.52" || bid.Size != "7" {
		t.Fatalf("best bid = %v", bid)
	}
	if ask, _ := book.Book().BestAsk(); ask.Price != "3988.59" || ask.Size != "3" {
		t.Fatalf("best ask = %v", ask)
	}
	if bids, asks := book.Book().Len(); bids != 5 || asks != 3 {
		t.Fatalf("level counts = %d bids, %d asks", bids, asks)
	}
	if got := levels(book.Book().Snapshot(0).Asks); got != "3988.59:3 3988.6:41 3988.62:8 " {
		t.Fatalf("asks = %s (the ignored range must not have been applied)", got)
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
		return orderbook.Snapshot{Symbol: "BTC-USDT", Sequence: 40, Bids: []orderbook.Level{lvl("500", "5")}, Asks: []orderbook.Level{lvl("501", "5")}}, nil
	}
	book, err := h.public(t).SubscribeOrderBook(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	if err := h.fake.Push(l2(17, 18, `[["3988.61","0","18"]]`, `[["3988.50","44","17"]]`)); err != nil {
		t.Fatal(err)
	}
	if err := h.fake.Push(l2(19, 19, `[["3988.62","7","19"]]`, "")); err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventUpdate, orderbook.EventUpdate)
	if err := h.fake.Push(l2(25, 26, `[["3988.62","1","26"]]`, "")); err != nil { // 20..24 were lost
		t.Fatal(err)
	}
	evs := expectEvents(t, book, orderbook.EventStale, orderbook.EventSnapshot)
	var gap *orderbook.GapError
	if !errors.Is(evs[0].Err, orderbook.ErrSequenceGap) || !errors.As(evs[0].Err, &gap) || gap.Have != 19 || gap.Start != 25 || gap.End != 26 {
		t.Fatalf("stale cause: %v", evs[0].Err)
	}
	if bid, _ := book.Book().BestBid(); bid.Price != "500" || book.Book().Sequence() != 40 {
		t.Fatalf("the book must come from the second snapshot: %v seq=%d", bid, book.Book().Sequence())
	}
	if book.Resyncs() != 1 || calls.Load() != 2 {
		t.Fatalf("resyncs=%d snapshot calls=%d", book.Resyncs(), calls.Load())
	}
	// Streaming continues from the new baseline.
	if err := h.fake.Push(l2(41, 41, "", `[["500","9","41"]]`)); err != nil {
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
		return orderbook.Snapshot{Symbol: "BTC-USDT", Sequence: 100, Bids: []orderbook.Level{lvl("700", "1")}, Asks: []orderbook.Level{lvl("701", "1")}}, nil
	}
	s := h.public(t)
	book, err := s.SubscribeOrderBook(ctx5(t), "BTC-USDT")
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
	if err := h.fake.Push(l2(101, 101, "", `[["700","2","101"]]`)); err != nil {
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
		return orderbook.Snapshot{Symbol: "BTC-USDT", Sequence: int64(n) * 10, Bids: []orderbook.Level{lvl("1", "1")}}, nil
	}
	s := h.public(t)
	book, err := s.SubscribeOrderBookTuned(ctx5(t), "BTC-USDT", orderbook.Tuning{ResetGrace: 30 * time.Millisecond})
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
	var calls atomic.Int32
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		calls.Add(1)
		return orderbook.Snapshot{}, boom
	}
	book, err := h.public(t).SubscribeOrderBookTuned(ctx5(t), "BTC-USDT", orderbook.Tuning{MaxAttempts: 3, RetryMin: time.Millisecond, RetryMax: 2 * time.Millisecond})
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
	if !errors.Is(book.Err(), stream.ErrResyncFailed) || calls.Load() != 3 {
		t.Fatalf("Err = %v after %d snapshot calls, want ErrResyncFailed after 3", book.Err(), calls.Load())
	}
	if _, ok := <-book.C(); ok {
		t.Fatal("C must be closed after the failure")
	}
	eventually(t, func() bool { return h.fake.Count("unsubscribe", l2Topic) == 1 }, "the failed book unsubscribes from the stream")
}

func TestOrderBook_TransientSnapshotErrorsAreRetried(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		switch calls.Add(1) {
		case 1:
			return orderbook.Snapshot{}, fmt.Errorf("get full order book: %w", transport.ErrServiceUnavailable)
		case 2:
			return orderbook.Snapshot{}, fmt.Errorf("get full order book: %w", transport.ErrRateLimited)
		}
		return docSnapshot(), nil
	}
	book, err := h.public(t).SubscribeOrderBookTuned(ctx5(t), "BTC-USDT", orderbook.Tuning{RetryMin: time.Millisecond, RetryMax: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	if calls.Load() != 3 || book.State() != orderbook.SyncSynced || book.Book().Sequence() != 16 || book.Err() != nil {
		t.Fatalf("snapshot calls=%d state=%v sequence=%d err=%v", calls.Load(), book.State(), book.Book().Sequence(), book.Err())
	}
}

func TestOrderBook_MissingCredentialsEndTheBookImmediately(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		calls.Add(1)
		// What the REST client returns for the signed full-order-book call when the
		// client has no credentials; it is not marked permanent by the caller.
		return orderbook.Snapshot{}, fmt.Errorf("get full order book: %w", transport.ErrCredentialsRequired)
	}
	s := h.public(t)
	book, err := s.SubscribeOrderBook(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	ev := expectEvents(t, book, orderbook.EventFailed)[0]
	if !errors.Is(ev.Err, transport.ErrCredentialsRequired) || !errors.Is(ev.Err, stream.ErrResyncFailed) {
		t.Fatalf("failure: %v", ev.Err)
	}
	select {
	case <-book.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the subscription did not end")
	}
	if !errors.Is(book.Err(), transport.ErrCredentialsRequired) || !errors.Is(book.Err(), stream.ErrResyncFailed) || calls.Load() != 1 {
		t.Fatalf("Err=%v calls=%d, want one call and an error matching both sentinels", book.Err(), calls.Load())
	}
	if !strings.Contains(book.Err().Error(), "needs API credentials") {
		t.Fatalf("the error must say what to do: %v", book.Err())
	}
	if _, ok := <-book.C(); ok {
		t.Fatal("C must be closed after the failure")
	}
	eventually(t, func() bool { return h.fake.Count("unsubscribe", l2Topic) == 1 }, "the failed book unsubscribes from the stream")
	if s.State() != stream.StateConnected {
		t.Fatal("the session itself must stay up")
	}
	// The session still serves its other subscriptions.
	h.fake.OnSubscribe("/market/match:BTC-USDT", spTrade)
	trades, err := s.SubscribeTrades(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	if v := next(t, trades); v.Symbol != "BTC-USDT" {
		t.Fatalf("trade: %+v", v)
	}
}

func TestOrderBook_RejectedCredentialsEndTheBookImmediately(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		calls.Add(1)
		return orderbook.Snapshot{}, fmt.Errorf("%w: %w", transport.ErrUnauthorized, &transport.KucoinError{HTTPStatus: 401, Code: "400005", Message: "Invalid KC-API-SIGN"})
	}
	book, err := h.public(t).SubscribeOrderBook(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventFailed)
	<-book.Done()
	var ke *transport.KucoinError
	if !errors.Is(book.Err(), transport.ErrUnauthorized) || !errors.Is(book.Err(), stream.ErrResyncFailed) || !errors.As(book.Err(), &ke) || ke.Code != "400005" || calls.Load() != 1 {
		t.Fatalf("Err=%v calls=%d", book.Err(), calls.Load())
	}
	if !strings.Contains(book.Err().Error(), "rejected") {
		t.Fatalf("the error must say the credentials were rejected: %v", book.Err())
	}
}

func TestOrderBook_ASnapshotOlderThanTheBufferIsFetchedAgain(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	s := h.public(t)
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		if calls.Add(1) == 1 {
			// A lagging snapshot: it ends before the first buffered update starts.
			// Give the update time to reach the buffer first (this runs on the
			// syncer's goroutine, so it must not use t.Fatal).
			deadline := time.Now().Add(5 * time.Second)
			for s.Stats().FramesReceived < 3 && time.Now().Before(deadline) {
				time.Sleep(2 * time.Millisecond)
			}
			time.Sleep(25 * time.Millisecond)
			return orderbook.Snapshot{Symbol: "BTC-USDT", Sequence: 5, Bids: []orderbook.Level{lvl("1", "1")}}, nil
		}
		return orderbook.Snapshot{Symbol: "BTC-USDT", Sequence: 12, Bids: []orderbook.Level{lvl("2", "2")}}, nil
	}
	h.fake.OnSubscribe(l2Topic, l2(10, 10, "", `[["2","3","10"]]`))
	book, err := s.SubscribeOrderBookTuned(ctx5(t), "BTC-USDT", orderbook.Tuning{RetryMin: time.Millisecond, RetryMax: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	// Normally the first snapshot cannot be aligned with the buffered update 10 and
	// is silently replaced; if the update arrived late instead, the gap forces a
	// resync. Both end with the second snapshot.
	for {
		ev := nextEvent(t, book)
		if ev.Type == orderbook.EventFailed {
			t.Fatalf("book failed: %v", ev.Err)
		}
		if ev.Type == orderbook.EventSnapshot && ev.Sequence == 12 {
			break
		}
	}
	if calls.Load() != 2 || book.Book().Sequence() != 12 || book.State() != orderbook.SyncSynced {
		t.Fatalf("snapshot calls=%d sequence=%d state=%v", calls.Load(), book.Book().Sequence(), book.State())
	}
	if bid, _ := book.Book().BestBid(); bid.Price != "2" || bid.Size != "2" {
		t.Fatalf("best bid = %v (update 10 is older than the snapshot and must not be applied)", bid)
	}
	if err := h.fake.Push(l2(13, 13, "", `[["2","4","13"]]`)); err != nil {
		t.Fatal(err)
	}
	if ev := expectEvents(t, book, orderbook.EventUpdate)[0]; ev.Sequence != 13 {
		t.Fatalf("update: %+v", ev)
	}
}

// pushFlood sends the frames one after the other, a hair apart. A tight loop of
// writes would pile them up in the socket buffers and keep the acknowledgement of a
// later request waiting behind them on a small machine.
func pushFlood(t *testing.T, h *harness, frames []string) {
	t.Helper()
	for _, frame := range frames {
		if err := h.fake.Push(frame); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

// pushPaced sends the single-row updates first..last in groups of at most group
// frames and waits for the book to take each group in before it sends the next. The
// connection's queue in front of the book (which stream.WithBuffer sizes as well)
// therefore cannot overflow, whatever the speed of the machine: only the consumer
// of the events is slow.
func pushPaced(t *testing.T, h *harness, book *BookStream, first, last, group int64) {
	t.Helper()
	for seq := first; seq <= last; {
		end := min(seq+group-1, last)
		for n := seq; n <= end; n++ {
			if err := h.fake.Push(l2(n, n, "", fmt.Sprintf(`[["3988.50","%d","%d"]]`, n, n))); err != nil {
				t.Fatal(err)
			}
		}
		eventually(t, func() bool { return book.Book().Sequence() == end }, fmt.Sprintf("the book to take in sequence %d", end))
		seq = end + 1
	}
}

func TestOrderBook_ASlowConsumerLosesEventsButNotTheBook(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		calls.Add(1)
		return docSnapshot(), nil
	}
	s := h.public(t)
	book, err := s.SubscribeOrderBook(ctx5(t), "BTC-USDT", stream.WithBuffer(8))
	if err != nil {
		t.Fatal(err)
	}
	// Nobody reads the events. The book must keep following the stream anyway: only
	// the queue of undelivered events is bounded.
	<-book.Ready()
	const last = 316
	pushPaced(t, h, book, 17, last, 4)
	if book.Dropped() == 0 {
		t.Fatalf("dropped = 0; the events that did not fit the queue must be counted (stats %+v)", s.Stats())
	}
	if book.State() != orderbook.SyncSynced || book.Resyncs() != 0 || calls.Load() != 1 {
		t.Fatalf("a slow consumer must not cost the book its synchronisation: state=%v resyncs=%d snapshot calls=%d", book.State(), book.Resyncs(), calls.Load())
	}
	// The consumer returns: it gets the initial snapshot, then updates and reload
	// markers (snapshot events that stand in for dropped updates) up to the present.
	snapshots := 0
	for {
		ev := nextEvent(t, book)
		switch ev.Type {
		case orderbook.EventSnapshot:
			snapshots++
		case orderbook.EventUpdate:
		default:
			t.Fatalf("unexpected %v event: %v", ev.Type, ev.Err)
		}
		if ev.Sequence >= last {
			break
		}
	}
	if snapshots < 2 {
		t.Fatalf("%d snapshot events; a reload marker must replace the dropped updates", snapshots)
	}
	if bid, _ := book.Book().BestBid(); bid.Price != "3988.51" || book.Err() != nil || book.Resyncs() != 0 {
		t.Fatalf("best bid=%v err=%v resyncs=%d", bid, book.Err(), book.Resyncs())
	}
}

// A consumer that does not read the events at all must not freeze the book: the
// book follows the stream whether or not anybody listens, and the events that pile
// up are collapsed into reload markers instead of stalling the connection. Whether
// the small queues absorb the burst or lose a push and force a resync, the book
// reaches the last sequence by itself, and a consumer that follows the documented
// protocol ends up with the live book.
func TestOrderBook_ConsumerThatNeverReadsDoesNotFreezeTheBook(t *testing.T) {
	h := newHarness(t)
	const last = 400
	var calls atomic.Int32
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		seq := int64(last)
		if calls.Add(1) == 1 {
			seq = 0
		}
		return orderbook.Snapshot{Symbol: "BTC-USDT", Sequence: seq, Bids: []orderbook.Level{lvl("100", "9")}, Asks: []orderbook.Level{lvl("101", "9")}}, nil
	}
	book, err := h.public(t).SubscribeOrderBook(ctx5(t), "BTC-USDT", stream.WithBuffer(32))
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	frames := make([]string, 0, last)
	for seq := 1; seq <= last; seq++ {
		frames = append(frames, l2(int64(seq), int64(seq), "", fmt.Sprintf(`[["100","%d","%d"]]`, seq, seq)))
	}
	pushFlood(t, h, frames)
	// Not a single event has been read.
	eventually(t, func() bool { return book.State() == orderbook.SyncSynced && book.Book().Sequence() == last }, "the book to reach sequence "+fmt.Sprint(last)+" without anybody reading its events")

	// The consumer follows the protocol: reload from the book on an EventSnapshot
	// (when the book is ready), treat an EventStale as "my copy is invalid", and skip
	// the updates a reload already contains. The book may still be resynchronising
	// while the consumer catches up, so it keeps following until everything is quiet.
	mirror := orderbook.New("BTC-USDT")
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
// subscription instead of being served reload markers.
func TestOrderBook_FailSubscriptionPolicyEndsTheBookOfASlowConsumer(t *testing.T) {
	h := newHarness(t)
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) { return docSnapshot(), nil }
	s := h.public(t)
	book, err := s.SubscribeOrderBook(ctx5(t), "BTC-USDT", stream.WithBuffer(4), stream.WithOverflow(stream.FailSubscription))
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	// Four updates fill the queue of a consumer that reads nothing (the event it is
	// stuck on does not count); the fifth does not fit. They go in two at a time so
	// that the connection's queue, sized by the same option, cannot overflow first.
	pushPaced(t, h, book, 17, 20, 2)
	if err := h.fake.Push(l2(21, 21, "", `[["3988.50","21","21"]]`)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return book.State() == orderbook.SyncFailed }, "the book to fail because its consumer cannot keep up")
	// The consumer sees what fitted into its queue, in order, and then the failure.
	expectEvents(t, book, orderbook.EventSnapshot)
	var last orderbook.Event
	updates := 0
	for {
		last = nextEvent(t, book)
		if last.Type == orderbook.EventFailed {
			break
		}
		if last.Type != orderbook.EventUpdate || last.Sequence != int64(16+updates+1) {
			t.Fatalf("event %d = %v at sequence %d, want update %d", updates, last.Type, last.Sequence, 16+updates+1)
		}
		updates++
	}
	if updates == 0 || updates > 4 {
		t.Fatalf("%d updates were delivered before the failure, want 1..4", updates)
	}
	if !errors.Is(last.Err, stream.ErrSlowConsumer) {
		t.Fatalf("failure: %v", last.Err)
	}
	select {
	case <-book.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the subscription did not end")
	}
	if !errors.Is(book.Err(), stream.ErrSlowConsumer) {
		t.Fatalf("Err = %v", book.Err())
	}
	if _, ok := <-book.C(); ok {
		t.Fatal("C must be closed after the failure")
	}
	eventually(t, func() bool { return h.fake.Count("unsubscribe", l2Topic) == 1 }, "the failed book unsubscribes from the stream")
	if s.State() != stream.StateConnected {
		t.Fatal("the session itself must stay up")
	}
}

// An absent consumer does not keep a failed book alive either: the subscription
// ends with the cause after a short grace period although nobody ever reads the
// final event.
func TestOrderBook_AFailedBookEndsEvenWhenNobodyReadsItsEvents(t *testing.T) {
	h := newHarness(t)
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		return orderbook.Snapshot{}, fmt.Errorf("get full order book: %w", transport.ErrCredentialsRequired)
	}
	book, err := h.public(t).SubscribeOrderBook(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-book.Done(): // not a single event is read
	case <-time.After(5 * time.Second):
		t.Fatal("the subscription did not end although its consumer is absent")
	}
	if !errors.Is(book.Err(), transport.ErrCredentialsRequired) || !errors.Is(book.Err(), stream.ErrResyncFailed) {
		t.Fatalf("Err = %v", book.Err())
	}
	eventually(t, func() bool { return h.fake.Count("unsubscribe", l2Topic) == 1 }, "the failed book unsubscribes from the stream")
}

// gatedHandler holds back the first frame it is given until it is released, which
// stalls the delivery goroutine of its subscription the way a stopped process or a
// starved machine would.
type gatedHandler struct {
	stream.Handler
	gate chan struct{}
	// entered is closed when the first frame has arrived and is being held back.
	entered  chan struct{}
	first    sync.Once
	released sync.Once
}

func (g *gatedHandler) OnFrame(f stream.Frame) {
	g.first.Do(func() {
		close(g.entered)
		<-g.gate
	})
	g.Handler.OnFrame(f)
}

func (g *gatedHandler) release() { g.released.Do(func() { close(g.gate) }) }

// OnAbort must unblock a pending OnFrame.
func (g *gatedHandler) OnAbort(err error) {
	g.release()
	g.Handler.OnAbort(err)
}

func int64Frames(n int) uint64 { return uint64(n + 2) } // welcome + ack + n pushes

// The goroutine that feeds the book cannot keep up with a burst (a stalled one and
// a tiny queue stand in for an overloaded machine): the connection drops pushes,
// the book can no longer follow the sequence and rebuilds itself from a fresh
// snapshot. The book is wired by hand, with the production handler, so that the
// stall is deterministic instead of a race against the speed of the machine.
func TestOrderBook_PumpOverflowForcesAResyncAndTheBookRecovers(t *testing.T) {
	h := newHarness(t)
	s := h.public(t)
	var calls atomic.Int32
	const last = 400
	sub := stream.NewSubscription[orderbook.Event](l2Topic, 0, nil)
	cfg := orderbook.SyncerConfig{
		Symbol: "BTC-USDT",
		Snapshot: func(context.Context) (orderbook.Snapshot, error) {
			seq := int64(last) // the "true" state after all the updates that will be streamed
			if calls.Add(1) == 1 {
				seq = 0 // the very first snapshot predates the stream
			}
			return orderbook.Snapshot{Symbol: "BTC-USDT", Sequence: seq, Bids: []orderbook.Level{lvl("100", "9")}, Asks: []orderbook.Level{lvl("101", "9")}}, nil
		},
	}
	cfg.DeliverTo(sub, nil)
	syncer := orderbook.NewSyncer(cfg)
	gated := &gatedHandler{Handler: &bookHandler{sub: sub, syncer: syncer, report: s.Client().ReportDecodeError}, gate: make(chan struct{}), entered: make(chan struct{})}
	handle, err := s.Client().SubscribeHandler(ctx5(t), l2Topic, false, gated, stream.WithBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	sub.BindDropped(func() uint64 { return handle.Dropped() + syncer.DroppedEvents() })
	syncer.Start()
	<-syncer.Ready()

	// The first update stalls the feeding goroutine; of the next 399 the 4-frame
	// queue keeps the newest four.
	frame := func(seq int) string {
		return l2(int64(seq), int64(seq), "", fmt.Sprintf(`[["100","%d","%d"]]`, seq, seq))
	}
	pushFlood(t, h, []string{frame(1)})
	select {
	case <-gated.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first update never reached the feeding goroutine")
	}
	rest := make([]string, 0, last-1)
	for seq := 2; seq <= last; seq++ {
		rest = append(rest, frame(seq))
	}
	pushFlood(t, h, rest)
	eventually(t, func() bool { return s.Stats().FramesReceived >= int64Frames(last) }, "every frame read from the socket")
	eventually(t, func() bool { return sub.Dropped() >= last-1-4 }, fmt.Sprintf("%d pushes to be lost to the 4-frame queue", last-1-4))
	gated.release()

	// The stalled update applies, the gap is noticed, the book is rebuilt from the
	// second snapshot and the four surviving pushes are older than it.
	snapshot := next(t, sub)
	if snapshot.Type != orderbook.EventSnapshot || snapshot.Sequence != 0 {
		t.Fatalf("first event: %+v", snapshot)
	}
	if ev := next(t, sub); ev.Type != orderbook.EventUpdate || ev.Sequence != 1 {
		t.Fatalf("the stalled update: %+v", ev)
	}
	stale := next(t, sub)
	if stale.Type != orderbook.EventStale || !errors.Is(stale.Err, orderbook.ErrUpdatesDropped) {
		t.Fatalf("after the gap: %+v (%v)", stale, stale.Err)
	}
	if ev := next(t, sub); ev.Type != orderbook.EventSnapshot || ev.Sequence != last {
		t.Fatalf("resynchronised: %+v", ev)
	}
	if syncer.Resyncs() != 1 || calls.Load() != 2 || syncer.State() != orderbook.SyncSynced || syncer.Book().Sequence() != last {
		t.Fatalf("resyncs=%d snapshot calls=%d state=%v sequence=%d", syncer.Resyncs(), calls.Load(), syncer.State(), syncer.Book().Sequence())
	}
	if bid, _ := syncer.Book().BestBid(); bid.Price != "100" || bid.Size != "9" {
		t.Fatalf("best bid = %v; the book must come from the second snapshot", bid)
	}
}

func TestOrderBook_UpdateBufferOverflowWhileSynchronisingRestartsTheAttempt(t *testing.T) {
	h := newHarness(t)
	release := make(chan struct{})
	var calls atomic.Int32
	h.snapshots = func(ctx context.Context, _ string) (orderbook.Snapshot, error) {
		calls.Add(1)
		select {
		case <-release:
			// By now the stream has moved on to sequence 26.
			return orderbook.Snapshot{Symbol: "BTC-USDT", Sequence: 26, Bids: []orderbook.Level{lvl("5", "5")}, Asks: []orderbook.Level{lvl("6", "6")}}, nil
		case <-ctx.Done():
			return orderbook.Snapshot{}, ctx.Err()
		}
	}
	s := h.public(t)
	book, err := s.SubscribeOrderBookTuned(ctx5(t), "BTC-USDT", orderbook.Tuning{MaxBuffered: 3})
	if err != nil {
		t.Fatal(err)
	}
	// The snapshot is slow: every fourth update overflows the buffer of three,
	// which abandons the attempt and starts another one.
	for seq := int64(17); seq <= 26; seq++ {
		if err := h.fake.Push(l2(seq, seq, "", fmt.Sprintf(`[["5","%d","%d"]]`, seq, seq))); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, func() bool { return book.Resyncs() == 2 }, "two buffer overflows")
	close(release)
	evs := expectEvents(t, book, orderbook.EventStale, orderbook.EventStale, orderbook.EventSnapshot)
	if !errors.Is(evs[0].Err, orderbook.ErrBufferOverflow) || !errors.Is(evs[1].Err, orderbook.ErrBufferOverflow) {
		t.Fatalf("stale causes: %v, %v", evs[0].Err, evs[1].Err)
	}
	if book.Book().Sequence() != 26 || calls.Load() != 3 || book.Resyncs() != 2 || book.State() != orderbook.SyncSynced {
		t.Fatalf("sequence=%d snapshot calls=%d resyncs=%d state=%v", book.Book().Sequence(), calls.Load(), book.Resyncs(), book.State())
	}
}

func TestBookHandler_DroppedFramesForceAResyncAndAForeignFrameIsIgnored(t *testing.T) {
	wstest.CheckLeaks(t)
	var calls atomic.Int32
	events := make(chan orderbook.Event, 16)
	sub := stream.NewSubscription[orderbook.Event]("test", 0, nil)
	syncer := orderbook.NewSyncer(orderbook.SyncerConfig{
		Symbol: "BTC-USDT",
		Snapshot: func(context.Context) (orderbook.Snapshot, error) {
			n := calls.Add(1)
			return orderbook.Snapshot{Symbol: "BTC-USDT", Sequence: int64(n) * 10, Bids: []orderbook.Level{lvl("1", "1")}}, nil
		},
		OnEvent: func(ev orderbook.Event) { events <- ev },
	})
	handler := &bookHandler{sub: sub, syncer: syncer, report: func(error) {}}
	next := func() orderbook.Event {
		select {
		case ev := <-events:
			return ev
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for a book event")
			return orderbook.Event{}
		}
	}
	syncer.Start()
	<-syncer.Ready()
	if ev := next(); ev.Type != orderbook.EventSnapshot || ev.Sequence != 10 {
		t.Fatalf("first event: %+v", ev)
	}
	// A frame that is not a Classic message cannot be part of the book.
	handler.OnFrame(stream.Frame{Msg: "not a classic message"})
	// The overflow policy of the connection dropped seven frames: updates are
	// missing, so the book is rebuilt.
	handler.OnGap(7)
	if ev := next(); ev.Type != orderbook.EventStale || !errors.Is(ev.Err, orderbook.ErrUpdatesDropped) {
		t.Fatalf("after a gap: %+v", ev)
	}
	if ev := next(); ev.Type != orderbook.EventSnapshot || ev.Sequence != 20 {
		t.Fatalf("resynchronised: %+v", ev)
	}
	if sub.Dropped() != 7 || calls.Load() != 2 || syncer.Resyncs() != 1 {
		t.Fatalf("dropped=%d snapshot calls=%d resyncs=%d", sub.Dropped(), calls.Load(), syncer.Resyncs())
	}
	// A restored connection (a reset marker) clears the book; the first update that
	// arrives afterwards proves the stream is alive again and starts the resync.
	handler.OnReset(2)
	if ev := next(); ev.Type != orderbook.EventStale || !errors.Is(ev.Err, orderbook.ErrReconnected) {
		t.Fatalf("after a reset: %+v", ev)
	}
	handler.OnFrame(stream.Frame{Msg: &classic.Message{Topic: l2Topic, Subject: "trade.l2update",
		Data: []byte(`{"changes":{"asks":[],"bids":[["1","2","25"]]},"sequenceStart":25,"sequenceEnd":25,"symbol":"BTC-USDT","time":1}`)}})
	if ev := next(); ev.Type != orderbook.EventSnapshot || ev.Sequence != 30 {
		t.Fatalf("resynchronised after the reset: %+v", ev)
	}
	if calls.Load() != 3 || syncer.Resyncs() != 2 {
		t.Fatalf("snapshot calls=%d resyncs=%d", calls.Load(), syncer.Resyncs())
	}
	handler.OnClosed()
	if _, ok := <-sub.C(); ok {
		t.Fatal("C must be closed once the handler is closed")
	}
	if syncer.State() != orderbook.SyncClosed {
		t.Fatalf("syncer state = %v", syncer.State())
	}
}

func TestOrderBook_UndecodableUpdateIsTreatedAsALostUpdate(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) {
		n := calls.Add(1)
		return orderbook.Snapshot{Symbol: "BTC-USDT", Sequence: int64(n) * 100, Bids: []orderbook.Level{lvl("1", "1")}}, nil
	}
	s := h.public(t)
	book, err := s.SubscribeOrderBook(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	// A row that is not a price/size pair.
	if err := h.fake.Push(wstest.Message(l2Topic, "trade.l2update", `{"changes":{"asks":[["garbage","1","101"]],"bids":[]},"sequenceStart":101,"sequenceEnd":101,"symbol":"BTC-USDT","time":1}`)); err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventStale, orderbook.EventSnapshot)
	if s.Stats().DecodeErrors != 1 || calls.Load() != 2 {
		t.Fatalf("decode errors=%d snapshot calls=%d", s.Stats().DecodeErrors, calls.Load())
	}
	// A push without a usable sequence range cannot be followed either.
	if err := h.fake.Push(wstest.Message(l2Topic, "trade.l2update", `{"changes":{"asks":[],"bids":[["1","5","201"]]},"symbol":"BTC-USDT","time":1}`)); err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventStale, orderbook.EventSnapshot)
	if s.Stats().DecodeErrors != 2 || calls.Load() != 3 {
		t.Fatalf("decode errors=%d snapshot calls=%d", s.Stats().DecodeErrors, calls.Load())
	}
}

func TestOrderBook_CloseEndsTheStreamAndReleasesTheSyncer(t *testing.T) {
	h := newHarness(t)
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) { return docSnapshot(), nil }
	book, err := h.public(t).SubscribeOrderBook(ctx5(t), "BTC-USDT")
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

func TestOrderBook_ClosingTheSessionReleasesTheBookWhileItSynchronises(t *testing.T) {
	h := newHarness(t)
	started := make(chan struct{}, 1)
	h.snapshots = func(ctx context.Context, _ string) (orderbook.Snapshot, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done() // a snapshot call that never answers
		return orderbook.Snapshot{}, ctx.Err()
	}
	s := h.public(t)
	book, err := s.SubscribeOrderBook(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the snapshot was never requested")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for range book.C() {
	}
	if book.State() != orderbook.SyncClosed || book.Err() != nil {
		t.Fatalf("state=%v err=%v", book.State(), book.Err())
	}
}

func TestOrderBook_RejectedSubscriptionLeavesNothingBehind(t *testing.T) {
	h := newHarness(t)
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) { return docSnapshot(), nil }
	h.fake.Reject(l2Topic, 404)
	_, err := h.public(t).SubscribeOrderBook(ctx5(t), "BTC-USDT")
	if !errors.Is(err, stream.ErrTopicNotFound) {
		t.Fatalf("error = %v", err)
	}
}

func TestOrderBook_ConcurrentReadersWhileStreaming(t *testing.T) {
	h := newHarness(t)
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) { return docSnapshot(), nil }
	s := h.public(t)
	book, err := s.SubscribeOrderBook(ctx5(t), "BTC-USDT", stream.WithBuffer(10000))
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
	frames := make([]string, 0, 583)
	for seq := int64(17); seq < 600; seq++ {
		frames = append(frames, l2(seq, seq, "", fmt.Sprintf(`[["3988.%02d","%d","%d"]]`, 10+seq%30, seq, seq)))
	}
	pushFlood(t, h, frames)
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

func TestOrderBook_WorksOnAPrivateSessionToo(t *testing.T) {
	h := newHarness(t)
	h.snapshots = func(context.Context, string) (orderbook.Snapshot, error) { return docSnapshot(), nil }
	book, err := h.private(t).SubscribeOrderBook(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	if bid, _ := book.Book().BestBid(); bid.Price != "3988.51" {
		t.Fatalf("best bid = %v", bid)
	}
	if h.fake.Last("subscribe")["privateChannel"] != false {
		t.Fatal("the order-book channel is public even on a private session")
	}
}
