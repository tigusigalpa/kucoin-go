package streaming

import (
	"context"
	"encoding/json"
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
	"github.com/tigusigalpa/kucoin-go/websocket/uta"
)

const (
	bookSymbol = "XBTUSDTM"
	bookKey    = "obu|XBTUSDTM|increment@10ms"
	restKey    = "obu|BTC-USDT|increment"
)

// levelsJSON renders levels as the body of a JSON array of [price, size] pairs.
func levelsJSON(ls []orderbook.Level) string {
	parts := make([]string, len(ls))
	for i, l := range ls {
		parts[i] = fmt.Sprintf(`[%q,%q]`, l.Price, l.Size)
	}
	return strings.Join(parts, ",")
}

// obuPush builds an order-book push of the futures XBTUSDTM.
func obuPush(depth, kind string, start, end int64, bids, asks []orderbook.Level) string {
	return fmt.Sprintf(`{"T":"obu.FUTURES","dp":%q,"t":%q,"P":%d,"d":{"s":%q,"O":%d,"C":%d,"M":1,"b":[%s],"a":[%s]}}`,
		depth, kind, 1000+end, bookSymbol, start, end, levelsJSON(bids), levelsJSON(asks))
}

func snapshotPush(seq int64, bids, asks []orderbook.Level) string {
	return obuPush("increment@10ms", "snapshot", seq, seq, bids, asks)
}

func deltaPush(start, end int64, bids, asks []orderbook.Level) string {
	return obuPush("increment@10ms", "delta", start, end, bids, asks)
}

// The book every test starts from, at sequence 100.
var (
	baseBids = []orderbook.Level{lv("99", "1"), lv("98", "2")}
	baseAsks = []orderbook.Level{lv("101", "1"), lv("102", "2")}
)

func levels(ls []orderbook.Level) string {
	s := ""
	for _, l := range ls {
		s += fmt.Sprintf("%s:%s ", l.Price, l.Size)
	}
	return s
}

// pushPaced sends frames with a short pause after each one. A tight loop of writes
// would pile up in the socket buffers and make the outcome depend on how fast the
// machine is relative to the connection's own goroutines.
func pushPaced(t *testing.T, fake *wstest.UTAFake, frames ...string) {
	t.Helper()
	for _, f := range frames {
		if err := fake.Push(f); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Microsecond)
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

// quick is the tuning that makes a book resubscribe without waiting.
var quick = orderbook.Tuning{RetryMin: time.Millisecond, RetryMax: 2 * time.Millisecond}

// A book of the 10ms feed starts from the snapshot KuCoin pushes first and then
// follows the deltas by KuCoin's sequence rule.
func TestOrderBook_SnapshotThenDeltas(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey,
		snapshotPush(100, baseBids, baseAsks),
		deltaPush(99, 100, []orderbook.Level{lv("99", "7")}, nil),                                // ends at 100: already contained, ignored
		deltaPush(100, 102, []orderbook.Level{lv("99", "3")}, []orderbook.Level{lv("101", "0")}), // overlaps the snapshot: applies
		deltaPush(103, 103, []orderbook.Level{lv("100", "4")}, nil),                              // continues it
		deltaPush(100, 106, nil, []orderbook.Level{lv("102", "5"), lv("103", "6")}),              // overlaps what is applied: applies
	)
	book, err := h.futures(t).SubscribeOrderBook(ctx5(t), bookSymbol)
	if err != nil {
		t.Fatal(err)
	}
	f := h.frames("subscribe", "obu")[0]
	if f["depth"] != "increment@10ms" || f["symbol"] != bookSymbol || f["tradeType"] != "FUTURES" || f["rpiFilter"] != nil {
		t.Fatalf("subscribe frame: %v", f)
	}
	<-book.Ready()
	evs := expectEvents(t, book, orderbook.EventSnapshot, orderbook.EventUpdate, orderbook.EventUpdate, orderbook.EventUpdate)
	if evs[0].Sequence != 100 || evs[0].Symbol != bookSymbol || evs[0].Book != book.Book() {
		t.Fatalf("snapshot event: %+v", evs[0])
	}
	equal(t, "first update", evs[1].Changes, []orderbook.Change{
		{Side: orderbook.Bid, Price: "99", Size: "3"}, {Side: orderbook.Ask, Price: "101", Size: "0"},
	})
	if evs[1].Sequence != 102 || evs[2].Sequence != 103 || evs[3].Sequence != 106 {
		t.Fatalf("update sequences: %d %d %d", evs[1].Sequence, evs[2].Sequence, evs[3].Sequence)
	}
	snap := book.Book().Snapshot(0)
	if snap.Sequence != 106 || levels(snap.Bids) != "100:4 99:3 98:2 " || levels(snap.Asks) != "102:5 103:6 " {
		t.Fatalf("book: seq %d bids %s asks %s", snap.Sequence, levels(snap.Bids), levels(snap.Asks))
	}
	if book.State() != orderbook.SyncSynced || book.Resyncs() != 0 || book.Dropped() != 0 || book.Book().Symbol() != bookSymbol {
		t.Fatalf("state=%v resyncs=%d dropped=%d", book.State(), book.Resyncs(), book.Dropped())
	}
	if n := h.fake.Count("subscribe", "obu"); n != 1 {
		t.Fatalf("%d subscribe frames", n)
	}
	if book.Key() != (uta.SubscribeSpec{Channel: "obu", TradeType: "FUTURES", Symbols: []string{bookSymbol}, Depth: "increment@10ms"}).Name() {
		t.Fatalf("key = %q", book.Key())
	}
}

// A sequence gap cannot be repaired from the stream: the book is cleared and
// subscribes again, which makes KuCoin push a fresh snapshot.
func TestOrderBook_SequenceGapResubscribesAndTheNewSnapshotRecoversIt(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, baseBids, baseAsks))
	book, err := h.futures(t).SubscribeOrderBookTuned(ctx5(t), bookSymbol, quick)
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)

	// What the server will push when asked again: a snapshot of a later state.
	h.fake.OnSubscribe(bookKey, snapshotPush(200, []orderbook.Level{lv("500", "5")}, []orderbook.Level{lv("501", "5")}))
	if err := h.fake.Push(deltaPush(101, 101, []orderbook.Level{lv("99", "9")}, nil)); err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventUpdate)
	if err := h.fake.Push(deltaPush(110, 110, []orderbook.Level{lv("99", "8")}, nil)); err != nil { // 102..109 were lost
		t.Fatal(err)
	}
	evs := expectEvents(t, book, orderbook.EventStale, orderbook.EventSnapshot)
	var gap *orderbook.GapError
	if !errors.Is(evs[0].Err, orderbook.ErrSequenceGap) || !errors.As(evs[0].Err, &gap) || gap.Have != 101 || gap.Start != 110 {
		t.Fatalf("stale cause: %v", evs[0].Err)
	}
	<-book.Ready()
	if bid, _ := book.Book().BestBid(); bid.Price != "500" || book.Book().Sequence() != 200 {
		t.Fatalf("the book must come from the second snapshot: %v seq=%d", bid, book.Book().Sequence())
	}
	if book.Resyncs() != 1 || h.fake.Count("subscribe", "obu") != 2 || h.fake.Count("unsubscribe", "obu") != 1 {
		t.Fatalf("resyncs=%d subscribes=%d unsubscribes=%d", book.Resyncs(), h.fake.Count("subscribe", "obu"), h.fake.Count("unsubscribe", "obu"))
	}
	// Streaming continues from the new baseline.
	if err := h.fake.Push(deltaPush(201, 201, []orderbook.Level{lv("500", "9")}, nil)); err != nil {
		t.Fatal(err)
	}
	if ev := expectEvents(t, book, orderbook.EventUpdate)[0]; ev.Sequence != 201 {
		t.Fatalf("update: %+v", ev)
	}
}

// If the stream pushes a snapshot by itself before the book got round to
// subscribing again, the book takes it and leaves the connection alone.
func TestOrderBook_GapRecoversWhenTheStreamPushesASnapshotItself(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, baseBids, baseAsks))
	slow := orderbook.Tuning{RetryMin: 600 * time.Millisecond, RetryMax: 600 * time.Millisecond}
	book, err := h.futures(t).SubscribeOrderBookTuned(ctx5(t), bookSymbol, slow)
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)

	if err := h.fake.Push(deltaPush(150, 150, []orderbook.Level{lv("99", "8")}, nil)); err != nil { // a gap
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventStale)
	if book.Book().Ready() || book.State() != orderbook.SyncIdle {
		t.Fatalf("a stale book must not present itself as ready: ready=%v state=%v", book.Book().Ready(), book.State())
	}
	// Updates of a stale book are ignored until a snapshot arrives.
	if err := h.fake.Push(deltaPush(151, 151, []orderbook.Level{lv("99", "7")}, nil)); err != nil {
		t.Fatal(err)
	}
	if err := h.fake.Push(snapshotPush(300, []orderbook.Level{lv("700", "1")}, []orderbook.Level{lv("701", "1")})); err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventSnapshot)
	if book.Book().Sequence() != 300 || book.State() != orderbook.SyncSynced {
		t.Fatalf("seq=%d state=%v", book.Book().Sequence(), book.State())
	}
	// Wait out the book's own resubscription delay (at least 450ms): it must find the
	// book healthy.
	time.Sleep(700 * time.Millisecond)
	if n := h.fake.Count("subscribe", "obu"); n != 1 {
		t.Fatalf("%d subscribe frames; the recovered book must not subscribe again", n)
	}
	if book.State() != orderbook.SyncSynced {
		t.Fatalf("state = %v", book.State())
	}
}

// After a reconnect the engine subscribes again; the new connection's snapshot
// restores the book without the book doing anything itself.
func TestOrderBook_ReconnectStalesTheBookAndTheNewConnectionsSnapshotRestoresIt(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, baseBids, baseAsks))
	s := h.futures(t)
	book, err := s.SubscribeOrderBook(ctx5(t), bookSymbol)
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)

	h.fake.OnSubscribe(bookKey, snapshotPush(150, []orderbook.Level{lv("120", "3")}, []orderbook.Level{lv("121", "3")}))
	h.fake.Server.DropAll()
	ev := expectEvents(t, book, orderbook.EventStale)[0]
	if !errors.Is(ev.Err, orderbook.ErrReconnected) {
		t.Fatalf("stale cause = %v", ev.Err)
	}
	expectEvents(t, book, orderbook.EventSnapshot)
	if book.Book().Sequence() != 150 || !book.Book().Ready() {
		t.Fatalf("seq=%d ready=%v", book.Book().Sequence(), book.Book().Ready())
	}
	eventually(t, func() bool { return s.State() == stream.StateConnected && h.fake.Server.Connections() == 2 }, "reconnected")
	if h.fake.Count("subscribe", "obu") != 2 || h.fake.Count("unsubscribe", "obu") != 0 || book.Resyncs() != 1 {
		t.Fatalf("subscribes=%d unsubscribes=%d resyncs=%d; the engine resubscribed, the book must not do it again",
			h.fake.Count("subscribe", "obu"), h.fake.Count("unsubscribe", "obu"), book.Resyncs())
	}
}

// A consumer that never reads the events must not freeze the book: it follows the
// stream whether or not anybody listens, and the events that pile up are collapsed
// into reload markers. Only end states are asserted: how many frames the
// connection absorbed without help and how often the book had to subscribe again
// depend on the machine, the sequence it ends at and the mirror it can feed do not.
func TestOrderBook_ConsumerThatNeverReadsDoesNotFreezeTheBook(t *testing.T) {
	h := newHarness(t)
	const last = 160
	frames := []string{snapshotPush(100, baseBids, baseAsks)}
	for seq := int64(101); seq <= last; seq++ {
		frames = append(frames, deltaPush(seq, seq, []orderbook.Level{lv("99", fmt.Sprint(seq))}, nil))
	}
	h.fake.OnSubscribe(bookKey, frames...)
	book, err := h.futures(t).SubscribeOrderBook(ctx5(t), bookSymbol, stream.WithBuffer(8))
	if err != nil {
		t.Fatal(err)
	}
	// Not a single event is read. Whether the burst was absorbed or cost a
	// resubscription, the book must reach the last sequence by itself.
	eventually(t, func() bool { return book.State() == orderbook.SyncSynced && book.Book().Sequence() == last }, "the book to reach the last sequence without anybody reading its events")

	// A consumer that follows the documented protocol ends up with the live book:
	// reload from the book on an EventSnapshot (when the book is ready), treat an
	// EventStale as "my copy is invalid", and skip the updates a reload already
	// contains. The book may still be resynchronising while it catches up, so the
	// consumer keeps following until everything is quiet.
	mirror := orderbook.New(bookSymbol)
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
		live := book.Book().Snapshot(0)
		if book.State() == orderbook.SyncSynced && valid && len(live.Bids) > 0 && mirror.Sequence() == live.Sequence &&
			levels(mirror.Snapshot(0).Bids) == levels(live.Bids) && levels(mirror.Snapshot(0).Asks) == levels(live.Asks) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the mirror (valid=%v, sequence %d) never caught up with the book (state %v, sequence %d)", valid, mirror.Sequence(), book.State(), live.Sequence)
		}
		time.Sleep(time.Millisecond)
	}
}

// With stream.FailSubscription a consumer that falls behind loses the
// subscription instead of being given a reload marker. The consumer never reads,
// so what does not fit into a queue of 8 has nowhere to go however slowly it
// arrives.
func TestOrderBook_FailSubscriptionPolicyEndsASlowConsumersBook(t *testing.T) {
	h := newHarness(t)
	frames := []string{snapshotPush(100, baseBids, baseAsks)}
	for seq := int64(101); seq <= 160; seq++ {
		frames = append(frames, deltaPush(seq, seq, []orderbook.Level{lv("99", "1")}, nil))
	}
	h.fake.OnSubscribe(bookKey, frames...)
	book, err := h.futures(t).SubscribeOrderBook(ctx5(t), bookSymbol, stream.WithBuffer(8), stream.WithOverflow(stream.FailSubscription))
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
	eventually(t, func() bool { return h.fake.Count("unsubscribe", "obu") == 1 }, "the failed book unsubscribes from the stream")
	for range book.C() { // the channel closes
	}
}

func TestOrderBook_UndecodableUpdateIsTreatedAsALostUpdate(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, baseBids, baseAsks))
	s := h.futures(t)
	book, err := s.SubscribeOrderBookTuned(ctx5(t), bookSymbol, quick)
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	h.fake.OnSubscribe(bookKey, snapshotPush(200, baseBids, baseAsks))
	// A level without a size cannot be decoded: an update that is missing.
	if err := h.fake.Push(`{"T":"obu.FUTURES","dp":"increment@10ms","t":"delta","P":1,"d":{"s":"XBTUSDTM","O":101,"C":101,"b":[["1"]],"a":[]}}`); err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventStale, orderbook.EventSnapshot)
	if s.Stats().DecodeErrors != 1 || h.fake.Count("subscribe", "obu") != 2 || book.Book().Sequence() != 200 {
		t.Fatalf("decode errors=%d subscribes=%d seq=%d", s.Stats().DecodeErrors, h.fake.Count("subscribe", "obu"), book.Book().Sequence())
	}
}

func TestOrderBook_InvalidLevelIsTreatedAsAGap(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, baseBids, baseAsks))
	s := h.futures(t)
	book, err := s.SubscribeOrderBookTuned(ctx5(t), bookSymbol, quick)
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	h.fake.OnSubscribe(bookKey, snapshotPush(200, baseBids, baseAsks))
	// The price decodes (a decimal is any text) but is not a number: the delta cannot be applied.
	if err := h.fake.Push(deltaPush(101, 101, []orderbook.Level{lv("abc", "1")}, nil)); err != nil {
		t.Fatal(err)
	}
	evs := expectEvents(t, book, orderbook.EventStale, orderbook.EventSnapshot)
	if !errors.Is(evs[0].Err, orderbook.ErrInvalidLevel) || s.Stats().DecodeErrors != 0 || book.Book().Sequence() != 200 {
		t.Fatalf("stale cause=%v decode errors=%d seq=%d", evs[0].Err, s.Stats().DecodeErrors, book.Book().Sequence())
	}
}

// A subscription that is acknowledged but never followed by a snapshot is retried;
// when it keeps failing the book ends with a typed error.
func TestOrderBook_NoSnapshotIsRetriedThenTheBookFails(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey) // acknowledges and then says nothing
	tuning := orderbook.Tuning{ResetGrace: 40 * time.Millisecond, RetryMin: time.Millisecond, RetryMax: 2 * time.Millisecond, MaxAttempts: 3}
	book, err := h.futures(t).SubscribeOrderBookTuned(ctx5(t), bookSymbol, tuning)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-book.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("the book did not give up: state=%v subscribes=%d", book.State(), h.fake.Count("subscribe", "obu"))
	}
	if !errors.Is(book.Err(), stream.ErrResyncFailed) {
		t.Fatalf("Err = %v", book.Err())
	}
	for range book.C() { // the channel closes; no event is delivered for this feed
	}
	select {
	case <-book.Ready():
		t.Fatal("a book that never got a snapshot is never ready")
	default:
	}
	if n := h.fake.Count("subscribe", "obu"); n != 3 {
		t.Fatalf("%d subscribe frames, want the first and two retries", n)
	}
	eventually(t, func() bool { return h.fake.Count("unsubscribe", "obu") == 3 }, "every subscription is unsubscribed again")
	if book.State() != orderbook.SyncClosed {
		t.Fatalf("state = %v", book.State())
	}
}

func TestOrderBook_ResubscriptionRejectedByTheServerEndsTheBook(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, baseBids, baseAsks))
	tuning := orderbook.Tuning{RetryMin: time.Millisecond, RetryMax: 2 * time.Millisecond, MaxAttempts: 2}
	book, err := h.futures(t).SubscribeOrderBookTuned(ctx5(t), bookSymbol, tuning)
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	h.fake.Reject(bookKey, "symbol delisted")
	if err := h.fake.Push(deltaPush(500, 500, nil, nil)); err != nil { // a gap
		t.Fatal(err)
	}
	select {
	case <-book.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the book did not end")
	}
	var se *stream.ServerError
	if !errors.Is(book.Err(), stream.ErrResyncFailed) || !errors.Is(book.Err(), uta.ErrSubscriptionFailed) || !errors.As(book.Err(), &se) || se.Message != "symbol delisted" {
		t.Fatalf("Err = %v", book.Err())
	}
	for range book.C() {
	}
	if n := h.fake.Count("subscribe", "obu"); n != 3 {
		t.Fatalf("%d subscribe frames, want the first and two rejected retries", n)
	}
}

func TestOrderBook_CloseEndsTheStreamAndReleasesEverything(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, baseBids, baseAsks))
	book, err := h.futures(t).SubscribeOrderBook(ctx5(t), bookSymbol)
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	if err := book.Close(); err != nil {
		t.Fatal(err)
	}
	if err := book.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	for range book.C() { // drain until closed
	}
	if book.Err() != nil {
		t.Fatalf("a requested close is not an error: %v", book.Err())
	}
	if h.fake.Count("unsubscribe", "obu") != 1 {
		t.Fatal("the server never saw the unsubscribe")
	}
	if book.State() != orderbook.SyncClosed {
		t.Fatalf("syncer state = %v", book.State())
	}
}

func TestOrderBook_ClosingWhileAResubscriptionIsPending(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, baseBids, baseAsks))
	slow := orderbook.Tuning{RetryMin: 10 * time.Second, RetryMax: 10 * time.Second}
	book, err := h.futures(t).SubscribeOrderBookTuned(ctx5(t), bookSymbol, slow)
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	if err := h.fake.Push(deltaPush(500, 500, nil, nil)); err != nil { // a gap: the worker now waits to resubscribe
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventStale)
	if err := book.Close(); err != nil {
		t.Fatal(err)
	}
	for range book.C() {
	}
	if book.Err() != nil || h.fake.Count("subscribe", "obu") != 1 {
		t.Fatalf("Err=%v subscribes=%d; closing must cancel the pending resubscription", book.Err(), h.fake.Count("subscribe", "obu"))
	}
}

func TestOrderBook_SessionCloseEndsTheBook(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, baseBids, baseAsks))
	s := h.futures(t)
	book, err := s.SubscribeOrderBook(ctx5(t), bookSymbol)
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-book.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the book outlived its session")
	}
	for range book.C() {
	}
	if book.Err() != nil || book.State() != orderbook.SyncClosed {
		t.Fatalf("Err=%v state=%v", book.Err(), book.State())
	}
}

func TestOrderBook_RejectedSubscriptionLeavesNothingBehind(t *testing.T) {
	h := newHarness(t)
	h.fake.Reject(bookKey, "topic not allowed")
	_, err := h.futures(t).SubscribeOrderBook(ctx5(t), bookSymbol)
	var se *stream.ServerError
	if !errors.Is(err, uta.ErrSubscriptionFailed) || !errors.As(err, &se) {
		t.Fatalf("error = %v", err)
	}
}

func TestOrderBook_ConcurrentReadersWhileStreaming(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, []orderbook.Level{lv("90", "1"), lv("89", "1")}, []orderbook.Level{lv("101", "1")}))
	book, err := h.futures(t).SubscribeOrderBook(ctx5(t), bookSymbol, stream.WithBuffer(10000))
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)

	var stop atomic.Bool
	var wg sync.WaitGroup
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()
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
				_ = book.Dropped()
				runtime.Gosched() // a reader that never yields would starve everything else on one CPU
			}
		}()
	}
	const last = 300
	frames := make([]string, 0, last-100)
	for seq := int64(101); seq <= last; seq++ {
		bid := lv(fmt.Sprint(70+seq%15), fmt.Sprint(seq))
		frames = append(frames, deltaPush(seq, seq, []orderbook.Level{bid}, nil))
	}
	pushPaced(t, h.fake, frames...)
	// The events are not counted (how many of them are delivered, rather than
	// collapsed, is up to the machine); the end state is what matters.
	deadline := time.After(10 * time.Second)
	for book.Book().Sequence() != last {
		select {
		case <-book.C():
		case <-deadline:
			t.Fatalf("the book stopped at sequence %d", book.Book().Sequence())
		case <-time.After(time.Millisecond):
		}
	}
	if book.State() != orderbook.SyncSynced {
		t.Fatalf("state = %v", book.State())
	}
}

// The engine reports updates it had to drop; the book can no longer follow the
// sequence and subscribes again.
func TestOrderBook_DroppedFramesMakeTheBookSubscribeAgain(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, baseBids, baseAsks))
	book, err := h.futures(t).SubscribeOrderBookTuned(ctx5(t), bookSymbol, quick)
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	h.fake.OnSubscribe(bookKey, snapshotPush(200, baseBids, baseAsks))

	book.core.mu.Lock()
	handler := book.core.handler
	book.core.mu.Unlock()
	handler.OnGap(3) // what the engine's pump calls when its queue dropped frames
	evs := expectEvents(t, book, orderbook.EventStale, orderbook.EventSnapshot)
	if !errors.Is(evs[0].Err, orderbook.ErrUpdatesDropped) || book.Book().Sequence() != 200 || h.fake.Count("subscribe", "obu") != 2 {
		t.Fatalf("stale cause=%v seq=%d subscribes=%d", evs[0].Err, book.Book().Sequence(), h.fake.Count("subscribe", "obu"))
	}
}

// A subscription the book has replaced stays silent even if its pump still holds a
// frame, and a frame that is not an order-book push is ignored.
func TestOrderBook_RetiredHandlersAreIgnored(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, baseBids, baseAsks))
	book, err := h.futures(t).SubscribeOrderBookTuned(ctx5(t), bookSymbol, quick)
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	book.core.mu.Lock()
	old := book.core.handler
	book.core.mu.Unlock()
	h.fake.OnSubscribe(bookKey, snapshotPush(200, baseBids, baseAsks))
	if err := h.fake.Push(deltaPush(900, 900, nil, nil)); err != nil { // a gap
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventStale, orderbook.EventSnapshot)
	if !old.retired.Load() {
		t.Fatal("the replaced subscription must be retired")
	}

	frame := stream.Frame{Msg: &uta.Push{T: "obu.FUTURES", Depth: "increment@10ms", Kind: "snapshot", Data: json.RawMessage(`{"s":"XBTUSDTM","O":999,"C":999,"b":[],"a":[]}`)}}
	old.OnFrame(frame)
	old.OnReset(7)
	old.OnGap(1)
	old.OnAbort(errors.New("late abort"))
	old.OnClosed()
	// The snapshot of the new subscription can reach the book before the worker has
	// recorded that subscription; wait for it.
	var current *bookHandler
	eventually(t, func() bool {
		book.core.mu.Lock()
		defer book.core.mu.Unlock()
		current = book.core.handler
		return current != nil && !current.retired.Load()
	}, "the book to record its new subscription")
	current.OnFrame(stream.Frame{Msg: "not a push"}) // ignored
	current.OnFrame(stream.Frame{})                  // ignored
	if book.Book().Sequence() != 200 || book.State() != orderbook.SyncSynced || book.Err() != nil {
		t.Fatalf("a retired handler disturbed the book: seq=%d state=%v err=%v", book.Book().Sequence(), book.State(), book.Err())
	}
	select {
	case <-book.Done():
		t.Fatal("a retired handler ended the book")
	default:
	}
}

// The connection layer reports a restored connection (a reset marker) to the
// book's handler in stream order. The engine subscribes again by itself, which
// makes KuCoin push a new snapshot; until it arrives the book is stale, and if it
// never does, the book subscribes again on its own.
func TestBookHandler_ResetMarkerStalesTheBookAndAMissingSnapshotIsRequestedAgain(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, baseBids, baseAsks))
	// (The grace period also bounds the wait for the first snapshot; it is long enough
	// for a machine that is slow but not stalled.)
	tuning := orderbook.Tuning{ResetGrace: 200 * time.Millisecond, RetryMin: time.Millisecond, RetryMax: 2 * time.Millisecond}
	book, err := h.futures(t).SubscribeOrderBookTuned(ctx5(t), bookSymbol, tuning)
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	h.fake.OnSubscribe(bookKey, snapshotPush(200, baseBids, baseAsks))

	book.core.mu.Lock()
	handler := book.core.handler
	book.core.mu.Unlock()
	handler.OnReset(2) // what the engine's pump calls once the connection is restored
	ev := expectEvents(t, book, orderbook.EventStale)[0]
	if !errors.Is(ev.Err, orderbook.ErrReconnected) {
		t.Fatalf("stale cause = %v", ev.Err)
	}
	// No snapshot follows the marker, as no connection was really lost: the book asks
	// for one itself once the grace period is over.
	expectEvents(t, book, orderbook.EventSnapshot)
	if book.Book().Sequence() != 200 || book.State() != orderbook.SyncSynced || book.Resyncs() < 1 || h.fake.Count("subscribe", "obu") < 2 {
		t.Fatalf("seq=%d state=%v resyncs=%d subscribes=%d", book.Book().Sequence(), book.State(), book.Resyncs(), h.fake.Count("subscribe", "obu"))
	}
}

// Too many updates pile up while the REST snapshot is being fetched: the attempt
// is abandoned, the consumer is told, and the book rebuilds itself from a fresh
// snapshot. The stalled first fetch makes this independent of machine speed.
func TestOrderBookIncrement_TooManyBufferedUpdatesRestartTheSyncAndTheBookRecovers(t *testing.T) {
	h := newHarness(t)
	const last = 50
	release := make(chan struct{})
	var calls atomic.Int32
	h.snapshots = func(ctx context.Context, tradeType, symbol string) (orderbook.Snapshot, error) {
		if calls.Add(1) == 1 {
			select { // the first fetch is slow: updates pile up behind it
			case <-release:
			case <-ctx.Done():
				return orderbook.Snapshot{}, ctx.Err()
			}
		}
		return orderbook.Snapshot{Symbol: symbol, Sequence: last, Bids: []orderbook.Level{lv("100", "9")}, Asks: []orderbook.Level{lv("101", "9")}}, nil
	}
	frames := make([]string, 0, last)
	for seq := int64(1); seq <= last; seq++ {
		frames = append(frames, restPush(seq, []orderbook.Level{lv("115404", "1")}, nil))
	}
	h.fake.OnSubscribe(restKey, frames...)
	book, err := h.spot(t).SubscribeOrderBookIncrementTuned(ctx5(t), "BTC-USDT", orderbook.Tuning{MaxBuffered: 3, RetryMin: time.Millisecond, RetryMax: 2 * time.Millisecond})
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
				if h.fake.Count("subscribe", "obu") != 1 {
					t.Fatal("the REST-synchronised book resynchronises over REST, not by subscribing again")
				}
				return
			}
		case <-deadline:
			t.Fatalf("book never recovered: state=%v seq=%d resyncs=%d", book.State(), book.Book().Sequence(), book.Resyncs())
		}
	}
}

// Neither the constructor nor State, Resyncs, Dropped or Book wait for a consumer
// that is not reading, however many events are pending.
func TestOrderBook_NothingWaitsForAConsumerThatIsNotReading(t *testing.T) {
	const last = 200
	for _, rest := range []bool{false, true} {
		name := "10ms feed"
		if rest {
			name = "REST-synchronised increment feed"
		}
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.snapshots = func(context.Context, string, string) (orderbook.Snapshot, error) {
				return orderbook.Snapshot{Symbol: bookSymbol, Sequence: 100, Bids: baseBids, Asks: baseAsks}, nil
			}
			var frames []string
			key := bookKey
			if rest {
				key = "obu|XBTUSDTM|increment"
			}
			depth := map[bool]string{false: "increment@10ms", true: "increment"}[rest]
			if !rest {
				frames = append(frames, snapshotPush(100, baseBids, baseAsks))
			}
			for seq := int64(101); seq <= last; seq++ {
				frames = append(frames, obuPush(depth, "delta", seq, seq, []orderbook.Level{lv("99", fmt.Sprint(seq))}, nil))
			}
			h.fake.OnSubscribe(key, frames...)
			s := h.futures(t)

			type result struct {
				book *BookStream
				err  error
			}
			made := make(chan result, 1)
			go func() {
				var b *BookStream
				var err error
				if rest {
					b, err = s.SubscribeOrderBookIncrement(ctx5(t), bookSymbol, stream.WithBuffer(4))
				} else {
					b, err = s.SubscribeOrderBook(ctx5(t), bookSymbol, stream.WithBuffer(4))
				}
				made <- result{b, err}
			}()
			var book *BookStream
			select {
			case r := <-made:
				if r.err != nil {
					t.Fatal(r.err)
				}
				book = r.book
			case <-time.After(5 * time.Second):
				t.Fatal("the constructor waited for a consumer that cannot read before it returned")
			}
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				for i := 0; i < 200; i++ {
					_ = book.State()
					_ = book.Resyncs()
					_ = book.Dropped()
					_ = book.Book().Snapshot(5)
					time.Sleep(time.Millisecond)
				}
			}()
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("the book's accessors waited for a consumer that is not reading")
			}
			eventually(t, func() bool { return book.Book().Sequence() == last }, "the book to keep following the stream")
		})
	}
}

// ---- the deprecated REST-synchronised increment book ------------------------------

// restDoc is the REST snapshot of the order-book example of KuCoin's UTA
// documentation (sequence 100001).
func restDoc() orderbook.Snapshot {
	return orderbook.Snapshot{
		Symbol: "BTC-USDT", Sequence: 100001,
		Asks: []orderbook.Level{lv("115669", "0.1"), lv("115553.5", "0.05"), lv("115442", "0.2")},
		Bids: []orderbook.Level{lv("115404", "0.5"), lv("115403.5", "0.3"), lv("115388.9", "0.1")},
	}
}

// The documentation's spot increment pushes: the type is lower case and the
// envelope keys come in another order than elsewhere.
const (
	restDelta100002 = `{"T":"obu.spot","t":"delta","dp":"increment","P":1760324595709048090,"d":{"C":100002,"M":1760324595706000,"O":100002,"a":[["115669","0.0151843"]],"b":[],"s":"BTC-USDT"}}`
	restDelta100003 = `{"T":"obu.spot","t":"delta","dp":"increment","P":1760324595709048090,"d":{"C":100003,"M":1760324595706000,"O":100003,"a":[],"b":[["115404","0"]],"s":"BTC-USDT"}}`
)

func restPush(seq int64, bids, asks []orderbook.Level) string {
	return fmt.Sprintf(`{"T":"obu.SPOT","t":"delta","dp":"increment","P":%d,"d":{"C":%d,"M":1,"O":%d,"a":[%s],"b":[%s],"s":"BTC-USDT"}}`,
		seq, seq, seq, levelsJSON(asks), levelsJSON(bids))
}

func TestOrderBookIncrement_FollowsKuCoinsDocumentedProcedure(t *testing.T) {
	h := newHarness(t)
	release := make(chan struct{})
	var calls atomic.Int32
	h.snapshots = func(ctx context.Context, tradeType, symbol string) (orderbook.Snapshot, error) {
		calls.Add(1)
		if tradeType != "SPOT" || symbol != "BTC-USDT" {
			t.Errorf("snapshot requested for %q on %q", symbol, tradeType)
		}
		select {
		case <-release:
			return restDoc(), nil
		case <-ctx.Done():
			return orderbook.Snapshot{}, ctx.Err()
		}
	}
	// 100000 and 100001 are contained in the snapshot (sequence 100001) and must be
	// discarded; 100002 and 100003 are the documented updates.
	h.fake.OnSubscribe(restKey,
		restPush(100000, []orderbook.Level{lv("115404", "0.9")}, nil),
		restPush(100001, []orderbook.Level{lv("115404", "0.5")}, nil),
		restDelta100002, restDelta100003)
	book, err := h.spot(t).SubscribeOrderBookIncrement(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	f := h.frames("subscribe", "obu")[0]
	if f["depth"] != "increment" || f["symbol"] != "BTC-USDT" || f["tradeType"] != "SPOT" {
		t.Fatalf("subscribe frame: %v", f)
	}
	if book.State() == orderbook.SyncSynced {
		t.Fatal("the book cannot be synchronised before the snapshot arrived")
	}
	select {
	case <-book.Ready():
		t.Fatal("not ready yet")
	default:
	}
	eventually(t, func() bool { return calls.Load() == 1 }, "the snapshot request")
	close(release)
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	// KuCoin's own "Final Order Book" of the example.
	eventually(t, func() bool { return book.Book().Sequence() == 100003 }, "the buffered updates to be replayed")
	snap := book.Book().Snapshot(0)
	if got := levels(snap.Asks); got != "115442:0.2 115553.5:0.05 115669:0.0151843 " {
		t.Fatalf("asks = %s", got)
	}
	if got := levels(snap.Bids); got != "115403.5:0.3 115388.9:0.1 " {
		t.Fatalf("bids = %s", got)
	}
	if calls.Load() != 1 || book.State() != orderbook.SyncSynced || book.Resyncs() != 0 {
		t.Fatalf("snapshot calls=%d state=%v resyncs=%d", calls.Load(), book.State(), book.Resyncs())
	}
	// Live updates after synchronisation arrive as Update events. Updates 100002 and
	// 100003 may have arrived after the snapshot instead of before it, in which case
	// their events come first.
	if err := h.fake.Push(restPush(100004, []orderbook.Level{lv("115403.5", "0.35")}, nil)); err != nil {
		t.Fatal(err)
	}
	for {
		ev := expectEvents(t, book, orderbook.EventUpdate)[0]
		if ev.Sequence < 100004 {
			continue
		}
		if ev.Sequence != 100004 || len(ev.Changes) != 1 || ev.Changes[0].Price != "115403.5" {
			t.Fatalf("update event: %+v", ev)
		}
		break
	}
}

func TestOrderBookIncrement_ASequenceGapTriggersARESTResync(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.snapshots = func(context.Context, string, string) (orderbook.Snapshot, error) {
		if calls.Add(1) == 1 {
			return restDoc(), nil
		}
		return orderbook.Snapshot{Symbol: "BTC-USDT", Sequence: 200000, Bids: []orderbook.Level{lv("500", "5")}, Asks: []orderbook.Level{lv("501", "5")}}, nil
	}
	book, err := h.spot(t).SubscribeOrderBookIncrement(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	if err := h.fake.Push(restDelta100002); err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventUpdate)
	if err := h.fake.Push(restPush(100010, []orderbook.Level{lv("115404", "1")}, nil)); err != nil { // 100003..100009 were lost
		t.Fatal(err)
	}
	evs := expectEvents(t, book, orderbook.EventStale, orderbook.EventSnapshot)
	var gap *orderbook.GapError
	if !errors.Is(evs[0].Err, orderbook.ErrSequenceGap) || !errors.As(evs[0].Err, &gap) || gap.Have != 100002 || gap.Start != 100010 {
		t.Fatalf("stale cause: %v", evs[0].Err)
	}
	if bid, _ := book.Book().BestBid(); bid.Price != "500" || book.Book().Sequence() != 200000 || book.Resyncs() != 1 || calls.Load() != 2 {
		t.Fatalf("bid=%v seq=%d resyncs=%d calls=%d", bid, book.Book().Sequence(), book.Resyncs(), calls.Load())
	}
	if h.fake.Count("subscribe", "obu") != 1 {
		t.Fatal("the REST-synchronised book resynchronises over REST, not by subscribing again")
	}
}

func TestOrderBookIncrement_ReconnectRebuildsFromANewSnapshot(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.snapshots = func(context.Context, string, string) (orderbook.Snapshot, error) {
		n := calls.Add(1)
		return orderbook.Snapshot{Symbol: "BTC-USDT", Sequence: int64(n) * 10, Bids: []orderbook.Level{lv("1", "1")}}, nil
	}
	s := h.spot(t)
	book, err := s.SubscribeOrderBookIncrementTuned(ctx5(t), "BTC-USDT", orderbook.Tuning{ResetGrace: 30 * time.Millisecond})
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
	expectEvents(t, book, orderbook.EventSnapshot) // the grace timer restarts the sync of a quiet symbol
	if book.Book().Sequence() != 20 || calls.Load() != 2 {
		t.Fatalf("sequence=%d snapshot calls=%d", book.Book().Sequence(), calls.Load())
	}
}

func TestOrderBookIncrement_GivesUpWhenSnapshotsKeepFailing(t *testing.T) {
	h := newHarness(t)
	boom := errors.New("snapshot endpoint down")
	h.snapshots = func(context.Context, string, string) (orderbook.Snapshot, error) { return orderbook.Snapshot{}, boom }
	book, err := h.spot(t).SubscribeOrderBookIncrementTuned(ctx5(t), "BTC-USDT", orderbook.Tuning{MaxAttempts: 2, RetryMin: time.Millisecond, RetryMax: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.fake.Push(restDelta100002); err != nil { // the first update starts the synchronisation
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
	for range book.C() {
	}
	eventually(t, func() bool { return h.fake.Count("unsubscribe", "obu") == 1 }, "the failed book unsubscribes from the stream")
}

func TestOrderBookIncrement_PermanentSnapshotErrorEndsImmediately(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.snapshots = func(context.Context, string, string) (orderbook.Snapshot, error) {
		calls.Add(1)
		return orderbook.Snapshot{}, stream.Permanent(fmt.Errorf("snapshot: %w", transport.ErrCredentialsRequired))
	}
	book, err := h.spot(t).SubscribeOrderBookIncrement(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventFailed)
	<-book.Done()
	if !errors.Is(book.Err(), transport.ErrCredentialsRequired) || !errors.Is(book.Err(), stream.ErrResyncFailed) || calls.Load() != 1 {
		t.Fatalf("Err=%v calls=%d", book.Err(), calls.Load())
	}
}

func TestOrderBookIncrement_WithoutASnapshotSourceIsRefused(t *testing.T) {
	wstest.CheckLeaks(t)
	fake := wstest.NewUTAFake(t)
	svc := NewService(Hosts{Spot: fake.URL(), Futures: fake.URL()}, nil, nil, fastOptions()...)
	s, err := svc.DialSpot(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.SubscribeOrderBookIncrement(ctx5(t), "BTC-USDT"); !errors.Is(err, ErrNoSnapshotSource) {
		t.Fatalf("error = %v", err)
	}
	if _, err := s.SubscribeOrderBookIncrementTuned(ctx5(t), "BTC-USDT", orderbook.Tuning{}); !errors.Is(err, ErrNoSnapshotSource) {
		t.Fatalf("error = %v", err)
	}
	if n := fake.Count("subscribe", ""); n != 0 {
		t.Fatalf("%d subscribe frames reached the server", n)
	}
	// The 10ms book needs no REST snapshot at all.
	fake.OnSubscribe("obu|BTC-USDT|increment@10ms")
	book, err := s.SubscribeOrderBook(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatalf("SubscribeOrderBook without a snapshot source: %v", err)
	}
	if err := book.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOrderBookIncrement_CloseEndsTheBook(t *testing.T) {
	h := newHarness(t)
	h.snapshots = func(context.Context, string, string) (orderbook.Snapshot, error) { return restDoc(), nil }
	book, err := h.spot(t).SubscribeOrderBookIncrement(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.fake.Push(restDelta100002); err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	if err := book.Close(); err != nil {
		t.Fatal(err)
	}
	for range book.C() {
	}
	if book.Err() != nil || h.fake.Count("unsubscribe", "obu") != 1 || book.State() != orderbook.SyncClosed {
		t.Fatalf("Err=%v unsubscribes=%d state=%v", book.Err(), h.fake.Count("unsubscribe", "obu"), book.State())
	}
}

func TestOrderBookIncrement_RejectedSubscriptionLeavesNothingBehind(t *testing.T) {
	h := newHarness(t)
	h.snapshots = func(context.Context, string, string) (orderbook.Snapshot, error) { return restDoc(), nil }
	h.fake.Reject(restKey, "depth increment is no longer offered")
	if _, err := h.spot(t).SubscribeOrderBookIncrement(ctx5(t), "BTC-USDT"); !errors.Is(err, uta.ErrSubscriptionFailed) {
		t.Fatalf("error = %v", err)
	}
}

// A snapshot the book cannot apply (the price is not a number) leaves it
// unsynchronised without any event; the book subscribes again for a usable one.
func TestOrderBook_UnusableSnapshotIsReplacedByANewOne(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, []orderbook.Level{lv("abc", "1")}, baseAsks))
	slow := orderbook.Tuning{RetryMin: 50 * time.Millisecond, RetryMax: 50 * time.Millisecond}
	s := h.futures(t)
	book, err := s.SubscribeOrderBookTuned(ctx5(t), bookSymbol, slow)
	if err != nil {
		t.Fatal(err)
	}
	// What the server pushes when asked again; the book waits at least 37ms first.
	h.fake.OnSubscribe(bookKey, snapshotPush(200, baseBids, baseAsks))
	eventually(t, func() bool { return book.State() == orderbook.SyncSynced && book.Book().Sequence() == 200 }, "the second snapshot to be applied")
	// (On a machine that stalls the test for longer than the book waits, the book may
	// have asked again before the replacement script was in place.)
	if n := h.fake.Count("subscribe", "obu"); n < 2 || book.Resyncs() != 0 || s.Stats().DecodeErrors != 0 {
		t.Fatalf("subscribes=%d resyncs=%d decode errors=%d", n, book.Resyncs(), s.Stats().DecodeErrors)
	}
	expectEvents(t, book, orderbook.EventSnapshot) // the only event: the unusable snapshot produced none
}

func TestOrderBookIncrement_UndecodableUpdateIsTreatedAsALostUpdate(t *testing.T) {
	h := newHarness(t)
	var calls atomic.Int32
	h.snapshots = func(context.Context, string, string) (orderbook.Snapshot, error) {
		n := calls.Add(1)
		return orderbook.Snapshot{Symbol: "BTC-USDT", Sequence: int64(n) * 100, Bids: []orderbook.Level{lv("1", "1")}}, nil
	}
	s := h.spot(t)
	book, err := s.SubscribeOrderBookIncrement(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	if err := h.fake.Push(`{"T":"obu.SPOT","dp":"increment","t":"delta","P":1,"d":{"s":"BTC-USDT","O":101,"C":101,"b":[["1"]],"a":[]}}`); err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventStale, orderbook.EventSnapshot)
	if s.Stats().DecodeErrors != 1 || calls.Load() != 2 {
		t.Fatalf("decode errors=%d snapshot calls=%d", s.Stats().DecodeErrors, calls.Load())
	}
}

// A snapshot pushed on the increment feed, which KuCoin does not document, replaces
// the book like any other.
func TestOrderBookIncrement_ASnapshotPushReplacesTheBook(t *testing.T) {
	h := newHarness(t)
	h.snapshots = func(context.Context, string, string) (orderbook.Snapshot, error) { return restDoc(), nil }
	book, err := h.spot(t).SubscribeOrderBookIncrement(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	expectEvents(t, book, orderbook.EventSnapshot)
	if err := h.fake.Push(`{"T":"obu.SPOT","dp":"increment","t":"snapshot","P":1,"d":{"s":"BTC-USDT","O":500000,"C":500000,"b":[["1","1"]],"a":[["2","2"]]}}`); err != nil {
		t.Fatal(err)
	}
	expectEvents(t, book, orderbook.EventSnapshot)
	if bid, _ := book.Book().BestBid(); bid.Price != "1" || book.Book().Sequence() != 500000 || book.State() != orderbook.SyncSynced {
		t.Fatalf("bid=%v seq=%d state=%v", bid, book.Book().Sequence(), book.State())
	}
}

// A managed book and a plain subscription of the same feed would receive the same
// pushes, which one connection cannot tell apart; the second one is refused.
func TestOrderBook_ExcludesAPlainSubscriptionOfTheSameFeed(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe(bookKey, snapshotPush(100, baseBids, baseAsks))
	h.fake.OnSubscribe("obu|XBTUSDTM|5")
	h.fake.OnSubscribe("obu|ETHUSDTM|increment@10ms")
	s := h.futures(t)
	book, err := s.SubscribeOrderBook(ctx5(t), bookSymbol)
	if err != nil {
		t.Fatal(err)
	}
	defer book.Close()
	if _, err := s.SubscribeOrderBookUpdates(ctx5(t), []string{bookSymbol}, DepthIncrement10ms); !errors.Is(err, stream.ErrAlreadySubscribed) {
		t.Fatalf("a plain subscription of the book's feed: %v", err)
	}
	if _, err := s.SubscribeOrderBook(ctx5(t), bookSymbol); !errors.Is(err, stream.ErrAlreadySubscribed) {
		t.Fatalf("a second book of the same feed: %v", err)
	}
	// Another depth and another symbol are different feeds.
	if _, err := s.SubscribeOrderBookUpdates(ctx5(t), []string{bookSymbol}, Depth5); err != nil {
		t.Fatalf("another depth: %v", err)
	}
	other, err := s.SubscribeOrderBook(ctx5(t), "ETHUSDTM")
	if err != nil {
		t.Fatalf("another symbol: %v", err)
	}
	other.Close()
}
