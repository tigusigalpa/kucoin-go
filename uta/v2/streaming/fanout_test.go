package streaming

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/websocket/uta"
)

// member builds a subscription the way a producer sees it; closed counts Close
// calls and closeErr is what the member's unsubscribe reports.
func member(name string, closed *atomic.Int32, closeErr error) *stream.Subscription[int] {
	return stream.NewSubscription[int](name, 0, func() error {
		closed.Add(1)
		return closeErr
	})
}

func drain[T any](t *testing.T, sub *stream.Subscription[T]) []T {
	t.Helper()
	var out []T
	timeout := time.After(5 * time.Second)
	for {
		select {
		case v, ok := <-sub.C():
			if !ok {
				return out
			}
			out = append(out, v)
		case <-timeout:
			t.Fatal("the channel never closed")
		}
	}
}

func TestMerge_DeliversEveryMembersUpdatesAndClosesAfterTheLast(t *testing.T) {
	var closed atomic.Int32
	a, b := member("a", &closed, nil), member("b", &closed, nil)
	merged := merge("a+b", []*stream.Subscription[int]{a, b})
	go func() {
		a.Deliver(1)
		a.Deliver(2)
		a.Seal()
	}()
	go func() {
		b.Deliver(10)
		b.Seal()
	}()
	got := drain(t, merged)
	sort.Ints(got)
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 10 {
		t.Fatalf("got %v", got)
	}
	if merged.Err() != nil || merged.Key() != "a+b" {
		t.Fatalf("Err=%v Key=%q", merged.Err(), merged.Key())
	}
}

func TestMerge_AMemberThatEndsWithAnErrorEndsTheWholeSubscription(t *testing.T) {
	boom := errors.New("boom")
	var closed atomic.Int32
	a, b := member("a", &closed, nil), member("b", &closed, nil)
	merged := merge("a+b", []*stream.Subscription[int]{a, b})
	a.Finish(boom)
	a.Seal()
	select {
	case <-merged.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the merged subscription did not end")
	}
	if !errors.Is(merged.Err(), boom) {
		t.Fatalf("Err = %v", merged.Err())
	}
	// The other member is closed too; its producer seals it once it is done.
	select {
	case <-b.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the healthy member was not closed")
	}
	b.Seal()
	drain(t, merged)
	if closed.Load() == 0 {
		t.Fatal("the members' close functions were never called")
	}
}

func TestMerge_ClosingTheMergedSubscriptionClosesTheMembersAndReportsTheirError(t *testing.T) {
	failed := errors.New("unsubscribe failed")
	var closed atomic.Int32
	a, b := member("a", &closed, nil), member("b", &closed, failed)
	merged := merge("a+b", []*stream.Subscription[int]{a, b})
	if err := merged.Close(); !errors.Is(err, failed) {
		t.Fatalf("Close returned %v", err)
	}
	if closed.Load() != 2 {
		t.Fatalf("%d members closed", closed.Load())
	}
	a.Seal()
	b.Seal()
	drain(t, merged)
	if merged.Err() != nil {
		t.Fatalf("a requested Close is not an error: %v", merged.Err())
	}
}

func TestMerge_AnUpdateForAnEndedSubscriptionReleasesItsMember(t *testing.T) {
	var closed atomic.Int32
	a := member("a", &closed, nil)
	merged := merge("a", []*stream.Subscription[int]{a})
	merged.Finish(nil) // the merged subscription ended before the member's update arrived
	go a.Deliver(7)
	deadline := time.Now().Add(5 * time.Second)
	for closed.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the member was not closed")
		}
		time.Sleep(time.Millisecond)
	}
	a.Seal()
	drain(t, merged)
}

func TestMerge_DroppedIsTheSumOfTheMembers(t *testing.T) {
	var closed atomic.Int32
	a, b := member("a", &closed, nil), member("b", &closed, nil)
	merged := merge("a+b", []*stream.Subscription[int]{a, b})
	a.AddDropped(3)
	b.AddDropped(4)
	if merged.Dropped() != 7 {
		t.Fatalf("dropped = %d", merged.Dropped())
	}
	a.Seal()
	b.Seal()
	drain(t, merged)
}

func tradeOf(symbol string) string { return strings.Replace(fxTradeFutures, "XBTUSDTM", symbol, 1) }

func TestFanOut_ManySymbolsAreAllSubscribedAndDelivered(t *testing.T) {
	h := newHarness(t)
	const n = 30
	symbols := make([]string, n)
	for i := range symbols {
		symbols[i] = fmt.Sprintf("SYM%02dUSDTM", i)
		h.fake.OnSubscribe("trade|"+symbols[i], tradeOf(symbols[i]))
	}
	s := h.futures(t)
	sub, err := s.SubscribeTrades(ctx5(t), symbols)
	if err != nil {
		t.Fatal(err)
	}
	frames := h.frames("subscribe", "trade")
	if len(frames) != n {
		t.Fatalf("%d subscribe frames, want one per symbol", len(frames))
	}
	seen := map[string]bool{}
	for _, f := range frames {
		sym, _ := f["symbol"].(string)
		seen[sym] = true
		if f["symbols"] != nil || f["tradeType"] != "FUTURES" {
			t.Fatalf("frame: %v", f)
		}
	}
	got := map[string]bool{}
	for i := 0; i < n; i++ {
		got[next(t, sub).Symbol] = true
	}
	for _, sym := range symbols {
		if !seen[sym] || !got[sym] {
			t.Fatalf("symbol %s: subscribed=%v delivered=%v", sym, seen[sym], got[sym])
		}
	}
	if s.Stats().Subscriptions != n {
		t.Fatalf("subscriptions = %d", s.Stats().Subscriptions)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if got := h.fake.Count("unsubscribe", "trade"); got != n {
		t.Fatalf("%d unsubscribe frames, want one per symbol", got)
	}
	drain(t, sub)
	if sub.Err() != nil || s.Stats().Subscriptions != 0 {
		t.Fatalf("Err=%v subscriptions=%d", sub.Err(), s.Stats().Subscriptions)
	}
}

func TestFanOut_ARejectedSymbolRollsEverythingBack(t *testing.T) {
	h := newHarness(t)
	h.fake.Reject("trade|BADUSDTM", "invalid request data")
	s := h.futures(t)
	_, err := s.SubscribeTrades(ctx5(t), []string{"AUSDTM", "BUSDTM", "BADUSDTM", "CUSDTM", "DUSDTM"})
	var se *stream.ServerError
	if !errors.Is(err, uta.ErrSubscriptionFailed) || !errors.As(err, &se) || !strings.Contains(err.Error(), "symbol BADUSDTM") {
		t.Fatalf("error = %v", err)
	}
	// Whatever was subscribed before the failure is unsubscribed again; the rejected
	// request needs no unsubscribe.
	eventually(t, func() bool {
		return h.fake.Count("unsubscribe", "trade") == h.fake.Count("subscribe", "trade")-1 && s.Stats().Subscriptions == 0
	}, "every accepted subscription to be rolled back")
}

func TestFanOut_ACanceledContextRollsBack(t *testing.T) {
	h := newHarness(t)
	s := h.futures(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.SubscribeTrades(ctx, []string{"AUSDTM", "BUSDTM", "CUSDTM"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	eventually(t, func() bool { return s.Stats().Subscriptions == 0 }, "no subscription is left behind")
}

// A member that fails (here: its consumer cannot keep up and asked to lose the
// subscription instead of updates) ends the whole merged subscription, even though
// nobody reads it.
func TestFanOut_AFailingMemberEndsTheMergedSubscription(t *testing.T) {
	h := newHarness(t)
	frames := make([]string, 30)
	for i := range frames {
		frames[i] = tradeOf("AUSDTM")
	}
	h.fake.OnSubscribe("trade|AUSDTM", frames...)
	s := h.futures(t)
	merged, err := s.SubscribeTrades(ctx5(t), []string{"AUSDTM", "BUSDTM", "CUSDTM"}, stream.WithBuffer(2), stream.WithOverflow(stream.FailSubscription))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-merged.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the merged subscription did not end")
	}
	if !errors.Is(merged.Err(), stream.ErrSlowConsumer) {
		t.Fatalf("Err = %v", merged.Err())
	}
	// The healthy members are unsubscribed.
	eventually(t, func() bool { return h.fake.Count("unsubscribe", "trade") >= 2 }, "the healthy members to be unsubscribed")
	drain(t, merged)
}
