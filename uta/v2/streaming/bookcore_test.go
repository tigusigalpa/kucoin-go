package streaming

import (
	"errors"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/websocket/uta"
)

var unitSpec = uta.SubscribeSpec{Channel: "obu", TradeType: "FUTURES", Symbols: []string{"X"}, Depth: "increment@10ms"}

// The engine reports the end of a subscription, also one that failed while the
// subscription call was still running, to the handler. The handler holds such a
// report back until the call returned and then acts on it.
func TestBookHandler_AnEndDuringTheSubscriptionCallIsReplayedWhenItReturns(t *testing.T) {
	core := newBookCore(nil, unitSpec, stream.SubscribeConfig{}, orderbook.SyncerConfig{Symbol: "X"}, true)
	h := newBookHandler(core)
	boom := errors.New("connection lost")
	h.OnAbort(boom)
	h.OnClosed()
	if core.isEnded() {
		t.Fatal("while the subscription call has not returned the end is only recorded")
	}
	h.arm()
	if !core.isEnded() || !errors.Is(core.sub.Err(), boom) {
		t.Fatalf("ended=%v err=%v", core.isEnded(), core.sub.Err())
	}
	drain(t, core.sub) // the replayed OnClosed released the book, which sealed the subscription
	if core.syncer.State() != orderbook.SyncClosed {
		t.Fatalf("syncer state = %v", core.syncer.State())
	}
}

func TestBookHandler_ARetiredHandlerIgnoresWhatItRecorded(t *testing.T) {
	core := newBookCore(nil, unitSpec, stream.SubscribeConfig{}, orderbook.SyncerConfig{Symbol: "X"}, true)
	h := newBookHandler(core)
	h.OnAbort(errors.New("the call failed"))
	h.OnClosed()
	h.retired.Store(true) // what a failed subscription call does
	h.arm()
	if core.isEnded() {
		t.Fatal("a failed attempt must not end the book")
	}
	core.abandon()
	drain(t, core.sub)
}

func TestBookHandler_WithNothingRecordedArmingChangesNothing(t *testing.T) {
	core := newBookCore(nil, unitSpec, stream.SubscribeConfig{}, orderbook.SyncerConfig{Symbol: "X"}, true)
	h := newBookHandler(core)
	h.arm()
	if core.isEnded() {
		t.Fatal("nothing ended")
	}
	// Once armed, an end is acted upon at once.
	h.OnAbort(nil)
	if !core.isEnded() || core.sub.Err() != nil {
		t.Fatalf("ended=%v err=%v", core.isEnded(), core.sub.Err())
	}
	h.OnClosed()
	drain(t, core.sub)
}

func TestBookCore_BackoffDoublesUpToTheCap(t *testing.T) {
	c := &bookCore{retryMin: 10 * time.Millisecond, retryMax: 80 * time.Millisecond}
	for failures, limit := range map[int]time.Duration{0: 10 * time.Millisecond, 1: 20 * time.Millisecond, 2: 40 * time.Millisecond, 3: 80 * time.Millisecond, 4: 80 * time.Millisecond, 20: 80 * time.Millisecond} {
		for i := 0; i < 50; i++ { // the jitter shortens the pause by at most a quarter
			if d := c.backoff(failures); d > limit || d < limit*3/4 {
				t.Fatalf("backoff(%d) = %v, want within [%v, %v]", failures, d, limit*3/4, limit)
			}
		}
	}
}

func TestBookCore_AWatchdogIsNotArmedAfterTheBookEnded(t *testing.T) {
	core := newBookCore(nil, unitSpec, stream.SubscribeConfig{}, orderbook.SyncerConfig{Symbol: "X"}, false)
	core.abandon()
	core.armWatchdog()
	core.mu.Lock()
	armed := core.watchdog != nil
	core.mu.Unlock()
	if armed {
		t.Fatal("a finished book must not start timers")
	}
	drain(t, core.sub)
}

// A worker that finds the connection closed ends the book without an error: the
// session was closed on purpose.
func TestBookCore_ResubscribingOnAClosedConnectionEndsTheBook(t *testing.T) {
	wstest.CheckLeaks(t)
	fake := wstest.NewUTAFake(t)
	client := uta.NewClient(fake.URL(), "", uta.WithStreamOptions(fastOptions()...))
	if err := client.Connect(ctx5(t)); err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	core := newBookCore(client, unitSpec, stream.SubscribeConfig{}, orderbook.SyncerConfig{Symbol: "X", RetryMin: time.Millisecond, RetryMax: time.Millisecond}, false)
	close(core.attached) // the worker waits for the first subscription, which this test does not make
	core.requestResubscribe(true)
	select {
	case <-core.sub.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the book did not end")
	}
	drain(t, core.sub)
	if core.sub.Err() != nil {
		t.Fatalf("Err = %v", core.sub.Err())
	}
}

// A worker that cannot subscribe at all gives up after MaxAttempts with a typed error.
func TestBookCore_ResubscribingOnAConnectionThatWasNeverOpenedFails(t *testing.T) {
	wstest.CheckLeaks(t)
	fake := wstest.NewUTAFake(t)
	client := uta.NewClient(fake.URL(), "", uta.WithStreamOptions(fastOptions()...)) // never connected
	core := newBookCore(client, unitSpec, stream.SubscribeConfig{}, orderbook.SyncerConfig{Symbol: "X", RetryMin: time.Millisecond, RetryMax: time.Millisecond, MaxAttempts: 2}, false)
	close(core.attached)
	core.requestResubscribe(true)
	select {
	case <-core.sub.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the book did not give up")
	}
	drain(t, core.sub)
	if !errors.Is(core.sub.Err(), stream.ErrResyncFailed) || !errors.Is(core.sub.Err(), stream.ErrNotConnected) {
		t.Fatalf("Err = %v", core.sub.Err())
	}
}
