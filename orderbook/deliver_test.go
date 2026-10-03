package orderbook

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/stream"
)

func fastGrace(t *testing.T) {
	t.Helper()
	old := finalEventGrace
	finalEventGrace = 30 * time.Millisecond
	t.Cleanup(func() { finalEventGrace = old })
}

func TestDeliverTo_EventsReachTheSubscriptionInOrder(t *testing.T) {
	wstest.CheckLeaks(t)
	bids, asks := baseLevels()
	rest := newSnapshots(snap(10, bids, asks))
	sub := stream.NewSubscription[Event]("book", 0, nil)
	cfg := SyncerConfig{Symbol: "XBTUSDTM", Snapshot: rest.fetch, RetryMin: time.Millisecond}
	cfg.DeliverTo(sub, nil)
	s := NewSyncer(cfg)
	t.Cleanup(func() { s.Close(); sub.Seal() })
	s.Start()
	<-s.Ready() // updates that arrive during the initial sync are replayed silently, not reported
	for seq := int64(11); seq <= 13; seq++ {
		s.HandleDelta(delta(seq, chg(Bid, "100", fmt.Sprint(seq))))
	}
	want := []EventType{EventSnapshot, EventUpdate, EventUpdate, EventUpdate}
	for i, w := range want {
		select {
		case ev := <-sub.C():
			if ev.Type != w {
				t.Fatalf("event %d = %v, want %v", i, ev.Type, w)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("event %d never arrived", i)
		}
	}
}

func TestDeliverTo_FailureEndsTheSubscriptionOfAnAbsentConsumer(t *testing.T) {
	wstest.CheckLeaks(t)
	fastGrace(t)
	boom := errors.New("snapshot endpoint down")
	rest := newSnapshots(fail(stream.Permanent(boom)))
	sub := stream.NewSubscription[Event]("book", 0, nil)
	var ended atomic.Int32
	cfg := SyncerConfig{Symbol: "XBTUSDTM", Snapshot: rest.fetch}
	cfg.DeliverTo(sub, func() { ended.Add(1) })
	s := NewSyncer(cfg)
	t.Cleanup(func() { s.Close(); sub.Seal() })

	// Nobody reads sub.C(); the final event cannot be delivered, but the
	// subscription must still end, with the cause.
	s.Start()
	select {
	case <-sub.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the subscription of an absent consumer never ended")
	}
	if !errors.Is(sub.Err(), stream.ErrResyncFailed) || !errors.Is(sub.Err(), boom) {
		t.Fatalf("Err = %v", sub.Err())
	}
	deadline := time.Now().Add(5 * time.Second)
	for ended.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ended.Load() == 0 {
		t.Fatal("the end callback was not called")
	}
}

func TestDeliverTo_AnAttentiveConsumerReceivesTheFinalEvent(t *testing.T) {
	wstest.CheckLeaks(t)
	fastGrace(t)
	boom := errors.New("snapshot endpoint down")
	rest := newSnapshots(fail(stream.Permanent(boom)))
	sub := stream.NewSubscription[Event]("book", 0, nil)
	cfg := SyncerConfig{Symbol: "XBTUSDTM", Snapshot: rest.fetch}
	cfg.DeliverTo(sub, nil)
	s := NewSyncer(cfg)
	t.Cleanup(func() { s.Close(); sub.Seal() })
	s.Start()
	select {
	case ev := <-sub.C():
		if ev.Type != EventFailed || !errors.Is(ev.Err, boom) {
			t.Fatalf("event: %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the final event was not delivered to a consumer that was waiting for it")
	}
	select {
	case <-sub.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the subscription did not end after its final event")
	}
	if !errors.Is(sub.Err(), boom) {
		t.Fatalf("Err = %v", sub.Err())
	}
}

func TestSyncer_OnFailRunsOnceAndNeverBehindTheQueue(t *testing.T) {
	wstest.CheckLeaks(t)
	c := newGatedConsumer() // OnEvent is stuck on its first event for the whole test
	failed := make(chan error, 4)
	rest := newSnapshots(fail(stream.Permanent(errors.New("down"))))
	s := NewSyncer(SyncerConfig{
		Symbol: "XBTUSDTM", Snapshot: rest.fetch, OnEvent: c.onEvent,
		OnFail: func(err error) { failed <- err },
	})
	t.Cleanup(s.Close)
	t.Cleanup(c.release)

	bids, asks := baseLevels()
	s.HandleSnapshot(Snapshot{Sequence: 1, Bids: bids, Asks: asks}) // the first event blocks the consumer
	<-c.entered
	s.HandleReset()
	s.HandleDelta(delta(2, chg(Bid, "100", "1"))) // starts the (failing) fetch
	select {
	case err := <-failed:
		if !errors.Is(err, stream.ErrResyncFailed) {
			t.Fatalf("cause = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnFail waited behind an event nobody is reading")
	}
	s.HandleDelta(delta(3))
	s.HandleReset()
	select {
	case err := <-failed:
		t.Fatalf("OnFail ran twice: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
}
