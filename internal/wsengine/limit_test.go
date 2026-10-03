package wsengine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/stream"
)

func TestSendWindow_IsOffWithoutALimit(t *testing.T) {
	if newSendWindow(0, time.Second) != nil || newSendWindow(10, 0) != nil || newSendWindow(-1, time.Second) != nil {
		t.Fatal("no limit must mean no window")
	}
	var w *sendWindow
	if err := w.reserve(context.Background(), nil, nil); err != nil {
		t.Fatalf("a nil window must let everything through: %v", err)
	}
	w.note() // must not panic
}

func TestSendWindow_KeepsATenthInReserve(t *testing.T) {
	for _, tt := range []struct{ limit, want int }{{100, 90}, {300, 270}, {10, 9}, {1, 1}, {5, 5}} {
		if got := newSendWindow(tt.limit, time.Second).max; got != tt.want {
			t.Errorf("limit %d: budget %d, want %d", tt.limit, got, tt.want)
		}
	}
}

func TestSendWindow_PacesToTheBudget(t *testing.T) {
	w := newSendWindow(10, 300*time.Millisecond) // a budget of 9
	ctx := context.Background()
	start := time.Now()
	for i := 0; i < 9; i++ {
		if err := w.reserve(ctx, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("what fits the budget must not wait, took %v", elapsed)
	}
	if err := w.reserve(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 290*time.Millisecond {
		t.Fatalf("the tenth message went out after %v, before the first had left the window", elapsed)
	}
}

func TestSendWindow_NeverExceedsTheBudgetInAnyWindow(t *testing.T) {
	const window = 100 * time.Millisecond
	w := newSendWindow(10, window) // a budget of 9
	var booked []time.Time
	for i := 0; i < 45; i++ {
		at, err := w.book(context.Background(), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		booked = append(booked, at)
	}
	for i := range booked {
		n := 0
		for j := i; j < len(booked) && booked[j].Sub(booked[i]) < window; j++ {
			n++
		}
		if n > 9 {
			t.Fatalf("%d messages booked within %v of message %d", n, window, i)
		}
	}
}

func TestSendWindow_ContextAndSessionEndStopTheWait(t *testing.T) {
	w := newSendWindow(1, time.Hour)
	if err := w.reserve(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := w.reserve(ctx, nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reserve with an expiring context = %v", err)
	}
	done := make(chan struct{})
	lost := errors.New("connection lost")
	go func() {
		time.Sleep(30 * time.Millisecond)
		close(done)
	}()
	if err := w.reserve(context.Background(), done, func() error { return lost }); !errors.Is(err, lost) {
		t.Fatalf("reserve on a session that died = %v", err)
	}
}

func TestSendWindow_HeartbeatsCountButAreNeverDelayed(t *testing.T) {
	w := newSendWindow(10, 200*time.Millisecond) // a budget of 9
	start := time.Now()
	for i := 0; i < 20; i++ {
		w.note()
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("note must never wait, took %v", elapsed)
	}
	if err := w.reserve(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("a request went out after %v although the window was full of heartbeats", elapsed)
	}
}

// measuredWindow is the interval the integration tests count server-side arrivals
// in: half of the 300ms window. The guarantee is about when the client books its
// messages (the unit tests check exactly that); on a loaded machine a goroutine that
// booked its slot can be scheduled tens of milliseconds late, so frames of one batch
// reach the server spread out. Batches are 300ms apart, hence half a window holds at
// most one of them (9 frames), whereas an unpaced restore, a frame every 10ms,
// puts 15 into it.
const measuredWindow = 150 * time.Millisecond

func limited(e *env, limit int, window time.Duration) {
	e.proto.mu.Lock()
	e.proto.limit, e.proto.window = limit, window
	e.proto.mu.Unlock()
}

func TestConn_SubscribesAreKeptWithinTheServersMessageLimit(t *testing.T) {
	e := newEnv(t)
	limited(e, 10, 300*time.Millisecond)
	e.connect()
	const n = 30
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := e.conn.Subscribe(ctx, specFor(fmt.Sprintf("t%d", i), newRec()))
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	}
	if got := e.beh.count(1, "subscribe", ""); got != n {
		t.Fatalf("server saw %d subscribe frames, want %d", got, n)
	}
	if got := e.beh.busiest(1, "subscribe", measuredWindow); got > 10 {
		t.Fatalf("%d subscribe frames within %v; the server tolerates 10 per 300ms (arrivals %v)", got, measuredWindow, e.beh.arrivals(1, "subscribe"))
	}
	// The pacing is real: 30 requests at 9 per 300ms cannot all fit in less than three windows.
	if span := e.beh.arrivals(1, "subscribe"); span[len(span)-1] < 800*time.Millisecond {
		t.Fatalf("all %d requests arrived within %v: they were not paced", n, span[len(span)-1])
	}
}

func TestConn_ConfiguredMessageLimitReplacesTheProtocolsAndCanBeSwitchedOff(t *testing.T) {
	for _, tt := range []struct {
		name         string
		mutate       func(*stream.Config)
		wantBursts   bool // all requests arrive at once
		wantPacedFor time.Duration
	}{
		{"replaced", func(c *stream.Config) {
			c.MessageLimit = stream.MessageLimit{Messages: 5, Window: 200 * time.Millisecond}
		}, false, 350 * time.Millisecond},
		{"off", func(c *stream.Config) { c.MessageLimit = stream.MessageLimit{Unlimited: true} }, true, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, tt.mutate)
			limited(e, 2, time.Hour) // the protocol's own limit would stall almost at once
			e.connect()
			const n = 12
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if _, err := e.conn.Subscribe(ctx, specFor(fmt.Sprintf("t%d", i), newRec())); err != nil {
						t.Errorf("Subscribe: %v", err)
					}
				}(i)
			}
			wg.Wait()
			arrivals := e.beh.arrivals(1, "subscribe")
			if len(arrivals) != n {
				t.Fatalf("server saw %d subscribe frames, want %d", len(arrivals), n)
			}
			if last := arrivals[len(arrivals)-1]; tt.wantBursts && last > 150*time.Millisecond {
				t.Fatalf("with pacing off the requests took %v", last)
			} else if !tt.wantBursts && last < tt.wantPacedFor {
				t.Fatalf("12 requests at 5 per 200ms arrived within %v, want at least %v", last, tt.wantPacedFor)
			}
		})
	}
}

func TestConn_RestoreStaysWithinTheServersMessageLimit(t *testing.T) {
	e := newEnv(t)
	limited(e, 10, 300*time.Millisecond)
	e.connect()
	const n = 20
	for i := 0; i < n; i++ {
		e.subscribe(fmt.Sprintf("t%d", i), newRec())
	}
	e.srv.DropAll()
	e.waitReconnected(1)
	if got := e.beh.count(2, "subscribe", ""); got != n {
		t.Fatalf("connection 2 saw %d subscribe frames, want %d", got, n)
	}
	if got := e.beh.busiest(2, "subscribe", measuredWindow); got > 10 {
		t.Fatalf("%d resubscribe frames within %v; the server tolerates 10 per 300ms (arrivals %v)", got, measuredWindow, e.beh.arrivals(2, "subscribe"))
	}
	if st := e.conn.Stats(); st.Subscriptions != n || st.State != stream.StateConnected {
		t.Fatalf("stats: %+v", st)
	}
}

func TestConn_SubscribeWaitingForTheMessageBudgetHonoursItsContext(t *testing.T) {
	e := newEnv(t)
	limited(e, 2, time.Hour) // a budget of 2 requests for the life of the test
	e.connect()
	e.subscribe("a", newRec())
	e.subscribe("b", newRec())

	h := newRec()
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err := e.conn.Subscribe(ctx, specFor("c", h))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Subscribe past the budget = %v, want the context's deadline", err)
	}
	h.expect(t, "abort:", "closed")
	if got := e.beh.count(1, "subscribe", "c"); got != 0 {
		t.Fatalf("the refused request must not have been sent, server saw %d", got)
	}
	if st := e.conn.Stats(); st.Subscriptions != 2 {
		t.Fatalf("the refused subscription must leave the registry: %+v", st)
	}
	if got := e.beh.count(1, "unsubscribe", "c"); got != 0 {
		t.Fatalf("nothing was subscribed, so nothing must be unsubscribed (%d)", got)
	}
}
