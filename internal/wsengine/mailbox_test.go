package wsengine

import (
	"fmt"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/stream"
)

func frameN(n int) stream.Frame { return stream.Frame{Route: fmt.Sprintf("r%d", n)} }

func drain(m *mailbox) []string {
	var out []string
	for {
		it, ok := m.pop()
		if !ok {
			return out
		}
		switch it.kind {
		case itemFrame:
			out = append(out, "f:"+it.frame.Route)
		case itemReset:
			out = append(out, fmt.Sprintf("reset:%d", it.n))
		case itemGap:
			out = append(out, fmt.Sprintf("gap:%d", it.n))
		}
	}
}

func TestMailbox_FIFOAndEmpty(t *testing.T) {
	m := newMailbox(10, stream.DropOldest)
	if _, ok := m.pop(); ok {
		t.Fatal("empty mailbox must not pop")
	}
	for i := 1; i <= 4; i++ {
		if res := m.pushFrame(frameN(i)); res != pushOK {
			t.Fatalf("push %d = %v", i, res)
		}
	}
	if got := fmt.Sprint(drain(m)); got != "[f:r1 f:r2 f:r3 f:r4]" {
		t.Fatalf("got %s", got)
	}
}

func TestMailbox_DropOldestKeepsNewestAndReportsGapInOrder(t *testing.T) {
	m := newMailbox(3, stream.DropOldest)
	var results []pushResult
	for i := 1; i <= 7; i++ {
		results = append(results, m.pushFrame(frameN(i)))
	}
	if results[2] != pushOK || results[3] != pushDroppedOldest {
		t.Fatalf("unexpected results %v", results)
	}
	if got := fmt.Sprint(drain(m)); got != "[gap:4 f:r5 f:r6 f:r7]" {
		t.Fatalf("got %s", got)
	}
	// After the marker was delivered a new burst starts clean.
	m.pushFrame(frameN(8))
	if got := fmt.Sprint(drain(m)); got != "[f:r8]" {
		t.Fatalf("got %s", got)
	}
}

func TestMailbox_DropNewestKeepsBacklog(t *testing.T) {
	m := newMailbox(2, stream.DropNewest)
	var dropped int
	for i := 1; i <= 5; i++ {
		if m.pushFrame(frameN(i)) == pushDroppedNewest {
			dropped++
		}
	}
	if dropped != 3 {
		t.Fatalf("dropped %d, want 3", dropped)
	}
	if got := fmt.Sprint(drain(m)); got != "[f:r1 f:r2 gap:3]" {
		t.Fatalf("got %s", got) // the lost frames are the newest: the gap is behind the backlog
	}
}

func TestMailbox_DropNewestReportsTheLossEvenWhenNothingFollows(t *testing.T) {
	m := newMailbox(1, stream.DropNewest)
	m.pushFrame(frameN(1))
	m.pushFrame(frameN(2))
	m.pushFrame(frameN(3))
	if got := fmt.Sprint(drain(m)); got != "[f:r1 gap:2]" {
		t.Fatalf("got %s", got)
	}
	m.pushFrame(frameN(4)) // once reported, a new burst starts clean
	if got := fmt.Sprint(drain(m)); got != "[f:r4]" {
		t.Fatalf("got %s", got)
	}
}

func TestMailbox_DropNewestSeparatesLossesByTheFramesBetweenThem(t *testing.T) {
	m := newMailbox(1, stream.DropNewest)
	m.pushFrame(frameN(1))
	m.pushFrame(frameN(2)) // lost
	if it, ok := m.pop(); !ok || it.kind != itemFrame {
		t.Fatalf("pop = %+v, %v", it, ok)
	}
	m.pushFrame(frameN(3)) // fits again
	m.pushFrame(frameN(4)) // lost
	if got := fmt.Sprint(drain(m)); got != "[gap:1 f:r3 gap:1]" {
		t.Fatalf("got %s", got)
	}
}

func TestMailbox_FailPolicyReportsOverflowWithoutQueueing(t *testing.T) {
	m := newMailbox(1, stream.FailSubscription)
	if m.pushFrame(frameN(1)) != pushOK {
		t.Fatal("first push must succeed")
	}
	if m.pushFrame(frameN(2)) != pushOverflowFail {
		t.Fatal("overflow must be reported")
	}
	if got := fmt.Sprint(drain(m)); got != "[f:r1]" {
		t.Fatalf("got %s", got)
	}
}

func TestMailbox_ResetMarkersAreOrderedAndNeverDropped(t *testing.T) {
	m := newMailbox(2, stream.DropOldest)
	m.pushFrame(frameN(1))
	m.pushReset(7)
	m.pushFrame(frameN(2))
	m.pushFrame(frameN(3)) // overflows: r1 is the oldest frame and is dropped, the marker survives
	m.pushReset(8)
	got := fmt.Sprint(drain(m))
	if got != "[gap:1 reset:7 f:r2 f:r3 reset:8]" {
		t.Fatalf("got %s", got)
	}
}

func TestMailbox_GapSitsWhereTheDroppedFrameWas(t *testing.T) {
	m := newMailbox(2, stream.DropOldest)
	m.pushReset(5)
	m.pushFrame(frameN(1))
	m.pushFrame(frameN(2))
	m.pushFrame(frameN(3)) // r1 came after the reset, so the loss is reported behind it
	if got := fmt.Sprint(drain(m)); got != "[reset:5 gap:1 f:r2 f:r3]" {
		t.Fatalf("got %s", got)
	}
}

func TestMailbox_RunsOfDropsAreOneMarkerEvenBehindResets(t *testing.T) {
	m := newMailbox(2, stream.DropOldest)
	m.pushReset(5)
	for i := 1; i <= 6; i++ {
		m.pushFrame(frameN(i))
	}
	if got := fmt.Sprint(drain(m)); got != "[reset:5 gap:4 f:r5 f:r6]" {
		t.Fatalf("got %s", got)
	}
}

func TestMailbox_EveryMarkerStaysInPlaceWhileFramesAreDropped(t *testing.T) {
	m := newMailbox(2, stream.DropOldest)
	m.pushReset(1)
	m.pushFrame(frameN(1))
	m.pushReset(2)
	m.pushFrame(frameN(2))
	m.pushFrame(frameN(3)) // r1 goes: [reset:1 gap:1 reset:2 r2 r3]
	m.pushFrame(frameN(4)) // r2 goes, between two different markers
	if got := fmt.Sprint(drain(m)); got != "[reset:1 gap:1 reset:2 gap:1 f:r3 f:r4]" {
		t.Fatalf("got %s", got)
	}
}

func TestMailbox_ResetsWithNothingBetweenThemMerge(t *testing.T) {
	m := newMailbox(4, stream.DropOldest)
	m.pushReset(3)
	m.pushReset(4)
	m.pushFrame(frameN(1))
	m.pushReset(5)
	m.pushReset(6)
	if got := fmt.Sprint(drain(m)); got != "[reset:4 f:r1 reset:6]" {
		t.Fatalf("got %s", got)
	}
}

func TestMailbox_StalledConsumerUnderSustainedOverflowStaysBounded(t *testing.T) {
	for _, policy := range []stream.OverflowPolicy{stream.DropOldest, stream.DropNewest} {
		m := newMailbox(8, policy)
		m.pushReset(1)
		for i := 0; i < 200000; i++ {
			m.pushFrame(frameN(i))
		}
		m.mu.Lock()
		live, capacity := len(m.q)-m.head, cap(m.q)
		m.mu.Unlock()
		if live > 8+3 || capacity > 1000 {
			t.Fatalf("%v: %d live items and capacity %d after a flood with a stalled consumer", policy, live, capacity)
		}
	}
}

func TestMailbox_OnlyControlItemsDoNotCountAgainstLimit(t *testing.T) {
	m := newMailbox(1, stream.FailSubscription)
	for i := 0; i < 5; i++ {
		m.pushReset(uint64(i))
	}
	if m.pushFrame(frameN(1)) != pushOK {
		t.Fatal("control markers must not consume frame capacity")
	}
}

func TestMailbox_CloseDiscardsAndIgnoresFurtherPushes(t *testing.T) {
	m := newMailbox(5, stream.DropOldest)
	m.pushFrame(frameN(1))
	m.closeDiscard()
	if !m.isClosed() {
		t.Fatal("not closed")
	}
	if _, ok := m.pop(); ok {
		t.Fatal("closed mailbox must be empty")
	}
	if m.pushFrame(frameN(2)) != pushOK {
		t.Fatal("push to a closed mailbox is silently ignored")
	}
	m.pushReset(1)
	if _, ok := m.pop(); ok {
		t.Fatal("closed mailbox must stay empty")
	}
	select {
	case <-m.wake:
	default:
		t.Fatal("closing must wake the consumer")
	}
}

func TestMailbox_CompactionPreservesOrderUnderChurn(t *testing.T) {
	m := newMailbox(10000, stream.DropOldest)
	next, want := 0, 0
	for round := 0; round < 50; round++ {
		for i := 0; i < 200; i++ {
			m.pushFrame(stream.Frame{Generation: uint64(next)})
			next++
		}
		for i := 0; i < 150; i++ {
			it, ok := m.pop()
			if !ok || it.frame.Generation != uint64(want) {
				t.Fatalf("round %d: got %v ok=%v want %d", round, it.frame.Generation, ok, want)
			}
			want++
		}
	}
	for want < next {
		it, ok := m.pop()
		if !ok || it.frame.Generation != uint64(want) {
			t.Fatalf("tail: got %v ok=%v want %d", it.frame.Generation, ok, want)
		}
		want++
	}
}

func TestMailbox_MinimumLimitIsOne(t *testing.T) {
	m := newMailbox(0, stream.DropOldest)
	m.pushFrame(frameN(1))
	m.pushFrame(frameN(2))
	if got := fmt.Sprint(drain(m)); got != "[gap:1 f:r2]" {
		t.Fatalf("got %s", got)
	}
}

func TestBackoffDelay(t *testing.T) {
	p := stream.NewConfig().Reconnect
	p.Jitter = 0.5
	for attempt := 1; attempt <= 12; attempt++ {
		upper := float64(p.MinDelay)
		for i := 1; i < attempt; i++ {
			upper *= p.Factor
		}
		if upper > float64(p.MaxDelay) {
			upper = float64(p.MaxDelay)
		}
		for i := 0; i < 200; i++ {
			d := backoffDelay(p, attempt)
			if float64(d) > upper || float64(d) < upper*(1-p.Jitter)-1 {
				t.Fatalf("attempt %d: delay %v outside [%v, %v]", attempt, d, time.Duration(upper*(1-p.Jitter)), time.Duration(upper))
			}
		}
	}
	if d := backoffDelay(p, 0); d <= 0 || d > p.MinDelay {
		t.Fatalf("attempt 0 must behave as attempt 1, got %v", d)
	}
	if d := backoffDelay(p, 10_000); d > p.MaxDelay {
		t.Fatalf("overflowing exponent must be capped, got %v", d)
	}
}

func TestResolveHeartbeat(t *testing.T) {
	old := minPingInterval
	defer func() { minPingInterval = old }()
	minPingInterval = time.Second

	tests := []struct {
		name        string
		cfg         stream.Config
		ep          Endpoint
		wi, wt      time.Duration
		wantSend    time.Duration
		wantTimeout time.Duration
		explanation string
	}{
		{"defaults", stream.Config{}, Endpoint{}, 0, 0, 9 * time.Second, 10 * time.Second, "18s default halved"},
		{"token response", stream.Config{}, Endpoint{PingInterval: 20 * time.Second, PingTimeout: 6 * time.Second}, 0, 0, 10 * time.Second, 6 * time.Second, "endpoint values halved"},
		{"welcome beats endpoint", stream.Config{}, Endpoint{PingInterval: 20 * time.Second, PingTimeout: 6 * time.Second}, 30 * time.Second, 4 * time.Second, 15 * time.Second, 4 * time.Second, "welcome wins"},
		{"explicit override is exact", stream.Config{PingInterval: 3 * time.Second, PingTimeout: 2 * time.Second}, Endpoint{PingInterval: 20 * time.Second}, 30 * time.Second, 9 * time.Second, 3 * time.Second, 2 * time.Second, "user wins, not halved"},
		{"floor of one second", stream.Config{PingInterval: 10 * time.Millisecond}, Endpoint{}, 0, 0, time.Second, 10 * time.Second, "KuCoin drops faster pingers"},
		{"tiny advertised interval floors", stream.Config{}, Endpoint{PingInterval: 500 * time.Millisecond}, 0, 0, time.Second, 10 * time.Second, "250ms floors to 1s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			send, timeout := resolveHeartbeat(tt.cfg, tt.ep, tt.wi, tt.wt)
			if send != tt.wantSend || timeout != tt.wantTimeout {
				t.Fatalf("%s: got send=%v timeout=%v, want %v / %v", tt.explanation, send, timeout, tt.wantSend, tt.wantTimeout)
			}
		})
	}
}
