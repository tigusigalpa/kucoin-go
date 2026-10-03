package wsengine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/stream"
)

// Regression tests for the lifecycle and ordering guarantees of the engine that
// are easy to break: who may end a subscription, in which order its unsubscribe
// reaches KuCoin, and what a reconnect owes the subscriptions it restores.

// countingHandler counts how often the terminal callbacks arrive.
type countingHandler struct {
	aborts, closes atomic.Int32
}

func (h *countingHandler) OnFrame(stream.Frame) {}
func (h *countingHandler) OnReset(uint64)       {}
func (h *countingHandler) OnGap(uint64)         {}
func (h *countingHandler) OnAbort(error)        { h.aborts.Add(1) }
func (h *countingHandler) OnClosed()            { h.closes.Add(1) }

// abortPanicker is a handler whose OnAbort panics.
type abortPanicker struct{ countingHandler }

func (h *abortPanicker) OnAbort(error) {
	h.aborts.Add(1)
	panic("boom in OnAbort")
}

func TestSub_APanicInOnAbortIsContainedAndStillReleasesTheQueue(t *testing.T) {
	e := newEnv(t)
	e.connect()
	h := &abortPanicker{}
	if _, err := e.conn.Subscribe(context.Background(), specFor("a", h)); err != nil {
		t.Fatal(err)
	}
	healthy := newRec()
	e.subscribe("b", healthy)
	// Closing the Conn ends every subscription on the caller's goroutine; the
	// panic of one handler must neither escape nor leave its pump goroutine behind
	// (the leak check at the end of the test would catch it).
	if err := e.conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	healthy.expect(t, "abort:", "closed")
	if h.aborts.Load() != 1 {
		t.Fatalf("OnAbort ran %d times", h.aborts.Load())
	}
	eventually(t, func() bool { return h.closes.Load() == 1 }, "OnClosed after the panicking OnAbort")
}

func TestSub_AbortReachesTheHandlerOnceHoweverManyCallersEndIt(t *testing.T) {
	c := New(&testProto{}, stream.Config{})
	h := &countingHandler{}
	c.mu.Lock()
	sb := c.newSubLocked(specFor("a", h))
	c.mu.Unlock()
	causes := []error{errors.New("first"), errors.New("second"), nil}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.abortSub(sb, causes[i%len(causes)])
		}(i)
	}
	wg.Wait()
	if n := h.aborts.Load(); n != 1 {
		t.Fatalf("OnAbort ran %d times; Shutdown, an overflow and a Close racing must not tell the handler twice", n)
	}
	select {
	case <-sb.done:
	default:
		t.Fatal("done must be closed once the handler was told")
	}
	if !sb.mb.isClosed() {
		t.Fatal("the queue must be released")
	}
}

func TestConn_NameStaysReservedUntilTheUnsubscribeFrameIsWritten(t *testing.T) {
	e := newEnv(t)
	e.connect()
	h := newRec()
	sh := e.subscribe("a", h)
	s := e.conn.currentSession()

	s.wmu.Lock() // stall every write on the live connection
	closed := make(chan error, 1)
	go func() { closed <- sh.Close() }()

	// Delivery stops at once, although the unsubscribe cannot be written yet ...
	h.expect(t, "abort:<nil>", "closed")
	// ... but the topic stays taken: a subscribe frame for it must not be able to
	// overtake the unsubscribe, which KuCoin would apply to the new subscription.
	res := make(chan error, 1)
	go func() {
		_, err := e.conn.Subscribe(context.Background(), specFor("a", newRec()))
		res <- err
	}()
	var err error
	select {
	case err = <-res:
	case <-time.After(2 * time.Second):
		err = errors.New("Subscribe was accepted and is waiting to write its frame")
	}
	s.wmu.Unlock()
	if !errors.Is(err, stream.ErrAlreadySubscribed) {
		t.Fatalf("Subscribe while the unsubscribe is pending = %v, want ErrAlreadySubscribed", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	e.subscribe("a", newRec())
	if got := fmt.Sprint(e.beh.sequence(1, "a")); got != "[subscribe unsubscribe subscribe]" {
		t.Fatalf("frames on the wire = %s", got)
	}
}

func TestConn_ConnectCannotStartANewLifeWhileTheOldOneIsStillPublishingItsEnd(t *testing.T) {
	e := newEnv(t)
	e.connect()
	e.conn.evMu.Lock() // the terminal event cannot be published: finish stalls inside emit
	closing := make(chan error, 1)
	go func() { closing <- e.conn.Close() }()

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.conn.State() == stream.StateClosed {
			e.conn.evMu.Unlock()
			t.Fatal("StateClosed was published before the life's event stream was closed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	res := make(chan error, 1)
	go func() { res <- e.conn.Connect(context.Background()) }()
	select {
	case err := <-res:
		if !errors.Is(err, stream.ErrClosed) {
			t.Errorf("Connect while the old life is ending = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Error("Connect blocked behind the ending life")
	}
	e.conn.evMu.Unlock()
	if err := <-closing; err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Afterwards a fresh life starts normally and owns a live event stream.
	e.connect()
	select {
	case <-e.conn.Done():
		t.Fatal("the new life must not be closed")
	default:
	}
	evs := watchEvents(e.conn)
	time.Sleep(20 * time.Millisecond)
	if evs.count(stream.EventClosed) != 0 {
		t.Fatalf("the new life received the old life's terminal event: %v", evs.types())
	}
}

func TestConn_EveryCloseCallReturnsOnlyOnceTheConnIsClosed(t *testing.T) {
	e := newEnv(t)
	e.connect()
	e.subscribe("a", newRec())
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.conn.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
			if st := e.conn.State(); st != stream.StateClosed {
				t.Errorf("Close returned in state %v", st)
			}
		}()
	}
	wg.Wait()
	select {
	case <-e.conn.Done():
	default:
		t.Fatal("Done must be closed once Close returned")
	}
}

func TestConn_ASecondCloseWaitsForTheFirstToFinish(t *testing.T) {
	e := newEnv(t)
	e.connect()
	e.conn.evMu.Lock() // the first Close stalls inside finish, publishing the terminal event
	first := make(chan error, 1)
	go func() { first <- e.conn.Close() }()
	eventually(t, func() bool { return e.conn.State() == stream.StateClosing }, "closing state")
	second := make(chan error, 1)
	go func() { second <- e.conn.Close() }()
	select {
	case err := <-second:
		e.conn.evMu.Unlock()
		t.Fatalf("the second Close returned (%v) while the first was still closing", err)
	case <-time.After(200 * time.Millisecond):
	}
	e.conn.evMu.Unlock()
	for i, c := range []chan error{first, second} {
		select {
		case err := <-c:
			if err != nil {
				t.Errorf("Close #%d: %v", i+1, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Close #%d did not return", i+1)
		}
	}
	if st := e.conn.State(); st != stream.StateClosed {
		t.Fatalf("state = %v", st)
	}
}

func TestConn_MaxAttemptsBudgetsEveryOutageSeparately(t *testing.T) {
	// StableAfter is an hour, so the backoff exponent never starts over; the
	// failure budget still must: every outage here is recovered at once.
	e := newEnv(t, func(c *stream.Config) { c.Reconnect.MaxAttempts = 2 })
	e.connect()
	e.subscribe("a", newRec())
	for outage := 1; outage <= 4; outage++ {
		e.srv.DropAll()
		e.waitReconnected(outage)
	}
	if st := e.conn.State(); st != stream.StateConnected {
		t.Fatalf("state = %v after four recovered outages (events: %v)", st, e.evs.types())
	}
}

func TestConn_ResubscribeThatKuCoinNeverAnswersFailsAloneOnALiveConnection(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) {
		c.AckTimeout = 300 * time.Millisecond
		c.PingInterval = 20 * time.Millisecond // the pongs prove the connection is alive
	})
	e.connect()
	good, silent := newRec(), newRec()
	e.subscribe("good", good)
	e.subscribe("silent", silent)
	e.beh.mu.Lock()
	e.beh.silentOnConn[2] = map[string]bool{"silent": true}
	e.beh.mu.Unlock()
	e.srv.DropAll()
	e.waitReconnected(1)

	silent.expect(t, "reset:", "abort:", "closed")
	if err := silent.err(); !errors.Is(err, stream.ErrAckTimeout) {
		t.Fatalf("silent ended with %v, want ErrAckTimeout", err)
	}
	eventually(t, func() bool {
		ev, ok := e.evs.first(stream.EventSubscriptionFailed)
		return ok && ev.Subscription == "silent"
	}, "a subscription-failed event for the silent one")
	good.expect(t, "reset:")
	push(t, e.serverConn(2), "good", 1)
	good.expect(t, "frame:good:1")
	// KuCoin may have applied the request it never answered: cancel it.
	eventually(t, func() bool { return e.beh.count(2, "unsubscribe", "silent") == 1 }, "an unsubscribe for the unanswered subscription")
	if st := e.conn.Stats(); st.State != stream.StateConnected || st.Subscriptions != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestConn_ResubscribeTimeoutOnASilentConnectionStillRetriesTheReconnect(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.AckTimeout = 150 * time.Millisecond })
	e.connect()
	h := newRec()
	e.subscribe("a", h)
	e.beh.mu.Lock()
	e.beh.silentOnConn[2] = map[string]bool{"a": true} // nothing at all comes back on connection 2
	e.beh.mu.Unlock()
	e.srv.DropAll()
	e.waitReconnected(1)

	if got := e.srv.Connections(); got < 3 {
		t.Fatalf("%d connections; a connection that answers nothing must be replaced", got)
	}
	push(t, e.serverConn(3), "a", 1)
	for { // the markers of the failed and of the successful attempt may or may not have merged
		got := h.next(t)
		if strings.HasPrefix(got, "frame:a:1") {
			break
		}
		if !strings.HasPrefix(got, "reset:") {
			t.Fatalf("unexpected handler callback %q", got)
		}
	}
}

func TestConn_SubscribeSurvivesTheConnectionDroppingBeforeTheAck(t *testing.T) {
	e := newEnv(t)
	e.connect()
	var dropped atomic.Bool
	e.beh.mu.Lock()
	e.beh.onFrame = func(c *wstest.Conn, m map[string]any) {
		if wstest.Str(m, "type") == "subscribe" && wstest.Str(m, "route") == "x" && c.Index == 1 && dropped.CompareAndSwap(false, true) {
			c.Drop() // the connection dies before KuCoin acknowledges
		}
	}
	e.beh.mu.Unlock()
	h := newRec()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sh, err := e.conn.Subscribe(ctx, specFor("x", h))
	if err != nil {
		t.Fatalf("Subscribe: %v; a call made on a healthy connection must outlive a reconnect", err)
	}
	e.waitReconnected(1)
	if got := e.beh.count(2, "subscribe", "x"); got != 1 {
		t.Fatalf("the restored connection saw %d subscribe frames, want 1", got)
	}
	push(t, e.serverConn(2), "x", 1)
	h.expect(t, "reset:", "frame:x:1")
	if err := sh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConn_SubscribeReportsTheCauseWhenTheConnectionDiesForGood(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.Reconnect.Disabled = true })
	e.connect()
	var dropped atomic.Bool
	e.beh.mu.Lock()
	e.beh.onFrame = func(c *wstest.Conn, m map[string]any) {
		if wstest.Str(m, "type") == "subscribe" && dropped.CompareAndSwap(false, true) {
			c.Drop()
		}
	}
	e.beh.mu.Unlock()
	h := newRec()
	_, err := e.conn.Subscribe(context.Background(), specFor("x", h))
	if err == nil || !strings.Contains(err.Error(), "connection lost") {
		t.Fatalf("Subscribe = %v, want the connection-lost cause", err)
	}
	h.expect(t, "abort:", "closed")
	if e.conn.Err() == nil {
		t.Fatal("the Conn must have closed with the cause")
	}
}

func TestConn_ClosingASubscriptionWhileItsResubscribeIsInFlight(t *testing.T) {
	e := newEnv(t)
	e.connect()
	h := newRec()
	sh := e.subscribe("a", h)
	release := make(chan struct{})
	e.beh.mu.Lock()
	e.beh.onFrame = func(c *wstest.Conn, m map[string]any) {
		if c.Index == 2 && wstest.Str(m, "type") == "subscribe" && wstest.Str(m, "route") == "a" {
			<-release // the acknowledgement of the restore is held back
		}
	}
	e.beh.mu.Unlock()
	e.srv.DropAll()
	eventually(t, func() bool { return e.beh.count(2, "subscribe", "a") == 1 }, "the resubscribe on connection 2")

	closed := make(chan error, 1)
	go func() { closed <- sh.Close() }()
	h.expect(t, "reset:", "abort:<nil>", "closed")
	close(release)
	e.waitReconnected(1)
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	// KuCoin got the subscription and must be told to drop it, after it.
	eventually(t, func() bool { return fmt.Sprint(e.beh.sequence(2, "a")) == "[subscribe unsubscribe]" }, "wire order on connection 2, got %v", e.beh.sequence(2, "a"))
	time.Sleep(50 * time.Millisecond)
	if got := fmt.Sprint(e.beh.sequence(2, "a")); got != "[subscribe unsubscribe]" {
		t.Fatalf("wire order on connection 2 = %s", got)
	}
}

func TestConn_ASubscriptionClosedBeforeItsResubscribeWasWrittenIsStillCancelled(t *testing.T) {
	e := newEnv(t)
	e.connect()
	h := newRec()
	sh := e.subscribe("a", h)
	held := make(chan *session, 1)
	e.proto.mu.Lock()
	e.proto.welcomed = func(_ context.Context, r Requester) error {
		if s, ok := r.(*session); ok && s.gen > 1 {
			s.wmu.Lock() // the restore cannot write its subscribe frame yet
			held <- s
		}
		return nil
	}
	e.proto.mu.Unlock()
	e.srv.DropAll()
	var ns *session
	select {
	case ns = <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("the replacement connection never came")
	}
	h.expect(t, "reset:") // the restore queued its marker and is stuck writing

	// Nothing was sent for this subscription on the new connection, so Close has
	// nothing to cancel there and returns at once ...
	if err := sh.Close(); err != nil {
		ns.wmu.Unlock()
		t.Fatalf("Close: %v", err)
	}
	h.expect(t, "abort:<nil>", "closed")
	ns.wmu.Unlock()
	// ... but the restore then writes the frame anyway; it must cancel it.
	e.waitReconnected(1)
	eventually(t, func() bool { return fmt.Sprint(e.beh.sequence(2, "a")) == "[subscribe unsubscribe]" }, "wire order on connection 2, got %v", e.beh.sequence(2, "a"))
}

// gateLogger blocks the first "too slow" warning until released.
type gateLogger struct {
	stream.NopLogger
	entered chan struct{}
	gate    chan struct{}
	once    sync.Once
}

func (l *gateLogger) Warn(msg string, _ ...any) {
	if strings.Contains(msg, "too slow") {
		l.once.Do(func() { close(l.entered) })
		<-l.gate
	}
}

func TestConn_RoutingNeverCallsOutWhileHoldingTheRegistryLock(t *testing.T) {
	gl := &gateLogger{entered: make(chan struct{}), gate: make(chan struct{})}
	e := newEnv(t, func(c *stream.Config) { c.BufferSize = 1; c.Logger = gl })
	e.connect()
	h := newRec()
	h.gate = make(chan struct{})
	e.subscribe("a", h)
	sc := e.serverConn(1)
	push(t, sc, "a", 1)
	h.expect(t, "frame:a:1") // in flight inside the handler
	push(t, sc, "a", 2)      // fills the one-slot buffer
	push(t, sc, "a", 3)      // overflows: the reader logs and blocks inside the logger
	select {
	case <-gl.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the overflow was never logged")
	}

	// The reader is stuck in the logger. A writer of the registry must not be.
	done := make(chan struct{})
	go func() {
		e.conn.release(&sub{spec: Spec{Name: "nobody"}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("the registry lock was held while logging")
	}
	close(gl.gate)
	close(h.gate)
}
