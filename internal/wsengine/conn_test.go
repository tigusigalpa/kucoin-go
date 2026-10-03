package wsengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/stream"
)

func init() {
	// The production floor of one ping per second would make heartbeat tests
	// crawl; the logic under test is identical at any interval.
	minPingInterval = time.Millisecond
}

// ---- a tiny fake KuCoin dialect -------------------------------------------

type tframe struct {
	Type         string `json:"type"`
	ID           string `json:"id,omitempty"`
	Route        string `json:"route,omitempty"`
	Code         int    `json:"code,omitempty"`
	Msg          string `json:"msg,omitempty"`
	PingInterval int    `json:"pingInterval,omitempty"` // milliseconds
	N            int    `json:"n,omitempty"`
	Pad          string `json:"pad,omitempty"`
}

type testProto struct {
	server *wstest.Server

	mu            sync.Mutex
	endpointCalls int
	endpointErrs  []error // consumed one per call; nil entries mean "succeed"
	gate          chan struct{}
	epPing        time.Duration
	epTimeout     time.Duration
	welcomed      func(ctx context.Context, r Requester) error
	limit         int // client messages per window the fake server tolerates; zero: no limit
	window        time.Duration
}

func (p *testProto) Name() string { return "test" }

func (p *testProto) Endpoint(ctx context.Context) (Endpoint, error) {
	p.mu.Lock()
	p.endpointCalls++
	call := p.endpointCalls
	var err error
	if len(p.endpointErrs) > 0 {
		err, p.endpointErrs = p.endpointErrs[0], p.endpointErrs[1:]
	}
	gate := p.gate
	p.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return Endpoint{}, ctx.Err()
		}
	}
	if err != nil {
		return Endpoint{}, err
	}
	return Endpoint{URL: fmt.Sprintf("%s/?token=t%d", p.server.URL(), call), PingInterval: p.epPing, PingTimeout: p.epTimeout}, nil
}

// MessageLimit implements MessageLimiter for the tests that set a limit.
func (p *testProto) MessageLimit() (int, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.limit, p.window
}

func (p *testProto) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.endpointCalls
}

func (p *testProto) Classify(raw []byte) Inbound {
	var f tframe
	if json.Unmarshal(raw, &f) != nil {
		return Inbound{}
	}
	switch f.Type {
	case "welcome":
		return Inbound{Kind: KindWelcome, ID: f.ID, PingInterval: time.Duration(f.PingInterval) * time.Millisecond}
	case "pong":
		return Inbound{Kind: KindPong}
	case "ack":
		return Inbound{Kind: KindAck, ID: f.ID}
	case "nack":
		return Inbound{Kind: KindNack, ID: f.ID, Err: &stream.ServerError{ID: f.ID, Code: f.Code, Message: f.Msg}}
	case "error":
		return Inbound{Kind: KindError, ID: f.ID, Err: &stream.ServerError{ID: f.ID, Code: f.Code, Message: f.Msg}}
	case "push":
		return Inbound{Kind: KindPush, Route: f.Route, Msg: &f}
	}
	return Inbound{}
}

func (p *testProto) Ping(id string) []byte { return []byte(`{"type":"ping","id":"` + id + `"}`) }

func (p *testProto) Welcomed(ctx context.Context, r Requester) error {
	p.mu.Lock()
	fn := p.welcomed
	p.mu.Unlock()
	if fn != nil {
		return fn(ctx, r)
	}
	return nil
}

func specFor(name string, h stream.Handler, routes ...string) Spec {
	if len(routes) == 0 {
		routes = []string{name}
	}
	return Spec{
		Name:    name,
		Routes:  routes,
		Handler: h,
		Subscribe: func(id string) []byte {
			return []byte(fmt.Sprintf(`{"type":"subscribe","id":%q,"route":%q}`, id, name))
		},
		Unsubscribe: func(id string) []byte {
			return []byte(fmt.Sprintf(`{"type":"unsubscribe","id":%q,"route":%q}`, id, name))
		},
	}
}

type recorded struct {
	conn  int
	typ   string
	route string
	at    time.Time
}

// behavior scripts the fake server.
type behavior struct {
	mu            sync.Mutex
	welcomeDelay  time.Duration
	noWelcome     bool
	welcomePingMs int
	ignorePings   map[int]bool // connection index -> never answer pings
	nack          map[string]int
	nackOnConn    map[int]map[string]int
	silentOnConn  map[int]map[string]bool // subscribe frames the server never answers
	noAck         bool
	frames        []recorded
	onFrame       func(c *wstest.Conn, m map[string]any)
}

func (b *behavior) serve(c *wstest.Conn) {
	b.mu.Lock()
	delay, none, pingMs := b.welcomeDelay, b.noWelcome, b.welcomePingMs
	b.mu.Unlock()
	if none {
		for {
			if _, err := c.ReadText(); err != nil {
				return
			}
		}
	}
	time.Sleep(delay)
	_ = c.Send(tframe{Type: "welcome", ID: "cid", PingInterval: pingMs})
	for {
		m, err := c.ReadJSON()
		if err != nil {
			return
		}
		typ, id, route := wstest.Str(m, "type"), wstest.Str(m, "id"), wstest.Str(m, "route")
		b.mu.Lock()
		b.frames = append(b.frames, recorded{c.Index, typ, route, time.Now()})
		ignore := b.ignorePings[c.Index]
		code := b.nack[route]
		if cc, ok := b.nackOnConn[c.Index]; ok && cc[route] != 0 {
			code = cc[route]
		}
		noAck, hook := b.noAck || b.silentOnConn[c.Index][route], b.onFrame
		b.mu.Unlock()
		if hook != nil {
			hook(c, m)
		}
		switch typ {
		case "ping":
			if !ignore {
				_ = c.Send(tframe{Type: "pong", ID: id})
			}
		case "subscribe":
			switch {
			case code != 0:
				_ = c.Send(tframe{Type: "nack", ID: id, Code: code, Msg: "rejected " + route})
			case !noAck:
				_ = c.Send(tframe{Type: "ack", ID: id})
			}
		case "unsubscribe":
			_ = c.Send(tframe{Type: "ack", ID: id})
		}
	}
}

// sequence lists, in order, the types of the subscribe and unsubscribe frames
// the server received for route on one connection.
func (b *behavior) sequence(conn int, route string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for _, f := range b.frames {
		if f.conn == conn && f.route == route && (f.typ == "subscribe" || f.typ == "unsubscribe") {
			out = append(out, f.typ)
		}
	}
	return out
}

// busiest returns the largest number of frames of one type the server received on
// a connection within any single interval of the given length.
func (b *behavior) busiest(conn int, typ string, window time.Duration) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	var times []time.Time
	for _, f := range b.frames {
		if f.conn == conn && f.typ == typ {
			times = append(times, f.at)
		}
	}
	most := 0
	for i := range times {
		n := 0
		for j := i; j < len(times) && times[j].Sub(times[i]) < window; j++ {
			n++
		}
		if n > most {
			most = n
		}
	}
	return most
}

// arrivals lists, as offsets from the first one, when frames of one type reached
// the server on a connection.
func (b *behavior) arrivals(conn int, typ string) []time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	var first time.Time
	var out []time.Duration
	for _, f := range b.frames {
		if f.conn == conn && f.typ == typ {
			if first.IsZero() {
				first = f.at
			}
			out = append(out, f.at.Sub(first).Round(time.Millisecond))
		}
	}
	return out
}

func (b *behavior) count(conn int, typ, route string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, f := range b.frames {
		if (conn == 0 || f.conn == conn) && f.typ == typ && (route == "" || f.route == route) {
			n++
		}
	}
	return n
}

// recHandler records everything a subscription's handler is told.
type recHandler struct {
	log       chan string
	gate      chan struct{} // when non-nil OnFrame blocks (after logging) until it is closed or the subscription aborts
	aborted   chan struct{}
	abortOnce sync.Once
	mu        sync.Mutex
	abortErr  error
	panicOn   int
}

func newRec() *recHandler {
	return &recHandler{log: make(chan string, 4096), aborted: make(chan struct{})}
}

func (h *recHandler) OnFrame(f stream.Frame) {
	m := f.Msg.(*tframe)
	h.log <- fmt.Sprintf("frame:%s:%d", f.Route, m.N)
	if h.panicOn != 0 && m.N == h.panicOn {
		panic("boom")
	}
	if h.gate != nil {
		select {
		case <-h.gate:
		case <-h.aborted:
		}
	}
}
func (h *recHandler) OnReset(g uint64) { h.log <- fmt.Sprintf("reset:%d", g) }
func (h *recHandler) OnGap(d uint64)   { h.log <- fmt.Sprintf("gap:%d", d) }
func (h *recHandler) OnClosed()        { h.log <- "closed" }
func (h *recHandler) OnAbort(err error) {
	h.abortOnce.Do(func() {
		h.mu.Lock()
		h.abortErr = err
		h.mu.Unlock()
		close(h.aborted)
		h.log <- fmt.Sprintf("abort:%v", err)
	})
}
func (h *recHandler) err() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.abortErr
}

func (h *recHandler) next(t *testing.T) string {
	t.Helper()
	select {
	case s := <-h.log:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a handler callback")
		return ""
	}
}

func (h *recHandler) expect(t *testing.T, want ...string) {
	t.Helper()
	for _, w := range want {
		if got := h.next(t); !strings.HasPrefix(got, w) {
			t.Fatalf("handler callback = %q, want prefix %q", got, w)
		}
	}
}

func (h *recHandler) expectNone(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case s := <-h.log:
		t.Fatalf("unexpected handler callback %q", s)
	case <-time.After(d):
	}
}

type evLog struct {
	mu  sync.Mutex
	evs []stream.Event
}

func watchEvents(c *Conn) *evLog {
	l := &evLog{}
	ch := c.Events()
	go func() {
		for ev := range ch {
			l.mu.Lock()
			l.evs = append(l.evs, ev)
			l.mu.Unlock()
		}
	}()
	return l
}

func (l *evLog) count(tp stream.EventType) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.evs {
		if e.Type == tp {
			n++
		}
	}
	return n
}

func (l *evLog) first(tp stream.EventType) (stream.Event, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.evs {
		if e.Type == tp {
			return e, true
		}
	}
	return stream.Event{}, false
}

func (l *evLog) types() []stream.EventType {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]stream.EventType, len(l.evs))
	for i, e := range l.evs {
		out[i] = e.Type
	}
	return out
}

func eventually(t *testing.T, cond func() bool, format string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met: "+format, args...)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type env struct {
	t     *testing.T
	srv   *wstest.Server
	beh   *behavior
	proto *testProto
	conn  *Conn
	evs   *evLog
}

func newEnv(t *testing.T, mutate ...func(*stream.Config)) *env {
	t.Helper()
	wstest.CheckLeaks(t) // first, so its cleanup runs last
	beh := &behavior{ignorePings: map[int]bool{}, nack: map[string]int{}, nackOnConn: map[int]map[string]int{}, silentOnConn: map[int]map[string]bool{}}
	srv := wstest.NewServer(t, beh.serve)
	proto := &testProto{server: srv}
	cfg := stream.Config{
		Reconnect:      stream.ReconnectPolicy{MinDelay: 5 * time.Millisecond, MaxDelay: 20 * time.Millisecond, Jitter: 0.2, StableAfter: time.Hour},
		ConnectTimeout: 2 * time.Second,
		AckTimeout:     time.Second,
		WriteTimeout:   2 * time.Second,
		PingInterval:   time.Hour, // keep heartbeats out of the way unless a test wants them
	}
	for _, m := range mutate {
		m(&cfg)
	}
	conn := New(proto, cfg)
	e := &env{t: t, srv: srv, beh: beh, proto: proto, conn: conn, evs: watchEvents(conn)}
	t.Cleanup(func() { _ = conn.Close() })
	return e
}

func (e *env) connect() {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.conn.Connect(ctx); err != nil {
		e.t.Fatalf("Connect: %v", err)
	}
}

func (e *env) subscribe(name string, h stream.Handler, routes ...string) *SubHandle {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sh, err := e.conn.Subscribe(ctx, specFor(name, h, routes...))
	if err != nil {
		e.t.Fatalf("Subscribe(%s): %v", name, err)
	}
	return sh
}

func push(t *testing.T, c *wstest.Conn, route string, n int) {
	t.Helper()
	if err := c.Send(tframe{Type: "push", Route: route, N: n}); err != nil {
		t.Fatalf("push: %v", err)
	}
}

func (e *env) serverConn(n int) *wstest.Conn {
	e.t.Helper()
	var c *wstest.Conn
	eventually(e.t, func() bool { c = e.srv.Conn(n); return c != nil }, "server connection %d", n)
	return c
}

func (e *env) waitReconnected(n int) {
	e.t.Helper()
	eventually(e.t, func() bool { return e.evs.count(stream.EventReconnected) >= n }, "reconnected event #%d (events: %v)", n, e.evs.types())
}

// ---- tests ------------------------------------------------------------------

func TestConn_ConnectSubscribePushUnsubscribe(t *testing.T) {
	e := newEnv(t)
	e.connect()
	if e.conn.State() != stream.StateConnected {
		t.Fatalf("state = %v", e.conn.State())
	}
	h := newRec()
	sh := e.subscribe("a", h)
	sc := e.serverConn(1)
	push(t, sc, "a", 1)
	push(t, sc, "other", 99) // nobody listens: must be ignored
	push(t, sc, "a", 2)
	push(t, sc, "a", 3)
	h.expect(t, "frame:a:1", "frame:a:2", "frame:a:3")

	if err := sh.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := sh.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	h.expect(t, "abort:<nil>", "closed")
	if got := e.beh.count(1, "unsubscribe", "a"); got != 1 {
		t.Fatalf("server saw %d unsubscribe frames, want 1", got)
	}
	push(t, sc, "a", 4)
	h.expectNone(t, 100*time.Millisecond)
	if st := e.conn.Stats(); st.Subscriptions != 0 || st.FramesReceived == 0 {
		t.Fatalf("stats: %+v", st)
	}

	// The same name can be subscribed again after it ended.
	h2 := newRec()
	e.subscribe("a", h2)
	push(t, sc, "a", 5)
	h2.expect(t, "frame:a:5")
}

func TestConn_ARouteListedTwiceDeliversEachPushOnce(t *testing.T) {
	e := newEnv(t)
	e.connect()
	h := newRec()
	e.subscribe("dup", h, "a", "b", "a")
	sc := e.serverConn(1)
	push(t, sc, "a", 1)
	h.expect(t, "frame:a:1")
	h.expectNone(t, 50*time.Millisecond) // not delivered a second time through the repeated route
	push(t, sc, "b", 2)
	h.expect(t, "frame:b:2")
	h.expectNone(t, 20*time.Millisecond)
}

func TestConn_OneSubscriptionCanListenOnSeveralKeys(t *testing.T) {
	e := newEnv(t)
	e.connect()
	multi, other := newRec(), newRec()
	e.subscribe("multi", multi, "x", "y")
	e.subscribe("other", other, "z")
	sc := e.serverConn(1)
	push(t, sc, "x", 1)
	push(t, sc, "z", 2)
	push(t, sc, "y", 3)
	multi.expect(t, "frame:x:1", "frame:y:3")
	other.expect(t, "frame:z:2")
	other.expectNone(t, 50*time.Millisecond)
}

func TestConn_SubscribeRejectedByServerIsTypedAndLeavesNoState(t *testing.T) {
	e := newEnv(t)
	e.beh.nack["bad"] = 404
	e.connect()
	h := newRec()
	start := time.Now()
	_, err := e.conn.Subscribe(context.Background(), specFor("bad", h))
	if err == nil {
		t.Fatal("expected an error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("rejection took %v; it must not wait for a timeout", elapsed)
	}
	var se *stream.ServerError
	if !errors.As(err, &se) || se.Code != 404 || !errors.Is(err, stream.ErrTopicNotFound) {
		t.Fatalf("error = %v, want a typed 404", err)
	}
	h.expect(t, "abort:", "closed")
	if st := e.conn.Stats(); st.Subscriptions != 0 {
		t.Fatalf("a rejected subscription must not stay registered: %+v", st)
	}
	if e.conn.State() != stream.StateConnected {
		t.Fatal("a rejected subscription must not disturb the connection")
	}
}

func TestConn_DuplicateSubscriptionIsRejected(t *testing.T) {
	e := newEnv(t)
	e.connect()
	h := newRec()
	e.subscribe("a", h)
	_, err := e.conn.Subscribe(context.Background(), specFor("a", newRec()))
	if !errors.Is(err, stream.ErrAlreadySubscribed) {
		t.Fatalf("error = %v, want ErrAlreadySubscribed", err)
	}
	push(t, e.serverConn(1), "a", 1)
	h.expect(t, "frame:a:1") // the original subscription is untouched
}

func TestConn_OverlappingRoutesAreRejected(t *testing.T) {
	e := newEnv(t)
	e.connect()
	first := newRec()
	e.subscribe("first", first, "x", "y")
	_, err := e.conn.Subscribe(context.Background(), specFor("second", newRec(), "y", "z"))
	if !errors.Is(err, stream.ErrAlreadySubscribed) || !strings.Contains(err.Error(), "overlaps first") {
		t.Fatalf("error = %v, want an overlap rejection", err)
	}
	if e.beh.count(1, "subscribe", "second") != 0 {
		t.Fatal("an overlapping subscription must be refused before anything is sent")
	}
	// After the first one ends, its routes are free again.
	if err := e.conn.UnsubscribeName("first"); err != nil {
		t.Fatal(err)
	}
	e.subscribe("second", newRec(), "y", "z")
	if err := e.conn.UnsubscribeName("never-subscribed"); err != nil {
		t.Fatalf("unsubscribing an unknown name must be a no-op, got %v", err)
	}
}

func TestConn_InvalidSpecIsRejected(t *testing.T) {
	e := newEnv(t)
	e.connect()
	for _, spec := range []Spec{
		{Name: "", Handler: newRec(), Subscribe: func(string) []byte { return nil }},
		{Name: "n", Subscribe: func(string) []byte { return nil }},
		{Name: "n", Handler: newRec()},
	} {
		if _, err := e.conn.Subscribe(context.Background(), spec); err == nil {
			t.Errorf("spec %+v must be rejected", spec.Name)
		}
	}
}

func TestConn_SubscribeAckTimeoutSendsBestEffortUnsubscribe(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.AckTimeout = 80 * time.Millisecond })
	e.beh.noAck = true
	e.connect()
	h := newRec()
	_, err := e.conn.Subscribe(context.Background(), specFor("slow", h))
	if !errors.Is(err, stream.ErrAckTimeout) {
		t.Fatalf("error = %v, want ErrAckTimeout", err)
	}
	h.expect(t, "abort:", "closed")
	eventually(t, func() bool { return e.beh.count(1, "unsubscribe", "slow") == 1 }, "best-effort unsubscribe after an ack timeout")
}

func TestConn_SubscribeHonoursContext(t *testing.T) {
	e := newEnv(t)
	e.beh.noAck = true
	e.connect()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_, err := e.conn.Subscribe(ctx, specFor("c", newRec()))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
}

func TestConn_SubscribeBeforeConnectAndAfterClose(t *testing.T) {
	e := newEnv(t)
	if _, err := e.conn.Subscribe(context.Background(), specFor("a", newRec())); !errors.Is(err, stream.ErrNotConnected) {
		t.Fatalf("before Connect: %v", err)
	}
	e.connect()
	if err := e.conn.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.conn.Subscribe(context.Background(), specFor("a", newRec())); !errors.Is(err, stream.ErrClosed) {
		t.Fatalf("after Close: %v", err)
	}
}

func TestConn_WelcomeTimeout(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.ConnectTimeout = 150 * time.Millisecond })
	e.beh.noWelcome = true
	err := e.conn.Connect(context.Background())
	if !errors.Is(err, stream.ErrWelcomeTimeout) {
		t.Fatalf("error = %v, want ErrWelcomeTimeout", err)
	}
	if e.conn.State() != stream.StateIdle {
		t.Fatalf("state after a failed Connect = %v, want idle", e.conn.State())
	}
}

func TestConn_ConnectErrorsAreTyped(t *testing.T) {
	t.Run("token failure", func(t *testing.T) {
		e := newEnv(t)
		cause := errors.New("token endpoint down")
		e.proto.endpointErrs = []error{cause}
		err := e.conn.Connect(context.Background())
		if !errors.Is(err, stream.ErrTokenUnavailable) || !errors.Is(err, cause) {
			t.Fatalf("error = %v", err)
		}
		if stream.IsPermanent(err) {
			t.Fatal("a transient token failure must not be permanent")
		}
	})
	t.Run("permanent token failure", func(t *testing.T) {
		e := newEnv(t)
		e.proto.endpointErrs = []error{stream.Permanent(errors.New("revoked key"))}
		err := e.conn.Connect(context.Background())
		if !errors.Is(err, stream.ErrTokenUnavailable) || !stream.IsPermanent(err) {
			t.Fatalf("error = %v, want permanent ErrTokenUnavailable", err)
		}
	})
	t.Run("http 429 on upgrade", func(t *testing.T) {
		e := newEnv(t)
		e.srv.RejectUpgrades(1, 429)
		if err := e.conn.Connect(context.Background()); !errors.Is(err, stream.ErrRateLimited) {
			t.Fatalf("error = %v, want ErrRateLimited", err)
		}
	})
	t.Run("http 401 on upgrade", func(t *testing.T) {
		e := newEnv(t)
		e.srv.RejectUpgrades(1, 401)
		if err := e.conn.Connect(context.Background()); !errors.Is(err, stream.ErrTokenInvalid) {
			t.Fatalf("error = %v, want ErrTokenInvalid", err)
		}
	})
	t.Run("http 503 on upgrade", func(t *testing.T) {
		e := newEnv(t)
		e.srv.RejectUpgrades(1, 503)
		if err := e.conn.Connect(context.Background()); !errors.Is(err, stream.ErrServiceBusy) {
			t.Fatalf("error = %v, want ErrServiceBusy", err)
		}
	})
	t.Run("authentication step fails", func(t *testing.T) {
		e := newEnv(t)
		e.proto.welcomed = func(context.Context, Requester) error { return stream.Permanent(stream.ErrAuthFailed) }
		err := e.conn.Connect(context.Background())
		if !errors.Is(err, stream.ErrAuthFailed) {
			t.Fatalf("error = %v", err)
		}
		if e.conn.State() != stream.StateIdle {
			t.Fatalf("state = %v", e.conn.State())
		}
	})
}

func TestConn_InitialConnectAttemptsRetry(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.InitialConnectAttempts = 3 })
	boom := errors.New("down")
	e.proto.endpointErrs = []error{boom, boom, nil}
	e.connect()
	if got := e.proto.calls(); got != 3 {
		t.Fatalf("endpoint called %d times, want 3", got)
	}

	// A permanent error is not retried.
	e2 := newEnv(t, func(c *stream.Config) { c.InitialConnectAttempts = 5 })
	e2.proto.endpointErrs = []error{stream.Permanent(boom)}
	if err := e2.conn.Connect(context.Background()); err == nil {
		t.Fatal("expected an error")
	}
	if got := e2.proto.calls(); got != 1 {
		t.Fatalf("permanent failure retried: endpoint called %d times", got)
	}
}

func TestConn_ConnectTwiceAndReconnectAfterClose(t *testing.T) {
	e := newEnv(t)
	e.connect()
	if err := e.conn.Connect(context.Background()); !errors.Is(err, stream.ErrAlreadyConnected) {
		t.Fatalf("second Connect = %v", err)
	}
	h := newRec()
	e.subscribe("a", h)
	if err := e.conn.Close(); err != nil {
		t.Fatal(err)
	}
	h.expect(t, "abort:<nil>", "closed")
	select {
	case <-e.conn.Done():
	default:
		t.Fatal("Done must be closed after Close")
	}
	if e.conn.State() != stream.StateClosed {
		t.Fatalf("state = %v", e.conn.State())
	}
	// A closed Conn can be connected again; it starts with no subscriptions.
	e.connect()
	if e.conn.State() != stream.StateConnected || e.conn.Stats().Subscriptions != 0 {
		t.Fatalf("reconnected life: %+v", e.conn.Stats())
	}
	h2 := newRec()
	e.subscribe("a", h2)
	push(t, e.serverConn(2), "a", 1)
	h2.expect(t, "frame:a:1")
}

func TestConn_CloseDuringConnect(t *testing.T) {
	e := newEnv(t)
	e.proto.gate = make(chan struct{}) // Endpoint blocks until released
	res := make(chan error, 1)
	go func() { res <- e.conn.Connect(context.Background()) }()
	eventually(t, func() bool { return e.conn.State() == stream.StateConnecting && e.proto.calls() == 1 }, "connect in progress")
	if err := e.conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-res:
		if !errors.Is(err, stream.ErrClosed) {
			t.Fatalf("Connect = %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Connect did not return after Close")
	}
}

func TestConn_ReconnectRefreshesTokenResubscribesAndMarksReset(t *testing.T) {
	e := newEnv(t)
	e.connect()
	a, b := newRec(), newRec()
	e.subscribe("a", a)
	e.subscribe("b", b)
	first := e.serverConn(1)
	push(t, first, "a", 1)
	a.expect(t, "frame:a:1")
	gen1 := e.conn.Generation()

	e.srv.DropAll()
	e.waitReconnected(1)

	if got := e.proto.calls(); got != 2 {
		t.Fatalf("endpoint calls = %d; every reconnect must fetch a fresh token/endpoint", got)
	}
	if e.beh.count(2, "subscribe", "a") != 1 || e.beh.count(2, "subscribe", "b") != 1 {
		t.Fatalf("subscriptions were not restored on the new connection: %+v", e.beh.frames)
	}
	if e.conn.Generation() <= gen1 {
		t.Fatal("generation must increase with every connection")
	}
	// An ordered reset marker tells each consumer that updates may be missing.
	a.expect(t, "reset:")
	b.expect(t, "reset:")
	push(t, e.serverConn(2), "a", 2)
	a.expect(t, "frame:a:2")

	want := []stream.EventType{stream.EventConnected, stream.EventDisconnected, stream.EventReconnecting, stream.EventReconnected}
	got := e.evs.types()
	if len(got) < len(want) {
		t.Fatalf("events = %v", got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("events = %v, want prefix %v", got, want)
		}
	}
	if st := e.conn.Stats(); st.Reconnects != 1 || st.State != stream.StateConnected || st.Subscriptions != 2 {
		t.Fatalf("stats: %+v", st)
	}
	if ev, _ := e.evs.first(stream.EventReconnecting); ev.Attempt != 1 || ev.Backoff <= 0 {
		t.Fatalf("reconnecting event: %+v", ev)
	}
}

func TestConn_ReconnectBacksOffThenGivesUpAfterMaxAttempts(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.Reconnect.MaxAttempts = 3 })
	e.connect()
	h := newRec()
	e.subscribe("a", h)
	boom := errors.New("token service unreachable")
	e.proto.mu.Lock()
	e.proto.endpointErrs = []error{boom, boom, boom, boom, boom}
	e.proto.mu.Unlock()
	e.srv.DropAll()

	select {
	case <-e.conn.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("Conn never gave up; events: %v", e.evs.types())
	}
	err := e.conn.Err()
	if err == nil || !strings.Contains(err.Error(), "giving up after 3 reconnect attempts") || !errors.Is(err, boom) {
		t.Fatalf("Err = %v", err)
	}
	h.expect(t, "abort:", "closed") // never restored, so no reset marker either
	if herr := h.err(); herr == nil || !errors.Is(herr, boom) {
		t.Fatalf("subscription error = %v", herr)
	}
	if got := e.evs.count(stream.EventReconnecting); got != 3 {
		t.Fatalf("reconnecting events = %d, want 3", got)
	}
	eventually(t, func() bool { return e.evs.count(stream.EventClosed) == 1 }, "the terminal event")
	if ev, _ := e.evs.first(stream.EventClosed); ev.Err == nil {
		t.Fatalf("terminal event: %+v", ev)
	}
	_, serr := e.conn.Subscribe(context.Background(), specFor("x", newRec()))
	if !errors.Is(serr, stream.ErrClosed) || !errors.Is(serr, boom) {
		t.Fatalf("Subscribe after fatal close = %v", serr)
	}
}

func TestConn_PermanentErrorStopsReconnecting(t *testing.T) {
	e := newEnv(t)
	e.connect()
	h := newRec()
	e.subscribe("a", h)
	revoked := stream.Permanent(errors.New("api key revoked"))
	e.proto.mu.Lock()
	e.proto.endpointErrs = []error{revoked}
	e.proto.mu.Unlock()
	e.srv.DropAll()
	select {
	case <-e.conn.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Conn did not stop after a permanent error")
	}
	if !stream.IsPermanent(e.conn.Err()) {
		t.Fatalf("Err = %v", e.conn.Err())
	}
	if got := e.proto.calls(); got != 2 {
		t.Fatalf("endpoint calls = %d, want exactly 2 (initial + one reconnect)", got)
	}
}

func TestConn_ReconnectDisabledEndsWithCause(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.Reconnect.Disabled = true })
	e.connect()
	h := newRec()
	e.subscribe("a", h)
	e.srv.DropAll()
	select {
	case <-e.conn.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Conn did not close")
	}
	h.expect(t, "abort:", "closed")
	if e.conn.Err() == nil || e.evs.count(stream.EventReconnecting) != 0 {
		t.Fatalf("Err=%v events=%v", e.conn.Err(), e.evs.types())
	}
}

func TestConn_ResubscribeRejectionEndsOnlyThatSubscription(t *testing.T) {
	e := newEnv(t)
	e.connect()
	a, b := newRec(), newRec()
	e.subscribe("a", a)
	e.subscribe("b", b)
	e.beh.mu.Lock()
	e.beh.nackOnConn[2] = map[string]int{"b": 404}
	e.beh.mu.Unlock()
	e.srv.DropAll()
	e.waitReconnected(1)

	b.expect(t, "reset:", "abort:", "closed")
	if herr := b.err(); herr == nil || !errors.Is(herr, stream.ErrTopicNotFound) {
		t.Fatalf("b ended with %v", herr)
	}
	eventually(t, func() bool { return e.evs.count(stream.EventSubscriptionFailed) > 0 }, "subscription-failed event")
	if ev, _ := e.evs.first(stream.EventSubscriptionFailed); ev.Subscription != "b" {
		t.Fatalf("event: %+v", ev)
	}
	a.expect(t, "reset:")
	push(t, e.serverConn(2), "a", 7)
	a.expect(t, "frame:a:7")
	if st := e.conn.Stats(); st.Subscriptions != 1 || st.State != stream.StateConnected {
		t.Fatalf("stats: %+v", st)
	}
}

func TestConn_SubscribeWaitsWhileReconnecting(t *testing.T) {
	e := newEnv(t)
	e.connect()
	e.proto.mu.Lock()
	e.proto.gate = make(chan struct{}) // hold the reconnect inside Endpoint
	gate := e.proto.gate
	e.proto.mu.Unlock()
	e.srv.DropAll()
	eventually(t, func() bool { return e.conn.State() == stream.StateReconnecting }, "reconnecting state")

	res := make(chan error, 1)
	h := newRec()
	go func() {
		_, err := e.conn.Subscribe(context.Background(), specFor("late", h))
		res <- err
	}()
	select {
	case err := <-res:
		t.Fatalf("Subscribe returned %v while the connection was being restored", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(gate)
	select {
	case err := <-res:
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe did not complete after the reconnect")
	}
	push(t, e.serverConn(2), "late", 1)
	h.expect(t, "frame:late:1")
}

func TestConn_SubscribeWhileReconnectingHonoursContext(t *testing.T) {
	e := newEnv(t)
	e.connect()
	e.proto.mu.Lock()
	e.proto.gate = make(chan struct{})
	e.proto.mu.Unlock()
	e.srv.DropAll()
	eventually(t, func() bool { return e.conn.State() == stream.StateReconnecting }, "reconnecting state")
	if err := e.conn.Connect(context.Background()); !errors.Is(err, stream.ErrReconnecting) {
		t.Fatalf("Connect during a reconnect = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if _, err := e.conn.Subscribe(ctx, specFor("x", newRec())); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Subscribe = %v", err)
	}
}

func TestConn_PingWatchdogDetectsSilentPeerAndRecovers(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) {
		c.PingInterval = 25 * time.Millisecond
		c.PingTimeout = 60 * time.Millisecond
	})
	e.beh.mu.Lock()
	e.beh.ignorePings[1] = true // the first connection never answers
	e.beh.mu.Unlock()
	e.connect()
	e.waitReconnected(1)
	ev, _ := e.evs.first(stream.EventDisconnected)
	if !errors.Is(ev.Err, stream.ErrPingTimeout) {
		t.Fatalf("disconnect cause = %v, want ErrPingTimeout", ev.Err)
	}
	// The second connection answers pings and stays up.
	time.Sleep(200 * time.Millisecond)
	if e.conn.State() != stream.StateConnected {
		t.Fatalf("state = %v", e.conn.State())
	}
	if e.beh.count(2, "ping", "") < 3 {
		t.Fatalf("heartbeat not running on the new connection: %d pings", e.beh.count(2, "ping", ""))
	}
}

func TestConn_AnyInboundFrameCountsAsLiveness(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) {
		c.PingInterval = 25 * time.Millisecond
		c.PingTimeout = 80 * time.Millisecond
	})
	e.beh.mu.Lock()
	e.beh.ignorePings[1] = true
	e.beh.mu.Unlock()
	e.connect()
	h := newRec()
	e.subscribe("a", h)
	sc := e.serverConn(1)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // a busy market keeps the connection alive without any pong
		defer wg.Done()
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(15 * time.Millisecond):
				if sc.Send(tframe{Type: "push", Route: "a", N: i}) != nil {
					return
				}
			}
		}
	}()
	time.Sleep(400 * time.Millisecond)
	close(stop)
	wg.Wait()
	if e.evs.count(stream.EventDisconnected) != 0 || e.conn.State() != stream.StateConnected {
		t.Fatalf("a connection that receives data must not be declared dead; events %v", e.evs.types())
	}
}

func TestConn_HeartbeatUsesWelcomeAndEndpointParameters(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.PingInterval = 0 }) // no override: use what KuCoin advertises
	e.beh.mu.Lock()
	e.beh.welcomePingMs = 60 // advertised 60ms -> pinged every 30ms
	e.beh.mu.Unlock()
	e.connect()
	time.Sleep(250 * time.Millisecond)
	if n := e.beh.count(1, "ping", ""); n < 4 || n > 12 {
		t.Fatalf("pings = %d; expected roughly one per 30ms", n)
	}
}

func TestConn_UnsolicitedServerErrorIsReportedAsEvent(t *testing.T) {
	e := newEnv(t)
	e.connect()
	sc := e.serverConn(1)
	if err := sc.Send(tframe{Type: "error", ID: "cid", Code: 400, Msg: "ping timeout"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return e.evs.count(stream.EventServerError) == 1 }, "server error event")
	ev, _ := e.evs.first(stream.EventServerError)
	if !errors.Is(ev.Err, stream.ErrPingTimeout) {
		t.Fatalf("event error = %v", ev.Err)
	}
	if e.conn.State() != stream.StateConnected {
		t.Fatal("an error notice alone must not drop the connection")
	}
}

func TestConn_OversizedFrameKillsTheConnectionAndReconnects(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.ReadLimit = 512 })
	e.connect()
	a := newRec()
	e.subscribe("a", a)
	sc := e.serverConn(1)
	if err := sc.Send(tframe{Type: "push", Route: "a", N: 1, Pad: strings.Repeat("x", 4096)}); err != nil {
		t.Fatal(err)
	}
	e.waitReconnected(1)
	ev, _ := e.evs.first(stream.EventDisconnected)
	if !errors.Is(ev.Err, websocket.ErrReadLimit) {
		t.Fatalf("disconnect cause = %v, want the read-limit error", ev.Err)
	}
}

func TestConn_OverflowDropOldest(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.BufferSize = 3 })
	e.connect()
	h := newRec()
	h.gate = make(chan struct{})
	e.subscribe("a", h)
	sc := e.serverConn(1)
	push(t, sc, "a", 1)
	h.expect(t, "frame:a:1") // frame 1 is now in flight inside the handler
	for i := 2; i <= 10; i++ {
		push(t, sc, "a", i)
	}
	eventually(t, func() bool { return e.conn.Stats().PushesDropped == 6 }, "6 drops, got %d", e.conn.Stats().PushesDropped)
	close(h.gate)
	h.expect(t, "gap:6", "frame:a:8", "frame:a:9", "frame:a:10")
	eventually(t, func() bool { return e.evs.count(stream.EventOverflow) > 0 }, "overflow event")
	if ev, _ := e.evs.first(stream.EventOverflow); ev.Subscription != "a" || ev.Dropped == 0 {
		t.Fatalf("overflow event: %+v", ev)
	}
}

func TestConn_OverflowDropNewest(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.BufferSize = 3; c.Overflow = stream.DropNewest })
	e.connect()
	h := newRec()
	h.gate = make(chan struct{})
	e.subscribe("a", h)
	sc := e.serverConn(1)
	push(t, sc, "a", 1)
	h.expect(t, "frame:a:1")
	for i := 2; i <= 10; i++ {
		push(t, sc, "a", i)
	}
	eventually(t, func() bool { return e.conn.Stats().PushesDropped == 6 }, "6 drops")
	close(h.gate)
	// The newest frames were the ones lost: the gap comes after the backlog, and
	// it is reported although no frame follows it.
	h.expect(t, "frame:a:2", "frame:a:3", "frame:a:4", "gap:6")
	h.expectNone(t, 50*time.Millisecond)
}

func TestConn_OverflowFailSubscription(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.BufferSize = 2; c.Overflow = stream.FailSubscription })
	e.connect()
	slow, healthy := newRec(), newRec()
	slow.gate = make(chan struct{})
	e.subscribe("slow", slow)
	e.subscribe("healthy", healthy)
	sc := e.serverConn(1)
	push(t, sc, "slow", 1)
	slow.expect(t, "frame:slow:1")
	for i := 2; i <= 6; i++ {
		push(t, sc, "slow", i)
	}
	push(t, sc, "healthy", 1)
	healthy.expect(t, "frame:healthy:1")

	eventually(t, func() bool { return errors.Is(slow.err(), stream.ErrSlowConsumer) }, "slow subscription failed with ErrSlowConsumer, err=%v", slow.err())
	// KuCoin keeps sending until told to stop: the failed subscription is cancelled there too,
	// and its name is free again once that frame was written.
	eventually(t, func() bool { return e.beh.count(1, "unsubscribe", "slow") == 1 }, "an unsubscribe for the failed subscription")
	eventually(t, func() bool { return e.conn.Stats().Subscriptions == 1 }, "the failed subscription to leave the registry")
	if e.conn.State() != stream.StateConnected {
		t.Fatal("one slow consumer must not take the connection down")
	}
	eventually(t, func() bool { return e.evs.count(stream.EventOverflow) > 0 }, "overflow event")
	if ev, _ := e.evs.first(stream.EventOverflow); !errors.Is(ev.Err, stream.ErrSlowConsumer) {
		t.Fatalf("overflow event: %+v", ev)
	}
	// The healthy neighbour keeps flowing.
	push(t, sc, "healthy", 2)
	healthy.expect(t, "frame:healthy:2")
}

func TestConn_PerSubscriptionBufferAndPolicyOverrideDefaults(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.BufferSize = 100 })
	e.connect()
	h := newRec()
	h.gate = make(chan struct{})
	spec := specFor("a", h)
	spec.Buffer, spec.Overflow, spec.OverflowSet = 2, stream.DropNewest, true
	if _, err := e.conn.Subscribe(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	sc := e.serverConn(1)
	push(t, sc, "a", 1)
	h.expect(t, "frame:a:1")
	for i := 2; i <= 6; i++ {
		push(t, sc, "a", i)
	}
	eventually(t, func() bool { return e.conn.Stats().PushesDropped == 3 }, "3 drops")
	close(h.gate)
	h.expect(t, "frame:a:2", "frame:a:3", "gap:3")
}

func TestConn_HandlerPanicEndsOnlyItsSubscription(t *testing.T) {
	e := newEnv(t)
	e.connect()
	bad, good := newRec(), newRec()
	bad.panicOn = 2
	e.subscribe("bad", bad)
	e.subscribe("good", good)
	sc := e.serverConn(1)
	push(t, sc, "bad", 1)
	push(t, sc, "bad", 2)
	bad.expect(t, "frame:bad:1", "frame:bad:2", "abort:", "closed")
	if herr := bad.err(); herr == nil || !strings.Contains(herr.Error(), "panicked") {
		t.Fatalf("bad ended with %v", herr)
	}
	push(t, sc, "good", 1)
	good.expect(t, "frame:good:1")
	if e.conn.State() != stream.StateConnected {
		t.Fatal("a handler panic must not affect the connection")
	}
	eventually(t, func() bool { return e.beh.count(1, "unsubscribe", "bad") == 1 }, "an unsubscribe for the subscription whose handler panicked")
}

func TestConn_ShutdownReleasesBlockedHandlersAndLeaksNothing(t *testing.T) {
	e := newEnv(t)
	e.connect()
	blocked := newRec()
	blocked.gate = make(chan struct{}) // never opened: OnFrame is stuck until OnAbort
	idle := newRec()
	e.subscribe("blocked", blocked)
	e.subscribe("idle", idle)
	sc := e.serverConn(1)
	push(t, sc, "blocked", 1)
	blocked.expect(t, "frame:blocked:1")

	done := make(chan error, 1)
	go func() { done <- e.conn.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung on a handler blocked in OnFrame")
	}
	blocked.expect(t, "abort:<nil>", "closed")
	idle.expect(t, "abort:<nil>", "closed")
	if err := e.conn.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestConn_ShutdownDuringReconnectIsPrompt(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.Reconnect.MinDelay = 10 * time.Second; c.Reconnect.MaxDelay = time.Minute })
	e.connect()
	h := newRec()
	e.subscribe("a", h)
	e.srv.DropAll()
	eventually(t, func() bool { return e.conn.State() == stream.StateReconnecting }, "reconnecting")
	start := time.Now()
	if err := e.conn.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("Close waited %v for a backoff sleep", time.Since(start))
	}
	h.expect(t, "abort:<nil>", "closed")
}

func TestConn_ShutdownHonoursItsContext(t *testing.T) {
	e := newEnv(t)
	e.connect()
	// A handler that ignores OnAbort and stays stuck models a buggy custom
	// Handler: Shutdown must still return when its context expires.
	stuck := &stuckHandler{release: make(chan struct{}), entered: make(chan struct{})}
	e.subscribe("stuck", stuck)
	push(t, e.serverConn(1), "stuck", 1)
	<-stuck.entered
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	err := e.conn.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want the context error", err)
	}
	close(stuck.release) // let the goroutine finish so the leak check passes
}

type stuckHandler struct {
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (h *stuckHandler) OnFrame(stream.Frame) {
	h.once.Do(func() { close(h.entered) })
	<-h.release
}
func (h *stuckHandler) OnReset(uint64) {}
func (h *stuckHandler) OnGap(uint64)   {}
func (h *stuckHandler) OnAbort(error)  {}
func (h *stuckHandler) OnClosed()      {}

func TestConn_ConcurrentSubscribeUnsubscribePushAndClose(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.BufferSize = 8 })
	e.connect()
	sc := e.serverConn(1)

	stop := make(chan struct{})
	var flood sync.WaitGroup
	flood.Add(1)
	go func() {
		defer flood.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if sc.Send(tframe{Type: "push", Route: fmt.Sprintf("r%d", i%8), N: i}) != nil {
				return
			}
			time.Sleep(100 * time.Microsecond) // a flood the reader can keep up with, so acks are not stuck behind a backlog
		}
	}()

	var wg sync.WaitGroup
	var delivered atomic.Int64
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				name := fmt.Sprintf("r%d", w)
				h := stream.HandlerFuncs{Frame: func(stream.Frame) { delivered.Add(1) }}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				sh, err := e.conn.Subscribe(ctx, specFor(name, h))
				cancel()
				if err != nil {
					if errors.Is(err, stream.ErrAlreadySubscribed) {
						continue
					}
					t.Errorf("Subscribe: %v", err)
					return
				}
				time.Sleep(time.Millisecond)
				_ = sh.Close()
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	flood.Wait()
	if delivered.Load() == 0 {
		t.Fatal("nothing was delivered while subscribing concurrently")
	}
}

func TestConn_CloseRacingWithEverything(t *testing.T) {
	for round := 0; round < 5; round++ {
		e := newEnv(t, func(c *stream.Config) { c.BufferSize = 4 })
		e.connect()
		sc := e.serverConn(1)
		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < 20; i++ {
					h := stream.HandlerFuncs{}
					sh, err := e.conn.Subscribe(context.Background(), specFor(fmt.Sprintf("s%d-%d", w, i), h))
					if err != nil {
						return
					}
					_ = sh.Close()
				}
			}(w)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if sc.Send(tframe{Type: "push", Route: "s0-0", N: i}) != nil {
					return
				}
			}
		}()
		time.Sleep(time.Duration(round) * time.Millisecond)
		if err := e.conn.Close(); err != nil {
			t.Fatalf("round %d: Close: %v", round, err)
		}
		wg.Wait()
	}
}

func TestConn_EventHandlerMode(t *testing.T) {
	var mu sync.Mutex
	var got []stream.EventType
	e := newEnv(t, func(c *stream.Config) {
		c.OnEvent = func(ev stream.Event) {
			mu.Lock()
			got = append(got, ev.Type)
			mu.Unlock()
		}
	})
	if _, ok := <-e.conn.Events(); ok {
		t.Fatal("with an event handler the Events channel is closed")
	}
	e.connect()
	if err := e.conn.Close(); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 2
	}, "the connected and closed events reach the handler")
	mu.Lock()
	defer mu.Unlock()
	if got[0] != stream.EventConnected || got[1] != stream.EventClosed {
		t.Fatalf("events = %v", got)
	}
}

func TestConn_EmitDropsOldestWhenNobodyReads(t *testing.T) {
	c := New(&testProto{}, stream.Config{EventBuffer: 2})
	for i := 1; i <= 10; i++ {
		c.emit(stream.Event{Type: stream.EventOverflow, Dropped: uint64(i)})
	}
	ch := c.Events()
	first, second := <-ch, <-ch
	if first.Dropped != 9 || second.Dropped != 10 {
		t.Fatalf("kept events %d and %d, want the newest two (9, 10)", first.Dropped, second.Dropped)
	}
	select {
	case ev := <-ch:
		t.Fatalf("unexpected extra event %+v", ev)
	default:
	}
	c.closeEvents()
	c.emit(stream.Event{Type: stream.EventClosed}) // after close: ignored, must not panic
	if _, ok := <-ch; ok {
		t.Fatal("events must be closed")
	}
}

func TestConn_DecodeErrorsAreCountedAndPublished(t *testing.T) {
	e := newEnv(t)
	e.connect()
	for i := 0; i < 50; i++ {
		e.conn.ReportDecodeError(&stream.DecodeError{Channel: "/market/ticker:BTC-USDT", Err: errors.New("bad")})
	}
	if st := e.conn.Stats(); st.DecodeErrors != 50 {
		t.Fatalf("decode errors = %d", st.DecodeErrors)
	}
	eventually(t, func() bool { return e.evs.count(stream.EventDecodeError) > 0 }, "decode error event")
	ev, _ := e.evs.first(stream.EventDecodeError)
	if ev.Subscription != "/market/ticker:BTC-USDT" {
		t.Fatalf("event: %+v", ev)
	}
	if e.conn.State() != stream.StateConnected {
		t.Fatal("decode errors must not affect the connection")
	}
}

func TestConn_WritesAreSerialisedAcrossGoroutines(t *testing.T) {
	e := newEnv(t)
	e.connect()
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				sh, err := e.conn.Subscribe(ctx, specFor(fmt.Sprintf("w%d-%d", w, i), stream.HandlerFuncs{}))
				cancel()
				if err != nil {
					t.Errorf("Subscribe: %v", err)
					return
				}
				_ = sh.Close()
			}
		}(w)
	}
	wg.Wait()
	if e.beh.count(0, "subscribe", "") != 16*20 {
		t.Fatalf("server saw %d subscribe frames, want %d", e.beh.count(0, "subscribe", ""), 16*20)
	}
}

func TestConn_RestoreRestoresManySubscriptionsInParallel(t *testing.T) {
	e := newEnv(t)
	e.connect()
	const n = 40
	recs := make([]*recHandler, n)
	for i := range recs {
		recs[i] = newRec()
		e.subscribe(fmt.Sprintf("s%02d", i), recs[i])
	}
	e.srv.DropAll()
	e.waitReconnected(1)
	if got := e.beh.count(2, "subscribe", ""); got != n {
		t.Fatalf("restored %d subscriptions, want %d", got, n)
	}
	sc := e.serverConn(2)
	for i := range recs {
		recs[i].expect(t, "reset:")
		push(t, sc, fmt.Sprintf("s%02d", i), i)
		recs[i].expect(t, fmt.Sprintf("frame:s%02d:%d", i, i))
	}
}

func TestConn_StatsAndStateTransitions(t *testing.T) {
	e := newEnv(t)
	if st := e.conn.Stats(); st.State != stream.StateIdle || !st.ConnectedSince.IsZero() {
		t.Fatalf("idle stats: %+v", st)
	}
	e.connect()
	st := e.conn.Stats()
	if st.State != stream.StateConnected || st.ConnectedSince.IsZero() || st.Generation == 0 {
		t.Fatalf("connected stats: %+v", st)
	}
	if err := e.conn.Err(); err != nil {
		t.Fatalf("Err while running = %v", err)
	}
	sc := e.serverConn(1)
	h := newRec()
	e.subscribe("a", h)
	push(t, sc, "a", 1)
	h.expect(t, "frame:a:1")
	if st := e.conn.Stats(); st.FramesReceived < 3 || st.LastFrame.IsZero() { // welcome + ack + push
		t.Fatalf("frame counters: %+v", st)
	}
}

func TestConn_RouteDropsFramesOfSupersededSessions(t *testing.T) {
	c := New(&testProto{}, stream.Config{})
	h := newRec()
	live := &session{c: c, gen: 2}
	stale := &session{c: c, gen: 1}
	c.mu.Lock()
	c.cur = live
	sb := c.newSubLocked(specFor("a", h))
	c.mu.Unlock()
	frame := []byte(`{}`)
	c.route(stale, Inbound{Kind: KindPush, Route: "a", Msg: &tframe{N: 1}}, frame, time.Now())
	c.route(live, Inbound{Kind: KindPush, Route: "a", Msg: &tframe{N: 2}}, frame, time.Now())
	if it, ok := sb.mb.pop(); !ok || it.frame.Generation != 2 {
		t.Fatalf("expected only the live session's frame, got %+v ok=%v", it, ok)
	}
	if _, ok := sb.mb.pop(); ok {
		t.Fatal("the superseded session's frame must have been dropped")
	}
}

// chaosHandler checks the ordering contract of a Handler while the connection is
// being torn down around it: frames of one connection generation arrive in
// order, generations never go backwards, no frame of a superseded connection
// follows a reset marker, and nothing is called after OnClosed.
type chaosHandler struct {
	name string

	mu         sync.Mutex
	lastGen    uint64
	resetGen   uint64
	lastN      map[uint64]int
	frames     int
	resets     int
	aborted    bool
	closed     bool
	violations []string
}

func newChaosHandler(name string) *chaosHandler {
	return &chaosHandler{name: name, lastN: map[uint64]int{}}
}

// bad records a violation; the caller holds h.mu.
func (h *chaosHandler) bad(format string, args ...any) {
	if len(h.violations) < 8 {
		h.violations = append(h.violations, h.name+": "+fmt.Sprintf(format, args...))
	}
}

func (h *chaosHandler) OnFrame(f stream.Frame) {
	m := f.Msg.(*tframe)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		h.bad("frame after OnClosed")
	}
	if f.Generation < h.lastGen {
		h.bad("generation went back: %d after %d", f.Generation, h.lastGen)
	}
	h.lastGen = f.Generation
	if f.Generation < h.resetGen {
		h.bad("frame of generation %d after the reset to %d", f.Generation, h.resetGen)
	}
	if last := h.lastN[f.Generation]; m.N <= last {
		h.bad("generation %d: update %d after %d", f.Generation, m.N, last)
	}
	h.lastN[f.Generation] = m.N
	h.frames++
}

func (h *chaosHandler) OnReset(g uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		h.bad("reset after OnClosed")
	}
	if g <= h.resetGen {
		h.bad("reset to %d after %d", g, h.resetGen)
	}
	h.resetGen = g
	h.resets++
}

func (h *chaosHandler) OnGap(d uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if d == 0 {
		h.bad("empty gap")
	}
	if h.closed {
		h.bad("gap after OnClosed")
	}
}

func (h *chaosHandler) OnAbort(error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.aborted {
		h.bad("OnAbort twice")
	}
	h.aborted = true
}

func (h *chaosHandler) OnClosed() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		h.bad("OnClosed twice")
	}
	h.closed = true
}

func (h *chaosHandler) snapshot() (frames, resets int, closed bool, violations []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.frames, h.resets, h.closed, append([]string(nil), h.violations...)
}

// TestConn_ChaosConnectionStorm keeps a dozen subscriptions streaming while the
// "network" kills every connection at random moments, refuses some reconnects and
// other goroutines subscribe and unsubscribe concurrently. Whatever the
// interleaving, the ordering contract must hold, the connection must come back,
// every subscription must keep flowing and Close must end them all.
func TestConn_ChaosConnectionStorm(t *testing.T) {
	e := newEnv(t, func(c *stream.Config) { c.BufferSize = 64 })
	e.beh.onFrame = func(c *wstest.Conn, m map[string]any) {
		if wstest.Str(m, "type") != "subscribe" {
			return
		}
		route := wstest.Str(m, "route")
		go func() { // pushes until the connection is gone
			for n := 1; ; n++ {
				if c.Send(tframe{Type: "push", Route: route, N: n}) != nil {
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}
	e.connect()

	const resident = 12
	handlers := make([]*chaosHandler, resident)
	for i := range handlers {
		handlers[i] = newChaosHandler(fmt.Sprintf("resident-%d", i))
		e.subscribe(handlers[i].name, handlers[i])
	}

	var (
		stop    = make(chan struct{})
		wg      sync.WaitGroup
		churned atomic.Int64
		mu      sync.Mutex
		churn   []*chaosHandler
	)
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				h := newChaosHandler(fmt.Sprintf("churn-%d-%d", g, i))
				ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
				sh, err := e.conn.Subscribe(ctx, specFor(h.name, h))
				cancel()
				mu.Lock()
				churn = append(churn, h)
				mu.Unlock()
				if err != nil {
					continue // refused while the connection is down: expected
				}
				churned.Add(1)
				time.Sleep(time.Duration(i%4) * time.Millisecond)
				_ = sh.Close()
			}
		}(g)
	}

	// A violent phase that interrupts reconnects half-way through restoring the
	// subscriptions, then a calmer one in which they can complete.
	for _, phase := range []struct {
		length    time.Duration
		min, span int // milliseconds between two kills: min + (0..span)
	}{{800 * time.Millisecond, 15, 40}, {1500 * time.Millisecond, 220, 150}} {
		end := time.Now().Add(phase.length)
		for i := 0; time.Now().Before(end); i++ {
			time.Sleep(time.Duration(phase.min+(i*37)%phase.span) * time.Millisecond)
			if i%3 == 2 {
				e.srv.RejectUpgrades(1, 503)
			}
			e.srv.DropAll()
		}
	}
	close(stop)
	wg.Wait()

	eventually(t, func() bool { return e.conn.State() == stream.StateConnected }, "the connection to recover (state %v)", e.conn.State())
	before := make([]int, resident)
	for i, h := range handlers {
		before[i], _, _, _ = h.snapshot()
	}
	eventually(t, func() bool {
		for i, h := range handlers {
			if n, _, _, _ := h.snapshot(); n <= before[i] {
				return false
			}
		}
		return true
	}, "every resident subscription to flow again after the storm")

	if got := e.conn.Stats().Reconnects; got < 2 {
		t.Errorf("only %d reconnects completed; the storm was too gentle to prove anything", got)
	}
	if churned.Load() == 0 {
		t.Error("no concurrent subscribe succeeded during the storm")
	}

	if err := e.conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	mu.Lock()
	all := append(append([]*chaosHandler(nil), handlers...), churn...)
	mu.Unlock()
	for _, h := range all {
		_, _, closed, violations := h.snapshot()
		for _, v := range violations {
			t.Errorf("ordering violation: %s", v)
		}
		if !closed && strings.HasPrefix(h.name, "resident") {
			t.Errorf("%s was not closed by Close", h.name)
		}
	}
	for _, h := range handlers {
		if _, resets, _, _ := h.snapshot(); resets == 0 {
			t.Errorf("%s never saw a reset marker although the connection was killed repeatedly", h.name)
		}
	}
}
