package stream

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestServerError_MapsToSentinels(t *testing.T) {
	tests := []struct {
		name string
		err  *ServerError
		want error
	}{
		{"topic invalid", &ServerError{Code: 400, Message: "topic is invalid"}, ErrTopicInvalid},
		{"ping timeout", &ServerError{Code: 400, Message: "ping timeout"}, ErrPingTimeout},
		{"token invalid", &ServerError{Code: 401, Message: "token is invalid"}, ErrTokenInvalid},
		{"login required", &ServerError{Code: 403, Message: "login is required"}, ErrLoginRequired},
		{"topic not found", &ServerError{Code: 404, Message: "topic does not exist"}, ErrTopicNotFound},
		{"topic required", &ServerError{Code: 406, Message: "topic is required"}, ErrTopicRequired},
		{"bad command", &ServerError{Code: 415, Message: "command type is invalid"}, ErrBadCommand},
		{"subscription limit", &ServerError{Code: 509, Message: "exceed max subscription count limitation of 100 per time"}, ErrSubscriptionLimit},
		{"session count", &ServerError{Code: 509, Message: "exceed max session count limitation of 50"}, ErrSessionLimit},
		{"permits", &ServerError{Code: 509, Message: "exceed max permits per second"}, ErrRateLimited},
		{"busy", &ServerError{Code: 509, Message: "service busy, please retry later"}, ErrServiceBusy},
		{"uta rate limit", &ServerError{Code: 429002, Message: "Too many requests"}, ErrRateLimited},
		{"uta busy", &ServerError{Code: 503000, Message: "Server is busy"}, ErrServiceBusy},
		{"uta auth", &ServerError{Code: 400005, Message: "Invalid KC-API-SIGN"}, ErrAuthFailed},
		{"auth without code", &ServerError{Message: "auth failed"}, ErrAuthFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapped := fmt.Errorf("subscribe: %w", tt.err)
			if !errors.Is(wrapped, tt.want) {
				t.Fatalf("errors.Is(%v, %v) = false", wrapped, tt.want)
			}
			var se *ServerError
			if !errors.As(wrapped, &se) || se.Code != tt.err.Code {
				t.Fatalf("errors.As lost the *ServerError: %v", se)
			}
		})
	}
	if errors.Is(&ServerError{Code: 404}, ErrTopicInvalid) {
		t.Fatal("404 must not match ErrTopicInvalid")
	}
	if errors.Is(&ServerError{Code: 509, Message: "something else"}, ErrSubscriptionLimit) {
		t.Fatal("an unrecognised 509 must not match a specific sentinel")
	}
	var nilErr *ServerError
	if nilErr.Is(ErrTopicNotFound) {
		t.Fatal("nil receiver must not match")
	}
}

func TestServerError_Message(t *testing.T) {
	for _, tt := range []struct {
		err  *ServerError
		want string
	}{
		{&ServerError{Code: 404, Message: "topic does not exist"}, "kucoin: ws: server error 404: topic does not exist"},
		{&ServerError{Code: 404}, "kucoin: ws: server error 404"},
		{&ServerError{Message: "invalid request data"}, "kucoin: ws: server error: invalid request data"},
		{&ServerError{}, "kucoin: ws: server error"},
	} {
		if got := tt.err.Error(); got != tt.want {
			t.Errorf("got %q, want %q", got, tt.want)
		}
	}
}

func TestPermanent(t *testing.T) {
	base := errors.New("bad key")
	p := Permanent(base)
	if !IsPermanent(p) || !errors.Is(p, base) {
		t.Fatalf("Permanent must be detectable and keep the cause: %v", p)
	}
	if IsPermanent(base) || IsPermanent(nil) {
		t.Fatal("plain and nil errors are not permanent")
	}
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
	if !IsPermanent(fmt.Errorf("ctx: %w", p)) {
		t.Fatal("permanence must survive wrapping")
	}
}

func TestDecodeError(t *testing.T) {
	cause := errors.New("boom")
	err := &DecodeError{Channel: "/market/ticker:BTC-USDT", Err: cause}
	if !errors.Is(err, cause) {
		t.Fatal("DecodeError must unwrap to its cause")
	}
	if got := err.Error(); got != "kucoin: ws: decode /market/ticker:BTC-USDT push: boom" {
		t.Fatalf("unexpected message %q", got)
	}
}

func TestStateAndEventStrings(t *testing.T) {
	states := map[State]string{StateIdle: "idle", StateConnecting: "connecting", StateConnected: "connected", StateReconnecting: "reconnecting", StateClosing: "closing", StateClosed: "closed", State(99): "unknown"}
	for s, want := range states {
		if s.String() != want {
			t.Errorf("State(%d) = %q, want %q", s, s.String(), want)
		}
	}
	events := map[EventType]string{EventConnected: "connected", EventDisconnected: "disconnected", EventReconnecting: "reconnecting", EventReconnected: "reconnected", EventSubscriptionFailed: "subscription_failed", EventOverflow: "overflow", EventDecodeError: "decode_error", EventServerError: "server_error", EventClosed: "closed", EventType(0): "unknown"}
	for e, want := range events {
		if e.String() != want {
			t.Errorf("EventType(%d) = %q, want %q", e, e.String(), want)
		}
	}
	for p, want := range map[OverflowPolicy]string{DropOldest: "drop_oldest", DropNewest: "drop_newest", FailSubscription: "fail_subscription", OverflowPolicy(9): "unknown"} {
		if p.String() != want {
			t.Errorf("OverflowPolicy(%d) = %q, want %q", p, p.String(), want)
		}
	}
}

func TestConfig_DefaultsAndOptions(t *testing.T) {
	cfg := NewConfig()
	if cfg.Logger == nil || cfg.Dialer == nil {
		t.Fatal("logger and dialer must default to non-nil")
	}
	if cfg.Reconnect.Disabled || cfg.Reconnect.MinDelay != 500*time.Millisecond || cfg.Reconnect.MaxDelay != time.Minute || cfg.Reconnect.Factor != 2 || cfg.Reconnect.Jitter != 0.5 || cfg.Reconnect.StableAfter != 30*time.Second {
		t.Fatalf("unexpected reconnect defaults: %+v", cfg.Reconnect)
	}
	if cfg.ConnectTimeout != 15*time.Second || cfg.AckTimeout != 10*time.Second || cfg.WriteTimeout != 10*time.Second || cfg.ReadLimit != 8<<20 || cfg.BufferSize != 1024 || cfg.EventBuffer != 128 || cfg.InitialConnectAttempts != 1 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.Overflow != DropOldest {
		t.Fatalf("default overflow policy = %v", cfg.Overflow)
	}

	custom := NewConfig(
		WithAutoReconnect(false),
		WithBufferSize(8),
		WithOverflowPolicy(FailSubscription),
		WithAckTimeout(time.Second),
		WithPingInterval(5*time.Second),
		WithInitialConnectAttempts(4),
		nil, // nil options are ignored
	)
	if !custom.Reconnect.Disabled || custom.BufferSize != 8 || custom.Overflow != FailSubscription || custom.AckTimeout != time.Second || custom.PingInterval != 5*time.Second || custom.InitialConnectAttempts != 4 {
		t.Fatalf("options not applied: %+v", custom)
	}

	odd := NewConfig(WithReconnect(ReconnectPolicy{MinDelay: 3 * time.Second, MaxDelay: time.Second, Factor: 0.5, Jitter: 7}))
	if odd.Reconnect.MaxDelay != 3*time.Second || odd.Reconnect.Factor != 2 || odd.Reconnect.Jitter != 0.5 {
		t.Fatalf("invalid reconnect values must be normalised: %+v", odd.Reconnect)
	}
	if cfg := NewConfig(WithConfig(Config{BufferSize: 3}), WithEventBuffer(5)); cfg.BufferSize != 3 || cfg.EventBuffer != 5 {
		t.Fatalf("WithConfig / later option ordering broken: %+v", cfg)
	}

	sc := NewSubscribeConfig(WithBuffer(4), WithOverflow(DropOldest), nil)
	if sc.Buffer != 4 || !sc.OverflowSet || sc.Overflow != DropOldest {
		t.Fatalf("subscribe options not applied: %+v", sc)
	}
	if NewSubscribeConfig().OverflowSet {
		t.Fatal("OverflowSet must be false by default")
	}
}

func TestConfig_MessageLimitOptions(t *testing.T) {
	if got := NewConfig().MessageLimit; got != (MessageLimit{}) {
		t.Fatalf("the default must defer to the protocol's published limit, got %+v", got)
	}
	if got := NewConfig(WithMessageLimit(50, 5*time.Second)).MessageLimit; got != (MessageLimit{Messages: 50, Window: 5 * time.Second}) {
		t.Fatalf("WithMessageLimit: %+v", got)
	}
	if got := NewConfig(WithoutMessagePacing()).MessageLimit; !got.Unlimited {
		t.Fatalf("WithoutMessagePacing: %+v", got)
	}
	// The last option wins, and a new limit clears an earlier "unlimited".
	if got := NewConfig(WithoutMessagePacing(), WithMessageLimit(7, time.Second)).MessageLimit; got.Unlimited || got.Messages != 7 {
		t.Fatalf("later option must replace the earlier one: %+v", got)
	}
}

func TestNopLoggerAndHandlerFuncs(t *testing.T) {
	var l Logger = NopLogger{}
	l.Debug("a")
	l.Info("a")
	l.Warn("a")
	l.Error("a")

	var h Handler = HandlerFuncs{} // all nil: every call must be a harmless no-op
	h.OnFrame(Frame{})
	h.OnReset(1)
	h.OnGap(1)
	h.OnAbort(nil)
	h.OnClosed()

	var got []string
	h = HandlerFuncs{
		Frame:  func(f Frame) { got = append(got, "frame:"+f.Route) },
		Reset:  func(g uint64) { got = append(got, fmt.Sprintf("reset:%d", g)) },
		Gap:    func(d uint64) { got = append(got, fmt.Sprintf("gap:%d", d)) },
		Abort:  func(err error) { got = append(got, fmt.Sprintf("abort:%v", err)) },
		Closed: func() { got = append(got, "closed") },
	}
	h.OnFrame(Frame{Route: "r"})
	h.OnReset(2)
	h.OnGap(3)
	h.OnAbort(nil)
	h.OnClosed()
	want := []string{"frame:r", "reset:2", "gap:3", "abort:<nil>", "closed"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestSubscription_DeliverAndSeal(t *testing.T) {
	s := NewSubscription[int]("k", 0, nil)
	if s.Key() != "k" || s.Err() != nil || s.Dropped() != 0 {
		t.Fatal("unexpected initial state")
	}
	got := make(chan []int, 1)
	go func() {
		var vals []int
		for v := range s.C() {
			vals = append(vals, v)
		}
		got <- vals
	}()
	for i := 1; i <= 3; i++ {
		if !s.Deliver(i) {
			t.Fatalf("Deliver(%d) = false", i)
		}
	}
	s.AddDropped(2)
	s.Seal()
	vals := <-got
	if fmt.Sprint(vals) != "[1 2 3]" {
		t.Fatalf("got %v", vals)
	}
	if s.Dropped() != 2 {
		t.Fatalf("Dropped = %d", s.Dropped())
	}
	select {
	case <-s.Done():
	default:
		t.Fatal("Done must be closed after Seal")
	}
	if s.Err() != nil {
		t.Fatalf("a clean end has no error, got %v", s.Err())
	}
	s.Seal() // second Seal is a no-op, not a panic
}

func TestSubscription_FinishUnblocksDeliverAndFirstErrorWins(t *testing.T) {
	s := NewSubscription[string]("k", 0, nil)
	blocked := make(chan bool, 1)
	go func() { blocked <- s.Deliver("x") }() // nobody receives: blocks until Finish
	time.Sleep(20 * time.Millisecond)
	first := errors.New("first")
	s.Finish(first)
	s.Finish(errors.New("second"))
	if ok := <-blocked; ok {
		t.Fatal("Deliver must report false when the subscription ended")
	}
	if !errors.Is(s.Err(), first) {
		t.Fatalf("Err = %v, want the first error", s.Err())
	}
	if s.Deliver("y") {
		t.Fatal("Deliver after Finish must fail")
	}
	s.Seal()
	if _, ok := <-s.C(); ok {
		t.Fatal("C must be closed after Seal")
	}
}

func TestSubscription_DeliverWithin(t *testing.T) {
	s := NewSubscription[int]("k", 0, nil)

	// A receiver is waiting: delivered at once.
	got := make(chan int, 1)
	go func() { got <- <-s.C() }()
	time.Sleep(10 * time.Millisecond)
	if !s.DeliverWithin(7, time.Second) || <-got != 7 {
		t.Fatal("a waiting consumer must receive the value")
	}

	// Nobody receives: gives up after the deadline without ending the subscription.
	start := time.Now()
	if s.DeliverWithin(8, 30*time.Millisecond) {
		t.Fatal("nobody received the value")
	}
	if d := time.Since(start); d < 25*time.Millisecond || d > 2*time.Second {
		t.Fatalf("gave up after %v", d)
	}
	select {
	case <-s.Done():
		t.Fatal("a missed delivery must not end the subscription")
	default:
	}

	// Finish releases a pending call at once, and later calls fail immediately.
	pending := make(chan bool, 1)
	go func() { pending <- s.DeliverWithin(9, time.Minute) }()
	time.Sleep(10 * time.Millisecond)
	s.Finish(nil)
	select {
	case ok := <-pending:
		if ok {
			t.Fatal("Finish must release the pending delivery with false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Finish did not release DeliverWithin")
	}
	if s.DeliverWithin(10, time.Minute) {
		t.Fatal("DeliverWithin after Finish must fail immediately")
	}
	s.Seal()
}

func TestSubscription_CloseIsIdempotentAndCallsCloseFnOnce(t *testing.T) {
	calls := 0
	boom := errors.New("unsubscribe failed")
	s := NewSubscription[int]("k", 2, func() error { calls++; return boom })
	if err := s.Close(); !errors.Is(err, boom) {
		t.Fatalf("Close = %v, want the closeFn error", err)
	}
	if err := s.Close(); !errors.Is(err, boom) {
		t.Fatalf("second Close = %v", err)
	}
	if calls != 1 {
		t.Fatalf("closeFn ran %d times", calls)
	}
	select {
	case <-s.Done():
	default:
		t.Fatal("Close must finish the subscription")
	}
	if s.Err() != nil {
		t.Fatalf("a requested Close is not an error, got %v", s.Err())
	}
	if NewSubscription[int]("n", -5, nil).C() == nil {
		t.Fatal("negative buffer must be treated as zero")
	}
	if err := NewSubscription[int]("n", 0, nil).Close(); err != nil {
		t.Fatalf("Close without closeFn = %v", err)
	}
}

func TestSubscription_ConcurrentUseIsRaceFree(t *testing.T) {
	s := NewSubscription[int]("k", 4, nil)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // the single producer
		defer wg.Done()
		defer s.Seal()
		for i := 0; ; i++ {
			if !s.Deliver(i) {
				return
			}
		}
	}()
	received := 0
	for v := range s.C() {
		_ = v
		received++
		if received == 100 {
			go s.Close() // concurrent Close from another goroutine
		}
		_ = s.Err()
		_ = s.Dropped()
	}
	wg.Wait()
	if received < 100 {
		t.Fatalf("received only %d values", received)
	}
}
