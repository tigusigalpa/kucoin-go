package stream

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestNewHandler_DecodesDeliversSkipsAndReports(t *testing.T) {
	sub := NewSubscription[int]("k", 8, nil)
	var reported []error
	decode := func(f Frame) (int, bool, error) {
		switch f.Route {
		case "bad":
			return 0, false, errors.New("malformed")
		case "skip":
			return 0, false, nil
		}
		return len(f.Raw), true, nil
	}
	h := NewHandler(sub, decode, func(err error) { reported = append(reported, err) })

	h.OnFrame(Frame{Route: "a", Raw: []byte("xyz")})
	h.OnFrame(Frame{Route: "skip"})
	h.OnFrame(Frame{Route: "bad", Raw: []byte("raw")})
	h.OnReset(7) // a plain subscription has nothing to do on a reset
	h.OnGap(5)   // lost frames are counted

	if got := <-sub.C(); got != 3 {
		t.Fatalf("delivered %d, want 3", got)
	}
	select {
	case v := <-sub.C():
		t.Fatalf("a skipped or malformed frame was delivered: %d", v)
	default:
	}
	var de *DecodeError
	if len(reported) != 1 || !errors.As(reported[0], &de) || de.Channel != "bad" || string(de.Raw) != "raw" {
		t.Fatalf("reported = %v, want one *DecodeError for the bad frame", reported)
	}
	if got := sub.Dropped(); got != 5 {
		t.Fatalf("Dropped = %d, want 5", got)
	}

	// Without a report function a malformed frame is simply skipped.
	NewHandler(sub, decode, nil).OnFrame(Frame{Route: "bad"})

	boom := errors.New("boom")
	h.OnAbort(boom)
	select {
	case <-sub.Done():
	default:
		t.Fatal("OnAbort must end the subscription")
	}
	if !errors.Is(sub.Err(), boom) {
		t.Fatalf("Err = %v, want the abort cause", sub.Err())
	}
	h.OnClosed()
	if _, ok := <-sub.C(); ok {
		t.Fatal("C must be closed after OnClosed")
	}
}

func TestSubscription_BindDroppedReportsTheLiveCounter(t *testing.T) {
	sub := NewSubscription[int]("k", 0, nil)
	sub.AddDropped(2)
	if got := sub.Dropped(); got != 2 {
		t.Fatalf("Dropped = %d, want the accumulated 2", got)
	}
	live := uint64(9)
	sub.BindDropped(func() uint64 { return live })
	if got := sub.Dropped(); got != 9 {
		t.Fatalf("Dropped = %d, want the live counter 9", got)
	}
	live = 11
	if got := sub.Dropped(); got != 11 {
		t.Fatalf("Dropped = %d, want the live counter 11", got)
	}
	sub.AddDropped(100) // the live counter is the source of truth once bound
	if got := sub.Dropped(); got != 11 {
		t.Fatalf("Dropped = %d after AddDropped on a bound subscription, want 11", got)
	}
}

func TestConfig_EveryOptionSetsItsField(t *testing.T) {
	dialer := &websocket.Dialer{}
	header := http.Header{"X-Test": {"1"}}
	var events int
	logger := NopLogger{}

	cfg := NewConfig(
		WithLogger(logger),
		WithConnectTimeout(time.Second),
		WithWriteTimeout(2*time.Second),
		WithReadLimit(1234),
		WithPingTimeout(3*time.Second),
		WithEventHandler(func(Event) { events++ }),
		WithDialer(dialer),
		WithHeader(header),
		WithReconnect(ReconnectPolicy{MinDelay: time.Millisecond}),
	)
	if cfg.Logger != Logger(logger) || cfg.ConnectTimeout != time.Second || cfg.WriteTimeout != 2*time.Second || cfg.ReadLimit != 1234 ||
		cfg.PingTimeout != 3*time.Second || cfg.Dialer != dialer || cfg.Header.Get("X-Test") != "1" || cfg.Reconnect.MinDelay != time.Millisecond {
		t.Fatalf("options not applied: %+v", cfg)
	}
	if cfg.OnEvent == nil {
		t.Fatal("WithEventHandler did not set the handler")
	}
	cfg.OnEvent(Event{})
	if events != 1 {
		t.Fatal("the configured handler is not the one given")
	}
}
