package classic

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

	classicws "github.com/tigusigalpa/kucoin-go/classic/ws"
	"github.com/tigusigalpa/kucoin-go/internal/wsengine"
	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/stream"
)

func init() {
	// KuCoin drops connections that ping faster than once per second; tests
	// exercise the heartbeat at millisecond scale instead.
	wsengine.SetMinPingInterval(time.Millisecond)
}

// fakeKuCoin speaks just enough of KuCoin's Classic wire protocol.
type fakeKuCoin struct {
	mu        sync.Mutex
	frames    []map[string]any
	reject    map[string]int // topic -> error code
	ackless   bool
	onFrame   func(c *wstest.Conn, m map[string]any)
	ignorePng bool
}

func (f *fakeKuCoin) serve(c *wstest.Conn) {
	connectID := c.Request.URL.Query().Get("connectId")
	_ = c.Send(map[string]any{"id": connectID, "type": "welcome"})
	for {
		m, err := c.ReadJSON()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.frames = append(f.frames, m)
		hook, ignore, ackless := f.onFrame, f.ignorePng, f.ackless
		code := f.reject[wstest.Str(m, "topic")]
		f.mu.Unlock()
		if hook != nil {
			hook(c, m)
		}
		id := wstest.Str(m, "id")
		switch wstest.Str(m, "type") {
		case "ping":
			if !ignore {
				_ = c.Send(map[string]any{"id": id, "type": "pong", "timestamp": time.Now().UnixMicro()})
			}
		case "subscribe":
			switch {
			case code != 0:
				_ = c.Send(map[string]any{"id": id, "type": "error", "code": code, "data": "topic rejected"})
			case !ackless:
				_ = c.Send(map[string]any{"id": id, "type": "ack"})
			}
		case "unsubscribe":
			_ = c.Send(map[string]any{"id": id, "type": "ack"})
		}
	}
}

func (f *fakeKuCoin) count(typ, topic string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, m := range f.frames {
		if wstest.Str(m, "type") == typ && (topic == "" || wstest.Str(m, "topic") == topic) {
			n++
		}
	}
	return n
}

func (f *fakeKuCoin) last(typ string) map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.frames) - 1; i >= 0; i-- {
		if wstest.Str(f.frames[i], "type") == typ {
			return f.frames[i]
		}
	}
	return nil
}

func newFake(t *testing.T) (*fakeKuCoin, *wstest.Server) {
	t.Helper()
	wstest.CheckLeaks(t)
	f := &fakeKuCoin{reject: map[string]int{}}
	return f, wstest.NewServer(t, f.serve)
}

func fastReconnect() Option {
	return WithStreamOptions(stream.WithReconnect(stream.ReconnectPolicy{MinDelay: 5 * time.Millisecond, MaxDelay: 20 * time.Millisecond, Jitter: 0.1, StableAfter: time.Hour}),
		stream.WithPingInterval(time.Hour))
}

func connected(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
}

func pushTopic(t *testing.T, c *wstest.Conn, topic, subject string, data string) {
	t.Helper()
	if err := c.SendText(fmt.Sprintf(`{"type":"message","topic":%q,"subject":%q,"data":%s}`, topic, subject, data)); err != nil {
		t.Fatalf("push: %v", err)
	}
}

func recv(t *testing.T, ch <-chan Message) Message {
	t.Helper()
	select {
	case m, ok := <-ch:
		if !ok {
			t.Fatal("channel closed while waiting for a message")
		}
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a message")
		return Message{}
	}
}

func eventually(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestClient_ConnectSubscribePushUnsubscribe(t *testing.T) {
	fake, srv := newFake(t)
	client := NewClient(srv.URL(), "test-token", WithAutoReconnect(false))
	connected(t, client)

	msgs, err := client.Subscribe(context.Background(), "/market/ticker:BTC-USDT", false)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	pushTopic(t, srv.Conn(1), "/market/ticker:BTC-USDT", "trade.ticker", `{"price":"1.23"}`)
	m := recv(t, msgs)
	if m.Topic != "/market/ticker:BTC-USDT" || m.Subject != "trade.ticker" || string(m.Data) != `{"price":"1.23"}` {
		t.Fatalf("unexpected message: %+v", m)
	}

	if err := client.Unsubscribe("/market/ticker:BTC-USDT"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if _, ok := <-msgs; ok {
		t.Fatal("the channel must be closed after Unsubscribe")
	}
	if fake.count("unsubscribe", "/market/ticker:BTC-USDT") != 1 {
		t.Fatal("the server never saw the unsubscribe request")
	}
	if err := client.Unsubscribe("/never/subscribed"); err != nil {
		t.Fatalf("unsubscribing an unknown topic must be a no-op, got %v", err)
	}
}

func TestClient_SubscribeFrameCarriesPrivateFlagAndAcknowledgementRequest(t *testing.T) {
	fake, srv := newFake(t)
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false))
	connected(t, client)
	if _, err := client.Subscribe(context.Background(), "/contractMarket/tradeOrders", true); err != nil {
		t.Fatal(err)
	}
	frame := fake.last("subscribe")
	if frame["privateChannel"] != true || frame["response"] != true || frame["topic"] != "/contractMarket/tradeOrders" || wstest.Str(frame, "id") == "" {
		t.Fatalf("unexpected subscribe frame: %v", frame)
	}
}

func TestClient_SubscribingTheSameTopicTwiceReturnsTheSameChannel(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false))
	connected(t, client)
	a, err := client.Subscribe(context.Background(), "/market/ticker:BTC-USDT", false)
	if err != nil {
		t.Fatal(err)
	}
	b, err := client.Subscribe(context.Background(), "/market/ticker:BTC-USDT", false)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("the compatibility Subscribe must hand back the existing channel")
	}
	if err := client.Unsubscribe("/market/ticker:BTC-USDT"); err != nil {
		t.Fatal(err)
	}
	c, err := client.Subscribe(context.Background(), "/market/ticker:BTC-USDT", false)
	if err != nil {
		t.Fatal(err)
	}
	if c == a {
		t.Fatal("a fresh subscription after Unsubscribe must get a fresh channel")
	}
}

// Regression: the old client keyed subscriptions by the subscribed topic string,
// so a push for one symbol of a multi-symbol subscription was never delivered.
func TestClient_MultiSymbolSubscriptionRoutesEachSymbol(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false))
	connected(t, client)
	msgs, err := client.Subscribe(context.Background(), "/market/ticker:BTC-USDT,ETH-USDT", false)
	if err != nil {
		t.Fatal(err)
	}
	pushTopic(t, srv.Conn(1), "/market/ticker:ETH-USDT", "trade.ticker", `{"price":"2"}`)
	pushTopic(t, srv.Conn(1), "/market/ticker:BTC-USDT", "trade.ticker", `{"price":"1"}`)
	pushTopic(t, srv.Conn(1), "/market/ticker:XRP-USDT", "trade.ticker", `{"price":"9"}`) // not subscribed
	if m := recv(t, msgs); m.Topic != "/market/ticker:ETH-USDT" {
		t.Fatalf("first push topic = %s", m.Topic)
	}
	if m := recv(t, msgs); m.Topic != "/market/ticker:BTC-USDT" {
		t.Fatalf("second push topic = %s", m.Topic)
	}
	select {
	case m := <-msgs:
		t.Fatalf("unexpected push for an unsubscribed symbol: %+v", m)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestClient_AllSymbolsTopicRoutesBySubjectCarrier(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false))
	connected(t, client)
	msgs, err := client.Subscribe(context.Background(), "/market/ticker:all", false)
	if err != nil {
		t.Fatal(err)
	}
	pushTopic(t, srv.Conn(1), "/market/ticker:all", "BTC-USDT", `{"price":"1"}`)
	if m := recv(t, msgs); m.Subject != "BTC-USDT" {
		t.Fatalf("subject = %q", m.Subject)
	}
}

// Regression: the old client ignored error frames, so Subscribe hung until its
// 10s timeout and the failure was untyped.
func TestClient_SubscribeErrorFrameIsTypedAndFast(t *testing.T) {
	fake, srv := newFake(t)
	fake.reject["/nope:X"] = 404
	fake.reject["/spotMarket/tradeOrders"] = 403
	fake.reject["/many:A"] = 509
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false))
	connected(t, client)

	start := time.Now()
	_, err := client.Subscribe(context.Background(), "/nope:X", false)
	if time.Since(start) > time.Second {
		t.Fatalf("a rejected subscription took %v", time.Since(start))
	}
	var se *stream.ServerError
	if !errors.As(err, &se) || se.Code != 404 || !errors.Is(err, stream.ErrTopicNotFound) {
		t.Fatalf("error = %v, want a typed 404", err)
	}
	if _, err := client.Subscribe(context.Background(), "/spotMarket/tradeOrders", true); !errors.Is(err, stream.ErrLoginRequired) {
		t.Fatalf("error = %v, want ErrLoginRequired", err)
	}
	if client.State() != stream.StateConnected {
		t.Fatal("rejections must not drop the connection")
	}
}

func TestClient_SubscribeAckTimeout(t *testing.T) {
	fake, srv := newFake(t)
	fake.ackless = true
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false), WithStreamOptions(stream.WithAckTimeout(80*time.Millisecond)))
	connected(t, client)
	if _, err := client.Subscribe(context.Background(), "/market/ticker:BTC-USDT", false); !errors.Is(err, stream.ErrAckTimeout) {
		t.Fatalf("error = %v, want ErrAckTimeout", err)
	}
}

func TestClient_ConnectTimesOutWithoutWelcome(t *testing.T) {
	wstest.CheckLeaks(t)
	srv := wstest.NewServer(t, func(c *wstest.Conn) {
		for {
			if _, err := c.ReadText(); err != nil {
				return
			}
		}
	})
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false), WithStreamOptions(stream.WithConnectTimeout(150*time.Millisecond)))
	err := client.Connect(context.Background())
	if !errors.Is(err, stream.ErrWelcomeTimeout) {
		t.Fatalf("error = %v, want ErrWelcomeTimeout", err)
	}
	if client.State() != stream.StateIdle {
		t.Fatalf("state = %v", client.State())
	}
}

func TestClient_ContextCancelsConnect(t *testing.T) {
	wstest.CheckLeaks(t)
	srv := wstest.NewServer(t, func(c *wstest.Conn) {
		for {
			if _, err := c.ReadText(); err != nil {
				return
			}
		}
	})
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := client.Connect(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
}

func TestClient_CloseIsIdempotentAndClosesSubscriptionChannels(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false))
	connected(t, client)
	msgs, err := client.Subscribe(context.Background(), "/market/ticker:BTC-USDT", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	select {
	case _, ok := <-msgs:
		if ok {
			t.Fatal("subscription channel is still open after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription channel was not closed")
	}
	select {
	case <-client.Done():
	default:
		t.Fatal("Done must be closed after Close")
	}
}

func TestClient_ReconnectsAndResubscribes(t *testing.T) {
	fake, srv := newFake(t)
	client := NewClient(srv.URL(), "test-token", fastReconnect())
	connected(t, client)
	msgs, err := client.Subscribe(context.Background(), "/market/ticker:BTC-USDT", false)
	if err != nil {
		t.Fatal(err)
	}
	srv.DropAll()
	eventually(t, func() bool { return srv.Connections() == 2 && fake.count("subscribe", "") == 2 }, "resubscribe after an unexpected disconnect")
	eventually(t, func() bool { return client.State() == stream.StateConnected }, "connected state after reconnect")
	pushTopic(t, srv.Conn(2), "/market/ticker:BTC-USDT", "trade.ticker", `{"price":"7"}`)
	if m := recv(t, msgs); string(m.Data) != `{"price":"7"}` {
		t.Fatalf("data after reconnect: %s", m.Data)
	}
}

// Regression: a client built with a fixed token reused the (possibly expired)
// token and endpoint forever. With a TokenSource every attempt gets fresh ones.
func TestClient_TokenSourceIsAskedAgainOnEveryReconnect(t *testing.T) {
	_, srv := newFake(t)
	var calls atomic.Int32
	source := TokenSourceFunc(func(ctx context.Context) (*classicws.Token, error) {
		n := calls.Add(1)
		return &classicws.Token{
			Token: fmt.Sprintf("token-%d", n),
			InstanceServers: []classicws.InstanceServer{
				{Endpoint: srv.URL() + "/a", PingInterval: 3_600_000, PingTimeout: 10_000},
				{Endpoint: srv.URL() + "/b", PingInterval: 3_600_000, PingTimeout: 10_000},
			},
		}, nil
	})
	client := NewClientWithTokenSource(source, fastReconnect())
	connected(t, client)
	if _, err := client.Subscribe(context.Background(), "/market/ticker:BTC-USDT", false); err != nil {
		t.Fatal(err)
	}
	srv.DropAll()
	eventually(t, func() bool { return srv.Connections() == 2 && client.State() == stream.StateConnected }, "reconnected")

	first, second := srv.Conn(1).Request, srv.Conn(2).Request
	if got := first.URL.Query().Get("token"); got != "token-1" {
		t.Fatalf("first token = %q", got)
	}
	if got := second.URL.Query().Get("token"); got != "token-2" {
		t.Fatalf("reconnect reused a stale token: %q", got)
	}
	if first.URL.Path != "/a" || second.URL.Path != "/b" {
		t.Fatalf("instance servers are not rotated: %s then %s", first.URL.Path, second.URL.Path)
	}
	if first.URL.Query().Get("connectId") == "" || first.URL.Query().Get("connectId") == second.URL.Query().Get("connectId") {
		t.Fatal("every connection needs its own connectId")
	}
}

func TestClient_TokenSourceErrorsAreTypedAndPermanentOnesStopReconnecting(t *testing.T) {
	_, srv := newFake(t)
	cause := errors.New("credentials required")
	source := TokenSourceFunc(func(context.Context) (*classicws.Token, error) { return nil, stream.Permanent(cause) })
	client := NewClientWithTokenSource(source)
	err := client.Connect(context.Background())
	if !errors.Is(err, stream.ErrTokenUnavailable) || !errors.Is(err, cause) || !stream.IsPermanent(err) {
		t.Fatalf("error = %v", err)
	}
	_ = srv

	empty := NewClientWithTokenSource(TokenSourceFunc(func(context.Context) (*classicws.Token, error) {
		return &classicws.Token{Token: "t"}, nil // no instance servers
	}))
	if err := empty.Connect(context.Background()); !errors.Is(err, stream.ErrTokenUnavailable) {
		t.Fatalf("a token response without instance servers: %v", err)
	}
}

func TestClient_HeartbeatFollowsTheTokenResponse(t *testing.T) {
	fake, srv := newFake(t)
	source := TokenSourceFunc(func(context.Context) (*classicws.Token, error) {
		return &classicws.Token{Token: "t", InstanceServers: []classicws.InstanceServer{{Endpoint: srv.URL(), PingInterval: 80, PingTimeout: 60}}}, nil
	})
	// No ping override: the 80ms advertised by the token is honoured (sent at half).
	client := NewClientWithTokenSource(source, WithAutoReconnect(false), WithStreamOptions(stream.WithPingInterval(0)))
	connected(t, client)
	time.Sleep(300 * time.Millisecond)
	pings := fake.count("ping", "")
	if pings < 3 {
		t.Fatalf("only %d pings in 300ms; the token's pingInterval was ignored", pings)
	}
	if first := fake.last("ping"); wstest.Str(first, "id") == "" {
		t.Fatalf("ping frame without an id: %v", first)
	}
}

func TestClient_ConnectEscapesTokenAndPreservesEndpointQuery(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL()+"?region=eu", "a+b&c", WithAutoReconnect(false))
	connected(t, client)
	q := srv.Conn(1).Request.URL.Query()
	if q.Get("region") != "eu" || q.Get("token") != "a+b&c" || q.Get("connectId") == "" {
		t.Fatalf("unexpected query: %s", srv.Conn(1).Request.URL.RawQuery)
	}
}

func TestClient_InvalidEndpointIsPermanent(t *testing.T) {
	client := NewClient("://bad", "tok")
	err := client.Connect(context.Background())
	if !stream.IsPermanent(err) || !errors.Is(err, stream.ErrTokenUnavailable) {
		t.Fatalf("error = %v", err)
	}
	if err := NewClient("", "tok").Connect(context.Background()); err == nil {
		t.Fatal("an empty endpoint must fail")
	}
}

// Regression: closing a subscription channel while the reader was still sending
// to it was a data race / "send on closed channel" panic.
func TestClient_UnsubscribeRacingWithPushesIsSafe(t *testing.T) {
	_, srv := newFake(t)
	// 150 subscribe/unsubscribe cycles are 300 requests: far more than the pacing
	// that protects a real connection would let through in a test's lifetime.
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false), WithStreamOptions(stream.WithoutMessagePacing()))
	connected(t, client)
	sc := srv.Conn(1)
	stop := make(chan struct{})
	var flood sync.WaitGroup
	flood.Add(1)
	go func() {
		defer flood.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if sc.SendText(`{"type":"message","topic":"/market/ticker:BTC-USDT","subject":"s","data":{"p":"1"}}`) != nil {
				return
			}
			// A flood the reader can keep up with: an unthrottled one only measures how
			// long the acknowledgement of each unsubscribe waits behind the backlog in
			// the socket buffers, which on a small CI machine is minutes.
			time.Sleep(100 * time.Microsecond)
		}
	}()
	for i := 0; i < 150; i++ {
		ch, err := client.Subscribe(context.Background(), "/market/ticker:BTC-USDT", false)
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		go func() {
			for range ch {
			}
		}()
		time.Sleep(time.Millisecond)
		if err := client.Unsubscribe("/market/ticker:BTC-USDT"); err != nil {
			t.Fatalf("Unsubscribe: %v", err)
		}
	}
	close(stop)
	flood.Wait()
}

func TestClient_ConcurrentSubscribesAreSafe(t *testing.T) {
	fake, srv := newFake(t)
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false))
	connected(t, client)
	const n = 32
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := client.Subscribe(context.Background(), fmt.Sprintf("/market/ticker:SYM%d-USDT", i), false)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("Subscribe: %v", err)
		}
	}
	if fake.count("subscribe", "") != n {
		t.Fatalf("server saw %d subscribes, want %d", fake.count("subscribe", ""), n)
	}
}

// Subscribing one topic from many goroutines at once must give every caller the
// same channel and put a single subscribe frame on the wire; the engine allows one
// subscription per topic, so the losers of the race used to get ErrAlreadySubscribed.
func TestClient_ConcurrentSubscribesOfOneTopicShareOneSubscription(t *testing.T) {
	fake, srv := newFake(t)
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false))
	connected(t, client)
	const (
		n     = 32
		topic = "/market/ticker:BTC-USDT"
	)
	chans := make([]<-chan Message, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			chans[i], errs[i] = client.Subscribe(context.Background(), topic, false)
		}(i)
	}
	wg.Wait()
	for i := range chans {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if chans[i] != chans[0] {
			t.Fatalf("caller %d got a different channel", i)
		}
	}
	if got := fake.count("subscribe", topic); got != 1 {
		t.Fatalf("server saw %d subscribe frames, want 1", got)
	}
	pushTopic(t, srv.Conn(1), topic, "s", `{"p":"1"}`)
	if m := recv(t, chans[0]); m.Topic != topic {
		t.Fatalf("message: %+v", m)
	}
	// After Unsubscribe the topic can be subscribed again, concurrently too.
	if err := client.Unsubscribe(topic); err != nil {
		t.Fatal(err)
	}
	again, err := client.Subscribe(context.Background(), topic, false)
	if err != nil || again == chans[0] {
		t.Fatalf("Subscribe after Unsubscribe = %v, %v (must be a new channel)", again, err)
	}
}

func TestClient_ConcurrentSubscribesToARejectedTopicAllGetTheRejection(t *testing.T) {
	fake, srv := newFake(t)
	fake.mu.Lock()
	fake.reject["/market/ticker:NOPE-USDT"] = 404
	fake.mu.Unlock()
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false))
	connected(t, client)
	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = client.Subscribe(context.Background(), "/market/ticker:NOPE-USDT", false)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if !errors.Is(err, stream.ErrTopicNotFound) {
			t.Fatalf("caller %d: %v, want the typed rejection", i, err)
		}
	}
	// The failure must not be remembered: once KuCoin accepts the topic it works.
	fake.mu.Lock()
	delete(fake.reject, "/market/ticker:NOPE-USDT")
	fake.mu.Unlock()
	if _, err := client.Subscribe(context.Background(), "/market/ticker:NOPE-USDT", false); err != nil {
		t.Fatalf("Subscribe after the rejection was lifted: %v", err)
	}
}

// KuCoin allows a Classic connection 100 client messages per 10 seconds; the
// engine paces its requests by what the protocol declares.
func TestProtocolDeclaresTheClassicMessageLimit(t *testing.T) {
	var p wsengine.Protocol = &protocol{}
	limiter, ok := p.(wsengine.MessageLimiter)
	if !ok {
		t.Fatal("the Classic protocol must declare its message limit")
	}
	if n, window := limiter.MessageLimit(); n != 100 || window != 10*time.Second {
		t.Fatalf("limit = %d per %v, want 100 per 10s", n, window)
	}
}

func TestSubscribeTyped_DecodesSkipsAndReportsErrors(t *testing.T) {
	_, srv := newFake(t)
	events := make(chan stream.Event, 16)
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false), WithStreamOptions(stream.WithEventHandler(func(ev stream.Event) { events <- ev })))
	connected(t, client)

	type ticker struct{ Price string }
	sub, err := SubscribeTyped(context.Background(), client, "/market/ticker:BTC-USDT", false, func(m *Message) (ticker, bool, error) {
		if m.Subject == "skip" {
			return ticker{}, false, nil
		}
		var v ticker
		if err := json.Unmarshal(m.Data, &v); err != nil {
			return ticker{}, false, err
		}
		return v, true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sc := srv.Conn(1)
	pushTopic(t, sc, "/market/ticker:BTC-USDT", "a", `{"price":"1"}`)
	pushTopic(t, sc, "/market/ticker:BTC-USDT", "skip", `{"price":"x"}`)
	pushTopic(t, sc, "/market/ticker:BTC-USDT", "a", `"not an object"`) // malformed: reported, stream continues
	pushTopic(t, sc, "/market/ticker:BTC-USDT", "a", `{"price":"2"}`)

	for _, want := range []string{"1", "2"} {
		select {
		case v := <-sub.C():
			if v.Price != want {
				t.Fatalf("price = %s, want %s", v.Price, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out")
		}
	}
	var sawDecodeError bool
	deadline := time.After(2 * time.Second)
	for !sawDecodeError {
		select {
		case ev := <-events:
			var de *stream.DecodeError
			if ev.Type == stream.EventDecodeError && errors.As(ev.Err, &de) && de.Channel == "/market/ticker:BTC-USDT" && len(de.Raw) > 0 {
				sawDecodeError = true
			}
		case <-deadline:
			t.Fatal("no decode error event")
		}
	}
	if client.Stats().DecodeErrors != 1 {
		t.Fatalf("decode errors = %d", client.Stats().DecodeErrors)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-sub.C(); ok {
		t.Fatal("C must close after Close")
	}
	if sub.Err() != nil {
		t.Fatalf("Err after a requested Close = %v", sub.Err())
	}
}

func TestSubscribeTyped_SlowConsumerDropsOldestAndCounts(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL(), "tok", WithAutoReconnect(false))
	connected(t, client)
	sub, err := SubscribeTyped(context.Background(), client, "/market/ticker:BTC-USDT", false,
		func(m *Message) (int, bool, error) {
			var v struct{ N int }
			err := json.Unmarshal(m.Data, &v)
			return v.N, true, err
		}, stream.WithBuffer(4))
	if err != nil {
		t.Fatal(err)
	}
	sc := srv.Conn(1)
	for i := 1; i <= 100; i++ {
		pushTopic(t, sc, "/market/ticker:BTC-USDT", "s", fmt.Sprintf(`{"n":%d}`, i))
	}
	eventually(t, func() bool { return client.Stats().FramesReceived >= 102 }, "all frames read by the socket reader")
	var got []int
	for {
		select {
		case v := <-sub.C():
			got = append(got, v)
			continue
		case <-time.After(300 * time.Millisecond):
		}
		break
	}
	if len(got) == 0 || got[len(got)-1] != 100 {
		t.Fatalf("the newest update must survive; got %v", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("delivery order broken: %v", got)
		}
	}
	if sub.Dropped() == 0 || sub.Dropped()+uint64(len(got)) != 100 {
		t.Fatalf("dropped=%d delivered=%d, want them to add up to 100", sub.Dropped(), len(got))
	}
}

func TestSubscribeHandler_ReceivesResetOnReconnect(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL(), "tok", fastReconnect())
	connected(t, client)
	log := make(chan string, 16)
	h, err := client.SubscribeHandler(context.Background(), "/contractMarket/level2:XBTUSDTM", false, stream.HandlerFuncs{
		Frame: func(f stream.Frame) {
			m := f.Msg.(*Message)
			log <- fmt.Sprintf("frame:%d", m.Sn)
		},
		Reset:  func(g uint64) { log <- "reset" },
		Abort:  func(err error) { log <- fmt.Sprintf("abort:%v", err) },
		Closed: func() { log <- "closed" },
	})
	if err != nil {
		t.Fatal(err)
	}
	if h.Topic() != "/contractMarket/level2:XBTUSDTM" {
		t.Fatalf("topic = %s", h.Topic())
	}
	if err := srv.Conn(1).SendText(`{"type":"message","topic":"/contractMarket/level2:XBTUSDTM","subject":"level2","sn":1748099963793,"data":{"sequence":1748099963793,"change":"84497.5,buy,2"}}`); err != nil {
		t.Fatal(err)
	}
	expect := func(want string) {
		t.Helper()
		select {
		case got := <-log:
			if got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %q", want)
		}
	}
	expect("frame:1748099963793")
	srv.DropAll()
	expect("reset")
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	expect("abort:<nil>")
	expect("closed")
	if _, err := client.SubscribeHandler(context.Background(), "x", false, nil); err == nil {
		t.Fatal("a nil handler must be rejected")
	}
}

func TestClient_StateEventsAndStats(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL(), "tok", fastReconnect())
	if client.State() != stream.StateIdle {
		t.Fatalf("state = %v", client.State())
	}
	connected(t, client)
	if client.State() != stream.StateConnected || client.Err() != nil {
		t.Fatalf("state=%v err=%v", client.State(), client.Err())
	}
	var types []stream.EventType
	timeout := time.After(5 * time.Second)
	srv.DropAll()
	for len(types) < 4 {
		select {
		case ev := <-client.Events():
			types = append(types, ev.Type)
		case <-timeout:
			t.Fatalf("events so far: %v", types)
		}
	}
	want := []stream.EventType{stream.EventConnected, stream.EventDisconnected, stream.EventReconnecting, stream.EventReconnected}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("events = %v, want %v", types, want)
		}
	}
	if st := client.Stats(); st.Reconnects != 1 || st.FramesReceived == 0 {
		t.Fatalf("stats = %+v", st)
	}
	if !strings.Contains(client.String(), "connected") {
		t.Fatalf("String() = %s", client.String())
	}
	if err := client.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRoutesFor(t *testing.T) {
	tests := map[string][]string{
		"/market/ticker:BTC-USDT":                 {"/market/ticker:BTC-USDT"},
		"/market/ticker:BTC-USDT,ETH-USDT":        {"/market/ticker:BTC-USDT", "/market/ticker:ETH-USDT", "/market/ticker:BTC-USDT,ETH-USDT"},
		"/market/ticker:all":                      {"/market/ticker:all"},
		"/contractMarket/tradeOrders":             {"/contractMarket/tradeOrders"},
		"/contractMarket/limitCandle:A_1min,B_1h": {"/contractMarket/limitCandle:A_1min", "/contractMarket/limitCandle:B_1h", "/contractMarket/limitCandle:A_1min,B_1h"},
		"/market/ticker: BTC-USDT , ":             {"/market/ticker:BTC-USDT", "/market/ticker: BTC-USDT , "},
		"/market/ticker:":                         {"/market/ticker:"},
	}
	for topic, want := range tests {
		got := routesFor(topic)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("routesFor(%q) = %q, want %q", topic, got, want)
		}
	}
}

func TestClassify(t *testing.T) {
	p := &protocol{}
	tests := []struct {
		name string
		raw  string
		kind string
	}{
		{"welcome", `{"id":"abc","type":"welcome"}`, "welcome"},
		{"pong", `{"id":"1545910590801","type":"pong","timestamp":1764215232226553}`, "pong"},
		{"ack with numeric id", `{"id":1545910660739,"type":"ack"}`, "ack"},
		{"error", `{"id":"x","type":"error","code":404,"data":"topic does not exist"}`, "error"},
		{"error with string code", `{"id":"x","type":"error","code":"509","data":"exceed max permits per second"}`, "error"},
		{"push", `{"type":"message","topic":"/a:B","subject":"s","sn":5,"data":{"x":1}}`, "push"},
		{"push with numeric user id", `{"type":"message","topic":"/a:B","userId":12345,"channelType":"private","data":{}}`, "push"},
		{"garbage", `not json`, "unknown"},
		{"no topic", `{"type":"message"}`, "unknown"},
	}
	for _, tt := range tests {
		in := p.Classify([]byte(tt.raw))
		got := map[int]string{0: "unknown", 1: "welcome", 2: "pong", 3: "ack", 4: "nack", 5: "push", 6: "error"}[int(in.Kind)]
		if got != tt.kind {
			t.Errorf("%s: kind = %s, want %s", tt.name, got, tt.kind)
		}
	}
	in := p.Classify([]byte(`{"id":"x","type":"error","code":509,"data":"exceed max permits per second"}`))
	var se *stream.ServerError
	if !errors.As(in.Err, &se) || se.Code != 509 || !errors.Is(in.Err, stream.ErrRateLimited) || in.ID != "x" {
		t.Fatalf("error frame: %+v", in)
	}
	push := p.Classify([]byte(`{"type":"message","topic":"/contractMarket/level2:XBTUSDTM","subject":"level2","sn":42,"data":{"sequence":42}}`))
	msg, ok := push.Msg.(*Message)
	if !ok || msg.Sn != 42 || msg.Topic != "/contractMarket/level2:XBTUSDTM" || push.Route != msg.Topic {
		t.Fatalf("push: %+v", push)
	}
}
