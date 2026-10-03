package uta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/auth"
	"github.com/tigusigalpa/kucoin-go/internal/wsengine"
	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
)

func init() {
	// KuCoin drops connections that ping faster than once per second; tests
	// exercise the heartbeat at millisecond scale instead.
	wsengine.SetMinPingInterval(time.Millisecond)
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

// fakeUTA speaks just enough of KuCoin's UTA v2 wire protocol.
type fakeUTA struct {
	mu        sync.Mutex
	frames    []map[string]any
	nack      map[string]string // channel -> failure field ("message" or "reason")
	onAuth    func(c *wstest.Conn, m map[string]any) (reply any)
	ackString bool
	noAck     bool
	pingMs    int
	pings     int
}

func (f *fakeUTA) serve(c *wstest.Conn) {
	f.mu.Lock()
	ms := f.pingMs
	f.mu.Unlock()
	if ms == 0 {
		ms = 3_600_000
	}
	_ = c.Send(map[string]any{"sessionId": "sess-" + fmt.Sprint(c.Index), "message": "welcome", "pingInterval": ms})
	for {
		m, err := c.ReadJSON()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.frames = append(f.frames, m)
		onAuth, ackString, noAck := f.onAuth, f.ackString, f.noAck
		field, nacked := f.nack[wstest.Str(m, "channel")]
		f.mu.Unlock()
		id := m["id"]
		switch {
		case m["op"] == "ping":
			f.mu.Lock()
			f.pings++
			f.mu.Unlock()
			_ = c.Send(map[string]any{"id": id, "op": "pong", "timestamp": time.Now().UnixMilli()})
		case m["op"] == "auth":
			if onAuth != nil {
				if reply := onAuth(c, m); reply != nil {
					_ = c.Send(reply)
				}
				continue
			}
			_ = c.Send(map[string]any{"id": id, "result": true})
		case strings.EqualFold(wstest.Str(m, "action"), "subscribe"), strings.EqualFold(wstest.Str(m, "action"), "unsubscribe"):
			switch {
			case nacked && strings.EqualFold(wstest.Str(m, "action"), "subscribe"):
				_ = c.Send(map[string]any{"id": id, "result": false, field: "topic not allowed in current environment"})
			case noAck:
			case ackString:
				_ = c.Send(map[string]any{"id": id, "result": "true"})
			default:
				_ = c.Send(map[string]any{"id": id, "result": true})
			}
		}
	}
}

func (f *fakeUTA) frame(action, channel string) map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.frames) - 1; i >= 0; i-- {
		if wstest.Str(f.frames[i], "action") == action && wstest.Str(f.frames[i], "channel") == channel {
			return f.frames[i]
		}
	}
	return nil
}

func (f *fakeUTA) count(action string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, m := range f.frames {
		if wstest.Str(m, "action") == action {
			n++
		}
	}
	return n
}

func newFake(t *testing.T) (*fakeUTA, *wstest.Server) {
	t.Helper()
	wstest.CheckLeaks(t)
	f := &fakeUTA{nack: map[string]string{}}
	return f, wstest.NewServer(t, f.serve)
}

func fastReconnect() Option {
	return WithStreamOptions(stream.WithReconnect(stream.ReconnectPolicy{MinDelay: 5 * time.Millisecond, MaxDelay: 20 * time.Millisecond, Jitter: 0.1, StableAfter: time.Hour}),
		stream.WithPingInterval(time.Hour))
}

func connect(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
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

func send(t *testing.T, c *wstest.Conn, frame string) {
	t.Helper()
	if err := c.SendText(frame); err != nil {
		t.Fatalf("send: %v", err)
	}
}

func recvPush(t *testing.T, ch <-chan Push) Push {
	t.Helper()
	select {
	case p, ok := <-ch:
		if !ok {
			t.Fatal("channel closed while waiting for a push")
		}
		return p
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a push")
		return Push{}
	}
}

func expectNoPush(t *testing.T, ch <-chan Push, what string) {
	t.Helper()
	select {
	case p := <-ch:
		t.Fatalf("%s: unexpected push %+v", what, p)
	case <-time.After(120 * time.Millisecond):
	}
}

var creds = transport.Credentials{APIKey: "test-key", APISecret: "test-secret", APIPassphrase: "test-passphrase"}

func TestClient_ConnectAuthenticatesWithValidSignature(t *testing.T) {
	fake, srv := newFake(t)
	now := time.UnixMilli(1_742_175_983_882)
	got := make(chan map[string]any, 1)
	fake.onAuth = func(c *wstest.Conn, m map[string]any) any {
		got <- m
		return map[string]any{"id": m["id"], "result": true}
	}
	client := NewClient(srv.URL(), "", WithAutoReconnect(false), WithCredentials(creds), WithClock(fixedClock{now}))
	connect(t, client)

	m := <-got
	signer := auth.NewSigner(creds.APISecret)
	ts := auth.TimestampMillis(now)
	if m["op"] != "auth" || m["kc-api-key"] != creds.APIKey || m["kc-api-timestamp"] != ts {
		t.Fatalf("auth frame: %v", m)
	}
	if want := signer.Sign(ts, "POST", "/api/websocket/users/verify", ""); m["kc-api-sign"] != want {
		t.Fatalf("signature = %v, want %v", m["kc-api-sign"], want)
	}
	if want := signer.SignPassphrase(creds.APIPassphrase); m["kc-api-passphrase"] != want {
		t.Fatalf("passphrase signature = %v, want %v", m["kc-api-passphrase"], want)
	}
	if strings.Contains(fmt.Sprint(m), creds.APISecret) {
		t.Fatal("the API secret must never be sent")
	}
}

func TestClient_AuthenticationFailureIsTypedAndPermanent(t *testing.T) {
	fake, srv := newFake(t)
	fake.onAuth = func(c *wstest.Conn, m map[string]any) any {
		return map[string]any{"id": m["id"], "result": false, "message": "auth failed"}
	}
	client := NewClient(srv.URL(), "", WithCredentials(creds)) // auto-reconnect on
	err := client.Connect(context.Background())
	if !errors.Is(err, ErrAuthenticationFailed) || !stream.IsPermanent(err) {
		t.Fatalf("Connect error = %v, want a permanent ErrAuthenticationFailed", err)
	}
	var se *stream.ServerError
	if !errors.As(err, &se) || se.Message != "auth failed" {
		t.Fatalf("the server's reason must be preserved: %v", err)
	}
	if srv.Connections() != 1 {
		t.Fatalf("connections = %d; a rejected key must not be retried", srv.Connections())
	}
}

func TestClient_AuthenticationFailureAfterReconnectEndsTheClient(t *testing.T) {
	fake, srv := newFake(t)
	var mu sync.Mutex
	auths := 0
	fake.onAuth = func(c *wstest.Conn, m map[string]any) any {
		mu.Lock()
		auths++
		n := auths
		mu.Unlock()
		if n == 1 {
			return map[string]any{"id": m["id"], "result": true}
		}
		return map[string]any{"id": m["id"], "result": false, "message": "key revoked"}
	}
	client := NewClient(srv.URL(), "", WithCredentials(creds), fastReconnect())
	connect(t, client)
	srv.DropAll()
	select {
	case <-client.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("client did not stop after the key was rejected")
	}
	if err := client.Err(); !errors.Is(err, ErrAuthenticationFailed) {
		t.Fatalf("Err = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if auths != 2 {
		t.Fatalf("auth attempts = %d, want exactly 2", auths)
	}
}

func TestClient_ReconnectAuthenticatesAgainAndResubscribes(t *testing.T) {
	fake, srv := newFake(t)
	client := NewClient(srv.URL(), "", WithCredentials(creds), fastReconnect())
	connect(t, client)
	if _, err := client.Subscribe("orderAll", "UNIFIED", ""); err != nil {
		t.Fatal(err)
	}
	srv.DropAll()
	eventually(t, func() bool { return srv.Connections() == 2 && fake.count("subscribe") == 2 }, "resubscribe on the new connection")
	authCount := 0
	fake.mu.Lock()
	for _, m := range fake.frames {
		if m["op"] == "auth" {
			authCount++
		}
	}
	fake.mu.Unlock()
	if authCount != 2 {
		t.Fatalf("auth frames = %d, want one per connection", authCount)
	}
}

func TestClient_IncompleteCredentialsAreRejectedBeforeAnyWrite(t *testing.T) {
	fake, srv := newFake(t)
	client := NewClient(srv.URL(), "", WithCredentials(transport.Credentials{APIKey: "only-a-key"}))
	err := client.Connect(context.Background())
	if !errors.Is(err, ErrIncompleteCredentials) || !stream.IsPermanent(err) {
		t.Fatalf("Connect error = %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.frames) != 0 {
		t.Fatalf("frames written with incomplete credentials: %v", fake.frames)
	}
}

func TestClient_SubscribeRawPushUnsubscribe(t *testing.T) {
	fake, srv := newFake(t)
	for _, ackString := range []bool{false, true} { // both documented ack spellings
		fake.mu.Lock()
		fake.ackString = ackString
		fake.mu.Unlock()
		client := NewClient(srv.URL(), "", WithAutoReconnect(false))
		connect(t, client)
		pushes, err := client.Subscribe("ticker", "SPOT", "BTC-USDT")
		if err != nil {
			t.Fatalf("ackString=%v: Subscribe: %v", ackString, err)
		}
		send(t, srv.Conn(srv.Connections()), `{"T":"ticker.SPOT","P":1768206966101166007,"d":{"s":"BTC-USDT","l":"90968.2"}}`)
		p := recvPush(t, pushes)
		if p.T != "ticker.SPOT" || p.P != 1768206966101166007 || !strings.Contains(string(p.Data), "90968.2") {
			t.Fatalf("push: %+v", p)
		}
		if err := client.Unsubscribe("ticker", "SPOT", "BTC-USDT"); err != nil {
			t.Fatal(err)
		}
		if _, ok := <-pushes; ok {
			t.Fatal("channel must close on Unsubscribe")
		}
		if fake.frame("unsubscribe", "ticker") == nil {
			t.Fatal("the server never saw an unsubscribe")
		}
		if err := client.Unsubscribe("ticker", "SPOT", "NEVER"); err != nil {
			t.Fatalf("unsubscribing an unknown subscription must be a no-op: %v", err)
		}
		_ = client.Close()
	}
}

func TestClient_SubscribeFrameUsesDocumentedFields(t *testing.T) {
	fake, srv := newFake(t)
	client := NewClient(srv.URL(), "", WithAutoReconnect(false))
	connect(t, client)
	ctx := context.Background()

	specs := []struct {
		spec SubscribeSpec
		want map[string]any
	}{
		{SubscribeSpec{Channel: "ticker", TradeType: "FUTURES", Symbols: []string{"XBTUSDTM"}}, map[string]any{"symbol": "XBTUSDTM", "tradeType": "FUTURES"}},
		{SubscribeSpec{Channel: "trade", TradeType: "FUTURES", Symbols: []string{"ETHUSDTM", "XRPUSDTM"}}, map[string]any{"symbols": []any{"ETHUSDTM", "XRPUSDTM"}, "tradeType": "FUTURES"}},
		{SubscribeSpec{Channel: "kline", TradeType: "FUTURES", Symbols: []string{"XBTUSDTM"}, Interval: "1min"}, map[string]any{"interval": "1min", "symbol": "XBTUSDTM"}},
		{SubscribeSpec{Channel: "obu", TradeType: "FUTURES", Symbols: []string{"XBTUSDTM"}, Depth: "increment@10ms", RPIFilter: 1}, map[string]any{"depth": "increment@10ms", "rpiFilter": float64(1)}},
		{SubscribeSpec{Channel: "mark-price", Symbols: []string{"XBTUSDTM"}}, map[string]any{"symbol": "XBTUSDTM"}},
		{SubscribeSpec{Channel: "funding-fee-all-symbols"}, map[string]any{}},
		{SubscribeSpec{Channel: "balance", AccountType: "UNIFIED"}, map[string]any{"accountType": "UNIFIED"}},
		{SubscribeSpec{Channel: "newThing", TradeType: "SPOT", Extra: map[string]any{"flavour": "x"}}, map[string]any{"flavour": "x"}},
	}
	for _, tt := range specs {
		if _, err := client.SubscribeSpec(ctx, tt.spec); err != nil {
			t.Fatalf("%s: %v", tt.spec.Channel, err)
		}
		frame := fake.frame("subscribe", tt.spec.Channel)
		if frame == nil || wstest.Str(frame, "id") == "" {
			t.Fatalf("%s: no subscribe frame with an id", tt.spec.Channel)
		}
		for k, v := range tt.want {
			if fmt.Sprint(frame[k]) != fmt.Sprint(v) {
				t.Errorf("%s: field %s = %v, want %v (frame %v)", tt.spec.Channel, k, frame[k], v, frame)
			}
		}
		if _, has := frame["symbol"]; has && len(tt.spec.Symbols) != 1 {
			t.Errorf("%s: unexpected single symbol field", tt.spec.Channel)
		}
		if tt.spec.Interval == "" {
			if _, has := frame["interval"]; has {
				t.Errorf("%s: stray interval", tt.spec.Channel)
			}
		}
	}
}

// Regression: every ticker subscription of a channel+product used to receive the
// pushes of every symbol.
func TestClient_PushesAreRoutedBySymbol(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL(), "", WithAutoReconnect(false))
	connect(t, client)
	btc, err := client.SubscribeTicker("FUTURES", "XBTUSDTM")
	if err != nil {
		t.Fatal(err)
	}
	eth, err := client.SubscribeTicker("FUTURES", "ETHUSDTM")
	if err != nil {
		t.Fatal(err)
	}
	sc := srv.Conn(1)
	send(t, sc, `{"T":"ticker.FUTURES","P":1,"d":{"a":"1","A":"1","b":"1","B":"1","s":"ETHUSDTM","S":"buy","E":2,"l":"1","q":"1","M":3}}`)
	send(t, sc, `{"T":"ticker.FUTURES","P":2,"d":{"a":"9","A":"1","b":"9","B":"1","s":"XBTUSDTM","S":"sell","E":4,"l":"9","q":"1","M":5}}`)

	select {
	case tk := <-btc:
		if tk.Symbol != "XBTUSDTM" || tk.Side != "sell" || tk.LastPrice != "9" || tk.TradeType != "FUTURES" || tk.GatewayTimestamp != 2 || tk.Sequence != 4 {
			t.Fatalf("btc ticker: %+v", tk)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no BTC ticker")
	}
	select {
	case tk := <-eth:
		if tk.Symbol != "ETHUSDTM" || tk.Side != "buy" {
			t.Fatalf("eth ticker: %+v", tk)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no ETH ticker")
	}
	select {
	case tk := <-btc:
		t.Fatalf("BTC subscriber got a second ticker: %+v", tk)
	case tk := <-eth:
		t.Fatalf("ETH subscriber got a second ticker: %+v", tk)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestClient_SubscribeTickerTwiceReturnsTheSameChannel(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL(), "", WithAutoReconnect(false))
	connect(t, client)
	a, err := client.SubscribeTicker("SPOT", "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	b, err := client.SubscribeTicker("SPOT", "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("expected the cached channel")
	}
}

// KuCoin allows a public UTA connection 300 client messages per 10 seconds and a
// private one 100; the engine paces its requests by what the protocol declares.
func TestProtocolDeclaresTheUTAMessageLimit(t *testing.T) {
	for _, tt := range []struct {
		name string
		p    *protocol
		want int
	}{
		{"public", &protocol{host: "wss://example"}, 300},
		{"private with credentials", &protocol{host: "wss://example", creds: &transport.Credentials{APIKey: "k"}}, 100},
		{"private with a legacy token", &protocol{host: "wss://example", token: "t"}, 100},
	} {
		limiter, ok := any(tt.p).(wsengine.MessageLimiter)
		if !ok {
			t.Fatalf("%s: the UTA protocol must declare its message limit", tt.name)
		}
		if n, window := limiter.MessageLimit(); n != tt.want || window != 10*time.Second {
			t.Errorf("%s: limit = %d per %v, want %d per 10s", tt.name, n, window, tt.want)
		}
	}
}

func TestClient_ConcurrentSubscribesOfOneSpecShareOneSubscription(t *testing.T) {
	fake, srv := newFake(t)
	client := NewClient(srv.URL(), "", WithAutoReconnect(false))
	connect(t, client)
	spec := SubscribeSpec{Channel: "trade", TradeType: "SPOT", Symbols: []string{"BTC-USDT"}}
	const n = 32
	raw := make([]<-chan Push, n)
	tickers := make([]<-chan Ticker, n)
	rawErrs := make([]error, n)
	tickerErrs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			raw[i], rawErrs[i] = client.SubscribeSpec(context.Background(), spec)
		}(i)
		go func(i int) {
			defer wg.Done()
			tickers[i], tickerErrs[i] = client.SubscribeTicker("FUTURES", "XBTUSDTM")
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if rawErrs[i] != nil || tickerErrs[i] != nil {
			t.Fatalf("caller %d: %v / %v", i, rawErrs[i], tickerErrs[i])
		}
		if raw[i] != raw[0] || tickers[i] != tickers[0] {
			t.Fatalf("caller %d got a different channel", i)
		}
	}
	if got := fake.count("subscribe"); got != 2 {
		t.Fatalf("server saw %d subscribe frames, want 2 (one per distinct subscription)", got)
	}
}

// Regression: channels whose pushes carry no ".TRADETYPE" suffix were never
// routed, and the symbol/depth/interval parameters could not be expressed.
func TestClient_RoutesChannelsWithUnusualPushTypes(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL(), "", WithAutoReconnect(false))
	connect(t, client)
	ctx := context.Background()
	sc := srv.Conn(1)

	mark, err := client.SubscribeSpec(ctx, SubscribeSpec{Channel: "mark-price", Symbols: []string{"XBTUSDTM"}})
	if err != nil {
		t.Fatal(err)
	}
	fund, err := client.SubscribeSpec(ctx, SubscribeSpec{Channel: "funding-fee", Symbols: []string{"XBTUSDTM"}})
	if err != nil {
		t.Fatal(err)
	}
	all, err := client.SubscribeSpec(ctx, SubscribeSpec{Channel: "funding-fee-all-symbols"})
	if err != nil {
		t.Fatal(err)
	}
	lw, err := client.SubscribeSpec(ctx, SubscribeSpec{Channel: "lw", TradeType: "UNIFIED"})
	if err != nil {
		t.Fatal(err)
	}
	auction, err := client.SubscribeSpec(ctx, SubscribeSpec{Channel: "callAuctionInfo", Symbols: []string{"GROVE-USDT"}})
	if err != nil {
		t.Fatal(err)
	}

	send(t, sc, `{"T":"mark-price","P":1,"d":{"s":"XBTUSDTM","mp":"1","ip":"1","oi":"1","ts":1}}`)
	send(t, sc, `{"T":"mark-price","P":1,"d":{"s":"ETHUSDTM","mp":"2","ip":"2","oi":"2","ts":2}}`) // another symbol: ignored
	send(t, sc, `{"T":"funding-fee","P":2,"d":{"s":"XBTUSDTM","fr":"0.0001"}}`)
	send(t, sc, `{"T":"funding-fee-all-symbols","P":3,"d":[{"s":"XBTUSDTM","fr":"0.0001"},{"s":"ETHUSDTM","fr":"0.0002"}]}`)
	send(t, sc, `{"T":"lw.UNIFIED","P":4,"d":{"eT":"MARGIN_CALL"}}`)
	send(t, sc, `{"T":"risk.UNIFIED","P":5,"d":{"eT":"MARGIN_CALL"}}`) // the documented alternative spelling
	send(t, sc, `{"T":"callAuctionInfo.SPOT","t":"snapshot","P":6,"d":{"s":"GROVE-USDT","ep":"0.05"}}`)

	if p := recvPush(t, mark); p.T != "mark-price" || !strings.Contains(string(p.Data), `"mp":"1"`) {
		t.Fatalf("mark-price: %+v", p)
	}
	expectNoPush(t, mark, "mark-price for another symbol")
	if p := recvPush(t, fund); p.T != "funding-fee" {
		t.Fatalf("funding-fee: %+v", p)
	}
	if p := recvPush(t, all); p.T != "funding-fee-all-symbols" || !strings.HasPrefix(string(p.Data), "[") {
		t.Fatalf("all funding: %+v", p)
	}
	if p := recvPush(t, lw); p.T != "lw.UNIFIED" {
		t.Fatalf("lw: %+v", p)
	}
	if p := recvPush(t, lw); p.T != "risk.UNIFIED" {
		t.Fatalf("lw alias: %+v", p)
	}
	if p := recvPush(t, auction); p.T != "callAuctionInfo.SPOT" || p.Kind != "snapshot" {
		t.Fatalf("call auction: %+v", p)
	}
}

func TestClient_OrderBookPushesAreScopedByDepthAndKlinesByInterval(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL(), "", WithAutoReconnect(false))
	connect(t, client)
	ctx := context.Background()
	sc := srv.Conn(1)

	bbo, err := client.SubscribeSpec(ctx, SubscribeSpec{Channel: "obu", TradeType: "FUTURES", Symbols: []string{"XBTUSDTM"}, Depth: "1"})
	if err != nil {
		t.Fatal(err)
	}
	inc, err := client.SubscribeSpec(ctx, SubscribeSpec{Channel: "obu", TradeType: "FUTURES", Symbols: []string{"XBTUSDTM"}, Depth: "increment@10ms"})
	if err != nil {
		t.Fatal(err)
	}
	k1, err := client.SubscribeSpec(ctx, SubscribeSpec{Channel: "kline", TradeType: "FUTURES", Symbols: []string{"XBTUSDTM"}, Interval: "1min"})
	if err != nil {
		t.Fatal(err)
	}
	k5, err := client.SubscribeSpec(ctx, SubscribeSpec{Channel: "kline", TradeType: "FUTURES", Symbols: []string{"XBTUSDTM"}, Interval: "5min"})
	if err != nil {
		t.Fatal(err)
	}

	send(t, sc, `{"T":"obu.FUTURES","dp":"1","t":"snapshot","P":1,"d":{"s":"XBTUSDTM","O":1,"C":1,"a":[["2","1"]],"b":[["1","1"]]}}`)
	send(t, sc, `{"T":"obu.FUTURES","dp":"increment@10ms","t":"delta","P":2,"d":{"s":"XBTUSDTM","O":5,"C":7,"a":[],"b":[["1","0"]]}}`)
	send(t, sc, `{"T":"OBU.FUTURES","dp":"increment@10ms","t":"delta","P":3,"d":{"s":"XBTUSDTM","O":8,"C":8,"a":[],"b":[]}}`) // case-insensitive type
	send(t, sc, `{"T":"kline.FUTURES","P":4,"d":{"s":"XBTUSDTM","i":"5min","o":"1"}}`)
	send(t, sc, `{"T":"kline.FUTURES","P":5,"d":{"s":"XBTUSDTM","i":"1min","o":"2"}}`)

	if p := recvPush(t, bbo); p.Depth != "1" || p.Kind != "snapshot" {
		t.Fatalf("bbo: %+v", p)
	}
	expectNoPush(t, bbo, "bbo subscriber must not see increment pushes")
	if p := recvPush(t, inc); p.Depth != "increment@10ms" || p.Kind != "delta" || p.P != 2 {
		t.Fatalf("increment: %+v", p)
	}
	if p := recvPush(t, inc); p.P != 3 {
		t.Fatalf("increment (upper-case type): %+v", p)
	}
	if p := recvPush(t, k5); !strings.Contains(string(p.Data), `"i":"5min"`) {
		t.Fatalf("5min kline: %+v", p)
	}
	if p := recvPush(t, k1); !strings.Contains(string(p.Data), `"i":"1min"`) {
		t.Fatalf("1min kline: %+v", p)
	}
	expectNoPush(t, k1, "kline intervals must not cross")
	expectNoPush(t, k5, "kline intervals must not cross")
}

func TestClient_OverlappingSymbolSetsAreRejected(t *testing.T) {
	fake, srv := newFake(t)
	client := NewClient(srv.URL(), "", WithAutoReconnect(false))
	connect(t, client)
	ctx := context.Background()
	if _, err := client.SubscribeSpec(ctx, SubscribeSpec{Channel: "ticker", TradeType: "FUTURES", Symbols: []string{"A", "B"}}); err != nil {
		t.Fatal(err)
	}
	_, err := client.SubscribeSpec(ctx, SubscribeSpec{Channel: "ticker", TradeType: "FUTURES", Symbols: []string{"B", "C"}})
	if !errors.Is(err, stream.ErrAlreadySubscribed) {
		t.Fatalf("error = %v, want ErrAlreadySubscribed", err)
	}
	if fake.count("subscribe") != 1 {
		t.Fatal("an overlapping subscription must not be sent")
	}
}

func TestClient_SubscribeRejectionIsTyped(t *testing.T) {
	fake, srv := newFake(t)
	fake.nack["obu"] = "reason"
	fake.nack["bogus"] = "message"
	client := NewClient(srv.URL(), "", WithAutoReconnect(false))
	connect(t, client)
	for _, channel := range []string{"obu", "bogus"} {
		_, err := client.Subscribe(channel, "FUTURES", "XBTUSDTM")
		var se *stream.ServerError
		if !errors.Is(err, ErrSubscriptionFailed) || !errors.As(err, &se) || !strings.Contains(se.Message, "not allowed") {
			t.Fatalf("%s: error = %v", channel, err)
		}
	}
	if client.State() != stream.StateConnected {
		t.Fatal("rejections must not drop the connection")
	}
}

func TestClient_SubscribeAckTimeoutAndWelcomeTimeout(t *testing.T) {
	fake, srv := newFake(t)
	fake.noAck = true
	client := NewClient(srv.URL(), "", WithAutoReconnect(false), WithStreamOptions(stream.WithAckTimeout(80*time.Millisecond)))
	connect(t, client)
	if _, err := client.Subscribe("ticker", "SPOT", "BTC-USDT"); !errors.Is(err, stream.ErrAckTimeout) {
		t.Fatalf("error = %v", err)
	}

	wstest.CheckLeaks(t)
	silent := wstest.NewServer(t, func(c *wstest.Conn) {
		for {
			if _, err := c.ReadText(); err != nil {
				return
			}
		}
	})
	c2 := NewClient(silent.URL(), "", WithAutoReconnect(false), WithStreamOptions(stream.WithConnectTimeout(150*time.Millisecond)))
	if err := c2.Connect(context.Background()); !errors.Is(err, stream.ErrWelcomeTimeout) {
		t.Fatalf("error = %v", err)
	}
	if c2.State() != stream.StateIdle {
		t.Fatalf("state = %v", c2.State())
	}
}

func TestClient_CloseIsIdempotentAndClosesChannels(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL(), "", WithAutoReconnect(false))
	connect(t, client)
	pushes, err := client.Subscribe("ticker", "SPOT", "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	typed, err := client.SubscribeTicker("SPOT", "ETH-USDT")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	for name, closed := range map[string]func() bool{
		"raw":   func() bool { _, ok := <-pushes; return !ok },
		"typed": func() bool { _, ok := <-typed; return !ok },
	} {
		done := make(chan bool, 1)
		go func() { done <- closed() }()
		select {
		case ok := <-done:
			if !ok {
				t.Fatalf("%s channel still open after Close", name)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s channel was not closed", name)
		}
	}
}

func TestClient_ReconnectsAndResubscribes(t *testing.T) {
	fake, srv := newFake(t)
	client := NewClient(srv.URL(), "", fastReconnect())
	connect(t, client)
	pushes, err := client.Subscribe("ticker", "SPOT", "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	srv.DropAll()
	eventually(t, func() bool {
		return srv.Connections() == 2 && fake.count("subscribe") == 2 && client.State() == stream.StateConnected
	}, "resubscribe after an unexpected disconnect")
	send(t, srv.Conn(2), `{"T":"ticker.SPOT","P":1,"d":{"s":"BTC-USDT","l":"7"}}`)
	if p := recvPush(t, pushes); !strings.Contains(string(p.Data), `"l":"7"`) {
		t.Fatalf("push after reconnect: %+v", p)
	}
}

func TestClient_ConnectEscapesTokenAndPreservesHostQuery(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL()+"?region=eu", "a+b&c", WithAutoReconnect(false))
	connect(t, client)
	q := srv.Conn(1).Request.URL.Query()
	if q.Get("region") != "eu" || q.Get("token") != "a+b&c" {
		t.Fatalf("unexpected query: %s", srv.Conn(1).Request.URL.RawQuery)
	}
	_, srv2 := newFake(t)
	public := NewClient(srv2.URL(), "", WithAutoReconnect(false))
	connect(t, public)
	if srv2.Conn(1).Request.URL.RawQuery != "" {
		t.Fatalf("a public connection must not carry a token: %s", srv2.Conn(1).Request.URL.RawQuery)
	}
	if err := NewClient("://bad", "").Connect(context.Background()); !stream.IsPermanent(err) {
		t.Fatalf("an invalid host must be a permanent error, got %v", err)
	}
}

func TestClient_HeartbeatFollowsTheWelcomeAndUsesTheDocumentedPingFrame(t *testing.T) {
	fake, srv := newFake(t)
	fake.pingMs = 80 // advertised by the welcome frame; pinged at half of it
	client := NewClient(srv.URL(), "", WithAutoReconnect(false), WithStreamOptions(stream.WithPingInterval(0)))
	connect(t, client)
	time.Sleep(300 * time.Millisecond)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.pings < 3 {
		t.Fatalf("only %d pings; the welcome's pingInterval was ignored", fake.pings)
	}
	var ping map[string]any
	for _, m := range fake.frames {
		if m["op"] == "ping" {
			ping = m
		}
	}
	if wstest.Str(ping, "id") == "" || wstest.Str(ping, "timestamp") == "" {
		t.Fatalf("ping frame: %v", ping)
	}
}

func TestClient_UnsolicitedGatewayErrorsBecomeEvents(t *testing.T) {
	_, srv := newFake(t)
	events := make(chan stream.Event, 8)
	client := NewClient(srv.URL(), "", WithAutoReconnect(false), WithStreamOptions(stream.WithEventHandler(func(ev stream.Event) { events <- ev })))
	connect(t, client)
	send(t, srv.Conn(1), `{"code":"429002","msg":"Too many requests in a short period"}`)
	for {
		select {
		case ev := <-events:
			if ev.Type != stream.EventServerError {
				continue
			}
			var se *stream.ServerError
			if !errors.As(ev.Err, &se) || se.Code != 429002 || !errors.Is(ev.Err, stream.ErrRateLimited) {
				t.Fatalf("event error: %v", ev.Err)
			}
			return
		case <-time.After(5 * time.Second):
			t.Fatal("no server error event")
		}
	}
}

func TestSubscribeTyped_AndHandlerSeeResetAfterReconnect(t *testing.T) {
	_, srv := newFake(t)
	client := NewClient(srv.URL(), "", fastReconnect())
	connect(t, client)
	spec := SubscribeSpec{Channel: "obu", TradeType: "FUTURES", Symbols: []string{"XBTUSDTM"}, Depth: "increment@10ms"}
	log := make(chan string, 16)
	h, err := client.SubscribeHandler(context.Background(), spec, stream.HandlerFuncs{
		Frame: func(f stream.Frame) {
			p := f.Msg.(*Push)
			log <- "frame:" + p.Kind
		},
		Reset:  func(uint64) { log <- "reset" },
		Closed: func() { log <- "closed" },
	})
	if err != nil {
		t.Fatal(err)
	}
	if h.Name() != spec.Name() {
		t.Fatalf("name = %q", h.Name())
	}
	send(t, srv.Conn(1), `{"T":"obu.FUTURES","dp":"increment@10ms","t":"snapshot","P":1,"d":{"s":"XBTUSDTM","O":1,"C":1,"a":[],"b":[]}}`)
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
	expect("frame:snapshot")
	srv.DropAll()
	expect("reset")
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	expect("closed")
	if _, err := client.SubscribeHandler(context.Background(), spec, nil); err == nil {
		t.Fatal("a nil handler must be rejected")
	}

	type pair struct{ P int64 }
	sub, err := SubscribeTyped(context.Background(), client, SubscribeSpec{Channel: "trade", TradeType: "FUTURES", Symbols: []string{"XBTUSDTM"}},
		func(p *Push) (pair, bool, error) {
			if p.P == 0 {
				return pair{}, false, nil // skipped
			}
			if string(p.Data) == `"bad"` {
				return pair{}, false, errors.New("bad payload")
			}
			return pair{p.P}, true, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return client.State() == stream.StateConnected && srv.Conn(2) != nil }, "connection ready")
	sc := srv.Conn(srv.Connections())
	send(t, sc, `{"T":"trade.FUTURES","P":0,"d":{"s":"XBTUSDTM"}}`)
	send(t, sc, `{"T":"trade.FUTURES","P":5,"d":"bad"}`) // not an object: unroutable by symbol, never delivered
	send(t, sc, `{"T":"trade.FUTURES","P":6,"d":{"s":"XBTUSDTM"}}`)
	select {
	case v := <-sub.C():
		if v.P != 6 {
			t.Fatalf("got %+v", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no typed push")
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSubscribeSpec(t *testing.T) {
	t.Run("push types", func(t *testing.T) {
		tests := []struct {
			spec SubscribeSpec
			want []string
		}{
			{SubscribeSpec{Channel: "ticker", TradeType: "SPOT"}, []string{"ticker.SPOT"}},
			{SubscribeSpec{Channel: "execution.lite", TradeType: "UNIFIED"}, []string{"execution.lite.UNIFIED"}},
			{SubscribeSpec{Channel: "balance", AccountType: "FUNDING"}, []string{"balance.FUNDING"}},
			{SubscribeSpec{Channel: "mark-price"}, []string{"mark-price"}},
			{SubscribeSpec{Channel: "Funding-Fee-All-Symbols"}, []string{"funding-fee-all-symbols"}},
			{SubscribeSpec{Channel: "callAuctionInfo"}, []string{"callAuctionInfo.SPOT"}},
			{SubscribeSpec{Channel: "lw", TradeType: "UNIFIED"}, []string{"lw.UNIFIED", "risk.UNIFIED"}},
			{SubscribeSpec{Channel: "unknown"}, []string{"unknown"}},
		}
		for _, tt := range tests {
			if got := tt.spec.PushTypes(); fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Errorf("%+v: PushTypes = %v, want %v", tt.spec, got, tt.want)
			}
		}
	})
	t.Run("name is independent of symbol order and distinguishes parameters", func(t *testing.T) {
		a := SubscribeSpec{Channel: "ticker", TradeType: "FUTURES", Symbols: []string{"B", "A"}}
		b := SubscribeSpec{Channel: "ticker", TradeType: "FUTURES", Symbols: []string{"A", "B"}}
		if a.Name() != b.Name() {
			t.Fatal("symbol order must not change the identity")
		}
		for _, other := range []SubscribeSpec{
			{Channel: "ticker", TradeType: "SPOT", Symbols: []string{"A", "B"}},
			{Channel: "ticker", TradeType: "FUTURES", Symbols: []string{"A"}},
			{Channel: "obu", TradeType: "FUTURES", Symbols: []string{"A", "B"}, Depth: "5"},
			{Channel: "ticker", TradeType: "FUTURES", Symbols: []string{"A", "B"}, Extra: map[string]any{"x": 1}},
		} {
			if a.Name() == other.Name() {
				t.Errorf("%+v must differ from %+v", other, a)
			}
		}
		if (SubscribeSpec{Channel: "obu", Depth: "5"}).Name() == (SubscribeSpec{Channel: "obu", Depth: "50"}).Name() {
			t.Error("depths must have distinct identities")
		}
		if (SubscribeSpec{Channel: "kline", Interval: "1min"}).Name() == (SubscribeSpec{Channel: "kline", Interval: "5min"}).Name() {
			t.Error("intervals must have distinct identities")
		}
	})
	t.Run("routes", func(t *testing.T) {
		spec := SubscribeSpec{Channel: "obu", TradeType: "FUTURES", Symbols: []string{"A", "B"}, Depth: "50"}
		if got := fmt.Sprint(spec.routes()); got != "[obu.futures|50|A| obu.futures|50|B|]" {
			t.Fatalf("routes = %s", got)
		}
		wild := SubscribeSpec{Channel: "orderAll", TradeType: "UNIFIED"}
		if got := fmt.Sprint(wild.routes()); got != "[orderall.unified||*|]" {
			t.Fatalf("routes = %s", got)
		}
		kl := SubscribeSpec{Channel: "kline", TradeType: "SPOT", Symbols: []string{"A"}, Interval: "1min"}
		if got := fmt.Sprint(kl.routes()); got != "[kline.spot||A|1min]" {
			t.Fatalf("routes = %s", got)
		}
	})
}

func TestClassify(t *testing.T) {
	p := &protocol{}
	kinds := map[int]string{0: "unknown", 1: "welcome", 2: "pong", 3: "ack", 4: "nack", 5: "push", 6: "error"}
	tests := []struct {
		name, raw, kind string
	}{
		{"welcome public", `{"sessionId":"s","message":"welcome","pingInterval":18000}`, "welcome"},
		{"welcome private", `{"sessionId":"s","message":"welcome","pingInterval":18000,"pingTimeout":10000}`, "welcome"},
		{"pong", `{"id":"1","op":"pong","timestamp":1}`, "pong"},
		{"legacy pong", `{"id":"1","type":"pong","ts":1}`, "pong"},
		{"ack bool", `{"id":"1","result":true}`, "ack"},
		{"ack string", `{"id":"1","result":"true"}`, "ack"},
		{"nack bool", `{"id":"1","result":false,"message":"auth failed"}`, "nack"},
		{"nack string", `{"id":"1","result":"false"}`, "nack"},
		{"nack reason", `{"id":"1","result":false,"reason":"topic Obu type = \"unknown\" not allowed"}`, "nack"},
		{"gateway error string code", `{"code":"400003","msg":"KC-API-KEY not exists.","inTime":1,"outTime":2}`, "error"},
		{"gateway error number code", `{"code":420001,"msg":"Too many errors, disconnected."}`, "error"},
		{"push", `{"T":"ticker.SPOT","P":1,"d":{"s":"BTC-USDT"}}`, "push"},
		{"push array payload", `{"T":"funding-fee-all-symbols","P":1,"d":[]}`, "push"},
		{"garbage", `nope`, "unknown"},
		{"empty object", `{}`, "unknown"},
	}
	for _, tt := range tests {
		in := p.Classify([]byte(tt.raw))
		if got := kinds[int(in.Kind)]; got != tt.kind {
			t.Errorf("%s: kind = %s, want %s", tt.name, got, tt.kind)
		}
	}
	in := p.Classify([]byte(`{"sessionId":"s","message":"welcome","pingInterval":18000,"pingTimeout":10000}`))
	if in.PingInterval != 18*time.Second || in.PingTimeout != 10*time.Second {
		t.Fatalf("welcome heartbeat: %+v", in)
	}
	in = p.Classify([]byte(`{"id":"7","result":false,"reason":"nope"}`))
	var se *stream.ServerError
	if !errors.As(in.Err, &se) || se.Message != "nope" || se.ID != "7" || in.ID != "7" {
		t.Fatalf("nack: %+v", in)
	}
	in = p.Classify([]byte(`{"code":"429001","msg":"Too many total requests"}`))
	if !errors.As(in.Err, &se) || se.Code != 429001 || !errors.Is(in.Err, stream.ErrRateLimited) {
		t.Fatalf("gateway error: %+v", in)
	}
	// Both-case keys must bind to their own fields: "s" is the symbol, "S" the side.
	push := p.Classify([]byte(`{"T":"ticker.FUTURES","P":9,"d":{"a":"1","s":"XBTUSDTM","S":"buy"}}`))
	if push.Route != "ticker.futures||XBTUSDTM|" || push.RouteAlt != "ticker.futures||*|" {
		t.Fatalf("routes: %q / %q", push.Route, push.RouteAlt)
	}
	msg := push.Msg.(*Push)
	if msg.P != 9 || msg.T != "ticker.FUTURES" || string(msg.Data) == "" {
		t.Fatalf("push: %+v", msg)
	}
	// The side key declared before the symbol key must not clobber it either.
	push = p.Classify([]byte(`{"T":"ticker.FUTURES","P":9,"d":{"S":"buy","s":"XBTUSDTM"}}`))
	if push.Route != "ticker.futures||XBTUSDTM|" {
		t.Fatalf("symbol clobbered by the side key: %q", push.Route)
	}
	push = p.Classify([]byte(`{"T":"kline.FUTURES","P":9,"d":{"s":"XBTUSDTM","i":"5min"}}`))
	if push.Route != "kline.futures||XBTUSDTM|5min" {
		t.Fatalf("kline route: %q", push.Route)
	}
}

func TestPushRoutesIgnoresMalformedPayloads(t *testing.T) {
	for _, d := range []string{``, `null`, `"x"`, `[1,2]`, `{`, `{"s":5}`} {
		route, alt := pushRoutes("ticker.SPOT", "", json.RawMessage(d))
		if route != "ticker.spot|||" || alt != "ticker.spot||*|" {
			t.Errorf("d=%q: routes %q / %q", d, route, alt)
		}
	}
}
