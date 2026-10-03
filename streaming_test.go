package kucoin_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kucoin "github.com/tigusigalpa/kucoin-go"
	futuresstreaming "github.com/tigusigalpa/kucoin-go/classic/futures/streaming"
	"github.com/tigusigalpa/kucoin-go/internal/wsengine"
	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
	"github.com/tigusigalpa/kucoin-go/types"
)

// These tests drive the streaming services through the root Client exactly as an
// application does: the REST calls (connection tokens, order-book snapshots) go to
// an httptest server, the WebSocket to a scriptable fake KuCoin. They prove the
// wiring — which executor and host each service uses, that tokens are fetched per
// connection, that credentials gate private sessions — on top of the per-package
// tests of the protocol, decoding and order-book logic.

func init() { wsengine.SetMinPingInterval(time.Millisecond) }

// fakeKuCoin is an HTTP + WebSocket fake of KuCoin's Classic API.
type fakeKuCoin struct {
	ws   *wstest.ClassicFake
	rest *httptest.Server

	mu       sync.Mutex
	requests []recordedRequest
	handlers map[string]http.HandlerFunc // "METHOD /path" -> handler
}

type recordedRequest struct {
	Method, Path, Query string
	Header              http.Header
}

func newFakeKuCoin(t *testing.T) *fakeKuCoin {
	t.Helper()
	wstest.CheckLeaks(t) // first, so that its cleanup runs last
	f := &fakeKuCoin{ws: wstest.NewClassicFake(t), handlers: map[string]http.HandlerFunc{}}
	token := func(w http.ResponseWriter, _ *http.Request) {
		writeEnvelope(w, f.ws.Token())
	}
	f.handlers["POST /api/v1/bullet-public"] = token
	f.handlers["POST /api/v1/bullet-private"] = token
	f.rest = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.requests = append(f.requests, recordedRequest{r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Clone()})
		h := f.handlers[r.Method+" "+r.URL.Path]
		f.mu.Unlock()
		if h == nil {
			http.NotFound(w, r)
			return
		}
		h(w, r)
	}))
	t.Cleanup(f.rest.Close)
	return f
}

func writeEnvelope(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"code": "200000", "data": data})
}

func (f *fakeKuCoin) handle(route string, h http.HandlerFunc) {
	f.mu.Lock()
	f.handlers[route] = h
	f.mu.Unlock()
}

func (f *fakeKuCoin) requestsTo(method, path string) []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []recordedRequest
	for _, r := range f.requests {
		if r.Method == method && r.Path == path {
			out = append(out, r)
		}
	}
	return out
}

func (f *fakeKuCoin) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

// quick makes the clients reconnect and time out fast and keeps heartbeats out of
// the way.
func quick() kucoin.Option {
	return kucoin.WithStreamOptions(
		stream.WithReconnect(stream.ReconnectPolicy{MinDelay: 5 * time.Millisecond, MaxDelay: 20 * time.Millisecond, Jitter: 0.1, StableAfter: time.Hour}),
		stream.WithPingInterval(time.Hour),
	)
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func recv[T any](t *testing.T, sub *stream.Subscription[T]) T {
	t.Helper()
	select {
	case v, ok := <-sub.C():
		if !ok {
			t.Fatalf("subscription ended: %v", sub.Err())
		}
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an update")
		var zero T
		return zero
	}
}

const (
	fxTickerV2 = `{"topic":"/contractMarket/tickerV2:XBTUSDTM","type":"message","subject":"tickerV2","sn":1713516609293,"data":{"symbol":"XBTUSDTM","sequence":1713516609293,"bestBidSize":5044,"bestBidPrice":"86454.5","bestAskPrice":"86454.6","bestAskSize":73,"ts":1740641976241000000}}`
	fxWallet   = `{"topic":"/contractAccount/wallet","type":"message","subject":"walletBalance.change","userId":"633559791e1cbc0001f319bc","channelType":"private","data":{"crossPosMargin":"17.551016","isolatedOrderMargin":"0","holdBalance":"0","equity":"387.224858816","version":"2118","availableBalance":"285.652001096","isolatedPosMargin":"63.98342359","maxWithdrawAmount":"285.645841096","walletBalance":"371.394298816","isolatedFundingFeeMargin":"2.89220199","crossUnPnl":"2.95996","totalCrossMargin":"310.370835226","currency":"USDT","isolatedUnPnl":"12.8706","crossOrderMargin":"10.06002012","timestamp":"1741164936624"}}`
)

func TestClassicFuturesStream_PublicSessionEndToEnd(t *testing.T) {
	f := newFakeKuCoin(t)
	f.ws.OnSubscribe("/contractMarket/tickerV2:XBTUSDTM", fxTickerV2)
	client := kucoin.NewClient(kucoin.WithClassicFuturesBaseURL(f.rest.URL), quick())

	session, err := client.Classic.Futures.Stream.DialPublic(testCtx(t))
	if err != nil {
		t.Fatalf("DialPublic: %v", err)
	}
	defer session.Close()
	ticks, err := session.SubscribeTickerV2(testCtx(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatalf("SubscribeTickerV2: %v", err)
	}
	tick := recv(t, ticks)
	if tick.Symbol != "XBTUSDTM" || tick.BestBidPrice != "86454.5" || tick.BestAskPrice != "86454.6" || tick.BestBidSize != 5044 {
		t.Fatalf("typed ticker: %+v", tick)
	}

	reqs := f.requestsTo(http.MethodPost, "/api/v1/bullet-public")
	if len(reqs) != 1 {
		t.Fatalf("expected one public token request, got %d", len(reqs))
	}
	if reqs[0].Header.Get("KC-API-KEY") != "" || reqs[0].Header.Get("KC-API-SIGN") != "" {
		t.Errorf("a public token request must not be signed: %v", reqs[0].Header)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-ticks.Done():
	case <-time.After(time.Second):
		t.Fatal("closing the session must end its subscriptions")
	}
	if ticks.Err() != nil {
		t.Fatalf("a requested close is not an error, got %v", ticks.Err())
	}
}

func TestClassicFuturesStream_EveryReconnectFetchesAFreshToken(t *testing.T) {
	f := newFakeKuCoin(t)
	f.ws.OnSubscribe("/contractMarket/tickerV2:XBTUSDTM", fxTickerV2)
	client := kucoin.NewClient(kucoin.WithClassicFuturesBaseURL(f.rest.URL), quick())

	session, err := client.Classic.Futures.Stream.DialPublic(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	ticks, err := session.SubscribeTickerV2(testCtx(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	recv(t, ticks)

	f.ws.Server.DropAll()
	recv(t, ticks) // the fake re-sends its script after the resubscription
	if n := len(f.requestsTo(http.MethodPost, "/api/v1/bullet-public")); n != 2 {
		t.Fatalf("a reconnect must fetch a new token: %d token requests", n)
	}
	// The counter moves once the restore finished, a moment after the first push.
	deadline := time.Now().Add(5 * time.Second)
	for session.Stats().Reconnects != 1 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if got := session.Stats().Reconnects; got != 1 {
		t.Fatalf("Reconnects = %d, want 1", got)
	}
}

func TestClassicFuturesStream_PrivateSessionNeedsCredentialsBeforeAnyNetworkAccess(t *testing.T) {
	f := newFakeKuCoin(t)
	client := kucoin.NewClient(kucoin.WithClassicFuturesBaseURL(f.rest.URL), quick())

	_, err := client.Classic.Futures.Stream.DialPrivate(testCtx(t))
	if !errors.Is(err, transport.ErrCredentialsRequired) {
		t.Fatalf("DialPrivate without credentials = %v, want transport.ErrCredentialsRequired", err)
	}
	if f.requestCount() != 0 || f.ws.Server.Connections() != 0 {
		t.Fatalf("no network access expected: %d REST requests, %d WebSocket connections", f.requestCount(), f.ws.Server.Connections())
	}
}

func TestClassicFuturesStream_PrivateSessionSignsTheTokenRequest(t *testing.T) {
	f := newFakeKuCoin(t)
	f.ws.OnSubscribe("/contractAccount/wallet", fxWallet)
	client := kucoin.NewClient(
		kucoin.WithClassicFuturesBaseURL(f.rest.URL),
		kucoin.WithCredentials(kucoin.Credentials{APIKey: "test-key", APISecret: "test-secret", APIPassphrase: "test-pass", APIKeyVersion: "2"}),
		quick(),
	)
	session, err := client.Classic.Futures.Stream.DialPrivate(testCtx(t))
	if err != nil {
		t.Fatalf("DialPrivate: %v", err)
	}
	defer session.Close()

	reqs := f.requestsTo(http.MethodPost, "/api/v1/bullet-private")
	if len(reqs) != 1 {
		t.Fatalf("expected one private token request, got %d", len(reqs))
	}
	h := reqs[0].Header
	if h.Get("KC-API-KEY") != "test-key" || h.Get("KC-API-SIGN") == "" || h.Get("KC-API-TIMESTAMP") == "" || h.Get("KC-API-PASSPHRASE") == "" {
		t.Fatalf("the private token request is not signed: %v", h)
	}
	if h.Get("KC-API-PASSPHRASE") == "test-pass" {
		t.Error("the passphrase must be sent signed, not in clear text")
	}

	balance, err := session.SubscribeBalance(testCtx(t))
	if err != nil {
		t.Fatalf("SubscribeBalance: %v", err)
	}
	ev := recv(t, balance)
	if ev.Currency != "USDT" || ev.AvailableBalance != "285.652001096" {
		t.Fatalf("typed balance: %+v", ev)
	}
}

func TestClassicFuturesStream_PrivateChannelOnAPublicSessionIsRejectedLocally(t *testing.T) {
	f := newFakeKuCoin(t)
	client := kucoin.NewClient(kucoin.WithClassicFuturesBaseURL(f.rest.URL), quick())
	session, err := client.Classic.Futures.Stream.DialPublic(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	_, err = session.SubscribeBalance(testCtx(t))
	if !errors.Is(err, futuresstreaming.ErrPrivateConnectionRequired) {
		t.Fatalf("SubscribeBalance on a public session = %v", err)
	}
	if f.ws.Count("subscribe", "/contractAccount/wallet") != 0 {
		t.Fatal("nothing must be sent to KuCoin for a request that cannot work")
	}
}

func TestClassicFuturesStream_TokenFailureIsTypedAndCarriesKuCoinsError(t *testing.T) {
	f := newFakeKuCoin(t)
	f.handle("POST /api/v1/bullet-public", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":"400007","msg":"Access denied - require more permission"}`))
	})
	client := kucoin.NewClient(kucoin.WithClassicFuturesBaseURL(f.rest.URL), quick())
	_, err := client.Classic.Futures.Stream.DialPublic(testCtx(t))
	if !errors.Is(err, stream.ErrTokenUnavailable) {
		t.Fatalf("DialPublic = %v, want stream.ErrTokenUnavailable", err)
	}
	var ke *transport.KucoinError
	if !errors.As(err, &ke) || ke.Code != "400007" {
		t.Fatalf("the REST error must stay reachable: %v", err)
	}
}

func TestClassicFuturesStream_ManagedOrderBookUsesTheRESTSnapshot(t *testing.T) {
	f := newFakeKuCoin(t)
	var snapshotRequests atomic.Int32
	f.handle("GET /api/v1/level2/snapshot", func(w http.ResponseWriter, r *http.Request) {
		snapshotRequests.Add(1)
		if r.URL.Query().Get("symbol") != "XBTUSDTM" {
			t.Errorf("snapshot requested with %q", r.URL.RawQuery)
		}
		// Live shape: levels are bare JSON numbers, some with a trailing ".0".
		writeEnvelope(w, json.RawMessage(`{"sequence":16,"symbol":"XBTUSDTM","ts":1731897467182000000,
			"bids":[[3988.5,40.0],[3988.4,9]],"asks":[[3988.60,50],[3988.7,5]]}`))
	})
	delta := func(seq int, change string) string {
		return `{"topic":"/contractMarket/level2:XBTUSDTM","type":"message","subject":"level2","sn":` + itoa(seq) +
			`,"data":{"sequence":` + itoa(seq) + `,"change":"` + change + `","timestamp":1731897467182}}`
	}
	// 15 and 16 are already inside the snapshot; 17 and 18 are the real updates.
	f.ws.OnSubscribe("/contractMarket/level2:XBTUSDTM",
		delta(15, "3988.5,buy,1"), delta(16, "3988.6,sell,1"), delta(17, "3988.50,buy,44"), delta(18, "3988.60,sell,0"))
	client := kucoin.NewClient(kucoin.WithClassicFuturesBaseURL(f.rest.URL), quick())

	session, err := client.Classic.Futures.Stream.DialPublic(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	book, err := session.SubscribeOrderBook(testCtx(t), "XBTUSDTM")
	if err != nil {
		t.Fatalf("SubscribeOrderBook: %v", err)
	}
	defer book.Close()
	select {
	case <-book.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("the book never became ready")
	}
	deadline := time.Now().Add(5 * time.Second)
	for book.Book().Sequence() < 18 {
		if time.Now().After(deadline) {
			t.Fatalf("book stuck at sequence %d", book.Book().Sequence())
		}
		time.Sleep(2 * time.Millisecond)
	}
	bid, _ := book.Book().BestBid()
	ask, _ := book.Book().BestAsk()
	// 17 replaced the 3988.5 bid (written "3988.50" by one feed, "3988.5" by the other:
	// one level), 18 removed the 3988.6 ask.
	if !sameNumber(bid.Price, "3988.5") || !sameNumber(bid.Size, "44") || !sameNumber(ask.Price, "3988.7") || !sameNumber(ask.Size, "5") {
		t.Fatalf("book best bid %+v best ask %+v", bid, ask)
	}
	if n := snapshotRequests.Load(); n != 1 {
		t.Fatalf("expected exactly one REST snapshot, got %d", n)
	}
	if req := f.requestsTo(http.MethodGet, "/api/v1/level2/snapshot"); len(req) != 1 || req[0].Header.Get("KC-API-KEY") != "" {
		t.Fatalf("the Futures snapshot is public and must not be signed: %+v", req)
	}
}

// sameNumber compares decimals as numbers: "3988.50" and "3988.5" are one price.
func sameNumber(got types.Decimal, want string) bool {
	c, err := got.Cmp(types.Decimal(want))
	return err == nil && c == 0
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func TestClassicFuturesStream_PerDialOptionsOverrideClientOptions(t *testing.T) {
	f := newFakeKuCoin(t)
	var fromClient, fromDial atomic.Int32
	client := kucoin.NewClient(
		kucoin.WithClassicFuturesBaseURL(f.rest.URL),
		quick(),
		kucoin.WithStreamOptions(stream.WithEventHandler(func(ev stream.Event) {
			if ev.Type == stream.EventConnected {
				fromClient.Add(1)
			}
		})),
	)
	// Without a per-dial option the client default applies.
	s1, err := client.Classic.Futures.Stream.DialPublic(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	// A per-dial handler replaces it for that session only.
	s2, err := client.Classic.Futures.Stream.DialPublic(testCtx(t), stream.WithEventHandler(func(ev stream.Event) {
		if ev.Type == stream.EventConnected {
			fromDial.Add(1)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	deadline := time.Now().Add(5 * time.Second)
	for (fromClient.Load() != 1 || fromDial.Load() != 1) && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if fromClient.Load() != 1 || fromDial.Load() != 1 {
		t.Fatalf("connected events: client handler %d (want 1), dial handler %d (want 1)", fromClient.Load(), fromDial.Load())
	}
}

func TestClassicFuturesMarket_IsWiredToTheFuturesHost(t *testing.T) {
	f := newFakeKuCoin(t)
	f.handle("GET /api/v1/timestamp", func(w http.ResponseWriter, _ *http.Request) { writeEnvelope(w, 1790982415541) })
	client := kucoin.NewClient(kucoin.WithClassicFuturesBaseURL(f.rest.URL))
	got, err := client.Classic.Futures.Market.GetServerTime(testCtx(t))
	if err != nil || got != 1790982415541 {
		t.Fatalf("GetServerTime = %d, %v", got, err)
	}
	if strings.Contains(f.rest.URL, "kucoin.com") {
		t.Fatal("test server URL must be local")
	}
}
