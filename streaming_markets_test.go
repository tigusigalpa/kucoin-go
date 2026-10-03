package kucoin_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	kucoin "github.com/tigusigalpa/kucoin-go"
	"github.com/tigusigalpa/kucoin-go/auth"
	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
	utastreaming "github.com/tigusigalpa/kucoin-go/uta/v2/streaming"
)

// The streaming services of Classic Spot, Classic Margin and UTA, reached through
// kucoin.Client like an application does. Protocol, decoding and order-book logic
// are tested in the streaming packages; these tests prove the wiring: hosts, token
// calls, credentials, snapshot adapters.

const (
	fxSpotTicker  = `{"type":"message","topic":"/market/ticker:BTC-USDT","subject":"trade.ticker","data":{"sequence":"1545896668986","price":"0.08","size":"0.011","bestAsk":"0.08","bestAskSize":"0.18","bestBid":"0.049","bestBidSize":"0.036","Time":1704873323416}}`
	fxMarginMark  = `{"topic":"/indicator/markPrice:USDT-BTC","type":"message","subject":"tick","data":{"symbol":"USDT-BTC","granularity":1000,"value":0.000011820000000,"timestamp":1740840036000}}`
	fxUTATicker   = `{"T":"ticker.FUTURES","P":1768218267869446269,"d":{"a":"90580.5","A":"36","q":"3","b":"90580.4","B":"4852","s":"XBTUSDTM","S":"buy","E":1905974001288,"l":"90580.5","M":1768218267868000000}}`
	fxUTABalance  = `{"P":1770116995060810093,"T":"balance.UNIFIED","d":{"U":1770116995058000000,"a":"0.0000517000","b":"0.0000517000","c":"BTC","e":"0.0000517000","h":"0.0000000000","l":"0.0000000000","cS":"1"}}`
	spotBookRoute = "GET /api/v3/market/orderbook/level2"
)

func spotL2(seq int, price, size string) string {
	return `{"topic":"/market/level2:BTC-USDT","type":"message","subject":"trade.l2update","data":{"changes":{"asks":[],"bids":[["` + price + `","` + size + `","` + itoa(seq) + `"]]},"sequenceEnd":` + itoa(seq) + `,"sequenceStart":` + itoa(seq) + `,"symbol":"BTC-USDT","time":1729816425625}}`
}

func testCredentials() kucoin.Credentials {
	return kucoin.Credentials{APIKey: "test-key", APISecret: "test-secret", APIPassphrase: "test-pass", APIKeyVersion: "2"}
}

func TestClassicSpotStream_PublicSessionEndToEnd(t *testing.T) {
	f := newFakeKuCoin(t)
	f.ws.OnSubscribe("/market/ticker:BTC-USDT", fxSpotTicker)
	client := kucoin.NewClient(kucoin.WithClassicBaseURL(f.rest.URL), quick())

	session, err := client.Classic.Spot.Stream.DialPublic(testCtx(t))
	if err != nil {
		t.Fatalf("DialPublic: %v", err)
	}
	defer session.Close()
	ticks, err := session.SubscribeTicker(testCtx(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatalf("SubscribeTicker: %v", err)
	}
	tick := recv(t, ticks)
	if tick.Symbol != "BTC-USDT" || tick.BestAsk != "0.08" || tick.BestBid != "0.049" || tick.Price != "0.08" {
		t.Fatalf("typed ticker: %+v", tick)
	}
	reqs := f.requestsTo(http.MethodPost, "/api/v1/bullet-public")
	if len(reqs) != 1 || reqs[0].Header.Get("KC-API-KEY") != "" {
		t.Fatalf("one unsigned public token request expected: %+v", reqs)
	}
}

func TestClassicSpotStream_PrivateSessionNeedsCredentials(t *testing.T) {
	f := newFakeKuCoin(t)
	client := kucoin.NewClient(kucoin.WithClassicBaseURL(f.rest.URL), quick())
	if _, err := client.Classic.Spot.Stream.DialPrivate(testCtx(t)); !errors.Is(err, transport.ErrCredentialsRequired) {
		t.Fatalf("DialPrivate without credentials = %v, want transport.ErrCredentialsRequired", err)
	}
	if f.requestCount() != 0 || f.ws.Server.Connections() != 0 {
		t.Fatalf("no network access expected: %d REST requests, %d WebSocket connections", f.requestCount(), f.ws.Server.Connections())
	}
}

func TestClassicSpotStream_ManagedOrderBookWithoutCredentialsFailsClearlyAndAtOnce(t *testing.T) {
	f := newFakeKuCoin(t)
	f.ws.OnSubscribe("/market/level2:BTC-USDT", spotL2(17, "3988.5", "44"))
	client := kucoin.NewClient(kucoin.WithClassicBaseURL(f.rest.URL), quick())
	session, err := client.Classic.Spot.Stream.DialPublic(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	book, err := session.SubscribeOrderBook(testCtx(t), "BTC-USDT")
	if err != nil {
		t.Fatalf("SubscribeOrderBook: %v", err)
	}
	select {
	case <-book.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a book that can never be seeded must end, not retry forever")
	}
	if !errors.Is(book.Err(), transport.ErrCredentialsRequired) || !errors.Is(book.Err(), stream.ErrResyncFailed) {
		t.Fatalf("Err = %v", book.Err())
	}
	if n := len(f.requestsTo(http.MethodGet, "/api/v3/market/orderbook/level2")); n != 0 {
		t.Fatalf("an unsigned snapshot request must never be sent, got %d", n)
	}
}

func TestClassicSpotStream_ManagedOrderBookUsesTheSignedSnapshot(t *testing.T) {
	f := newFakeKuCoin(t)
	f.handle(spotBookRoute, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("symbol") != "BTC-USDT" {
			t.Errorf("snapshot requested with %q", r.URL.RawQuery)
		}
		writeEnvelope(w, json.RawMessage(`{"time":1729816425000,"sequence":"16","bids":[["3988.51","56"],["3988.50","15"]],"asks":[["3988.59","3"],["3988.60","47"]]}`))
	})
	// 16 is contained in the snapshot, 17 is the first real update.
	f.ws.OnSubscribe("/market/level2:BTC-USDT", spotL2(16, "3988.51", "1"), spotL2(17, "3988.50", "44"))
	client := kucoin.NewClient(kucoin.WithClassicBaseURL(f.rest.URL), kucoin.WithCredentials(testCredentials()), quick())
	session, err := client.Classic.Spot.Stream.DialPublic(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	book, err := session.SubscribeOrderBook(testCtx(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	defer book.Close()
	eventually(t, func() bool { return book.State() == orderbook.SyncSynced && book.Book().Sequence() == 17 }, "the book to follow the stream to sequence 17")
	bid, _ := book.Book().BestBid()
	if !sameNumber(bid.Price, "3988.51") || !sameNumber(bid.Size, "56") {
		t.Fatalf("best bid %+v: the stale update 16 must not have been applied", bid)
	}
	snap := book.Book().Snapshot(0)
	if len(snap.Bids) != 2 || !sameNumber(snap.Bids[1].Size, "44") {
		t.Fatalf("bids %+v: update 17 must have replaced the 3988.5 level", snap.Bids)
	}
	reqs := f.requestsTo(http.MethodGet, "/api/v3/market/orderbook/level2")
	if len(reqs) != 1 || reqs[0].Header.Get("KC-API-KEY") != "test-key" || reqs[0].Header.Get("KC-API-SIGN") == "" {
		t.Fatalf("the Spot snapshot is a signed endpoint: %+v", reqs)
	}
}

func TestClassicMarginStream_PublicAndPrivateWiring(t *testing.T) {
	f := newFakeKuCoin(t)
	f.ws.OnSubscribe("/indicator/markPrice:USDT-BTC", fxMarginMark)
	client := kucoin.NewClient(kucoin.WithClassicBaseURL(f.rest.URL), quick())

	session, err := client.Classic.Margin.Stream.DialPublic(testCtx(t))
	if err != nil {
		t.Fatalf("DialPublic: %v", err)
	}
	defer session.Close()
	marks, err := session.SubscribeMarkPrice(testCtx(t), []string{"USDT-BTC"})
	if err != nil {
		t.Fatalf("SubscribeMarkPrice: %v", err)
	}
	m := recv(t, marks)
	if m.Symbol != "USDT-BTC" || !sameNumber(m.Value, "0.00001182") {
		t.Fatalf("typed mark price: %+v", m)
	}
	// The Margin documentation reuses the Spot channels; a Margin session serves them too.
	if _, err := session.SubscribeTicker(testCtx(t), []string{"BTC-USDT"}); err != nil {
		t.Fatalf("an inherited Spot channel: %v", err)
	}

	if _, err := client.Classic.Margin.Stream.DialPrivate(testCtx(t)); !errors.Is(err, transport.ErrCredentialsRequired) {
		t.Fatalf("DialPrivate without credentials = %v", err)
	}
}

func TestUTAStream_PublicFuturesSessionEndToEnd(t *testing.T) {
	wstest.CheckLeaks(t)
	fake := wstest.NewUTAFake(t)
	fake.OnSubscribe("ticker", fxUTATicker)
	client := kucoin.NewClient(kucoin.WithUTAWebSocketHosts(utastreaming.Hosts{Futures: fake.URL(), Spot: fake.URL(), Private: fake.URL()}), quick())

	session, err := client.UTA.V2.Stream.DialFutures(testCtx(t))
	if err != nil {
		t.Fatalf("DialFutures: %v", err)
	}
	defer session.Close()
	ticks, err := session.SubscribeTicker(testCtx(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatalf("SubscribeTicker: %v", err)
	}
	tick := recv(t, ticks)
	if tick.Symbol != "XBTUSDTM" || tick.BestBidPrice != "90580.4" || tick.BestAskPrice != "90580.5" || tick.BestBidSize != "4852" {
		t.Fatalf("typed ticker: %+v", tick)
	}
}

func TestUTAStream_PrivateSessionAuthenticatesWithTheClientsCredentialsAndClock(t *testing.T) {
	wstest.CheckLeaks(t)
	fake := wstest.NewUTAFake(t)
	fake.OnSubscribe("balance", fxUTABalance)
	var authFrame map[string]any
	fake.OnAuth(func(m map[string]any) map[string]any {
		authFrame = m
		return map[string]any{"id": m["id"], "result": true}
	})
	client := kucoin.NewClient(
		kucoin.WithUTAWebSocketHosts(utastreaming.Hosts{Private: fake.URL()}),
		kucoin.WithCredentials(testCredentials()),
		kucoin.WithClock(fixedClock{time.UnixMilli(1700000000123)}),
		quick(),
	)
	session, err := client.UTA.V2.Stream.DialPrivate(testCtx(t))
	if err != nil {
		t.Fatalf("DialPrivate: %v", err)
	}
	defer session.Close()
	balance, err := session.SubscribeBalance(testCtx(t), utastreaming.AccountTypeUnified)
	if err != nil {
		t.Fatalf("SubscribeBalance: %v", err)
	}
	if b := recv(t, balance); b.Currency != "BTC" {
		t.Fatalf("typed balance: %+v", b)
	}
	if authFrame == nil || authFrame["op"] != "auth" || authFrame["kc-api-key"] != "test-key" {
		t.Fatalf("authentication frame: %v", authFrame)
	}
	// Timestamped by the client's clock, and signed exactly as KuCoin verifies it.
	if authFrame["kc-api-timestamp"] != "1700000000123" {
		t.Errorf("timestamp %v, want the injected clock's", authFrame["kc-api-timestamp"])
	}
	signer := auth.NewSigner("test-secret")
	if want := signer.Sign("1700000000123", "POST", "/api/websocket/users/verify", ""); authFrame["kc-api-sign"] != want {
		t.Errorf("signature %v, want %v", authFrame["kc-api-sign"], want)
	}
	if authFrame["kc-api-passphrase"] != signer.SignPassphrase("test-pass") {
		t.Errorf("the passphrase must travel signed, got %v", authFrame["kc-api-passphrase"])
	}
	raw, _ := json.Marshal(authFrame)
	if strings.Contains(string(raw), "test-secret") {
		t.Errorf("the secret must never travel: %s", raw)
	}
}

func TestUTAStream_PrivateSessionNeedsCredentialsBeforeAnyNetworkAccess(t *testing.T) {
	wstest.CheckLeaks(t)
	fake := wstest.NewUTAFake(t)
	client := kucoin.NewClient(kucoin.WithUTAWebSocketHosts(utastreaming.Hosts{Private: fake.URL()}), quick())
	if _, err := client.UTA.V2.Stream.DialPrivate(testCtx(t)); err == nil || !stream.IsPermanent(err) {
		t.Fatalf("DialPrivate without credentials = %v, want a permanent error", err)
	}
	if fake.Server.Connections() != 0 {
		t.Fatalf("no connection expected, got %d", fake.Server.Connections())
	}
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func eventually(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met: %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
