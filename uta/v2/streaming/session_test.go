package streaming

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/internal/wsengine"
	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
	"github.com/tigusigalpa/kucoin-go/types"
	"github.com/tigusigalpa/kucoin-go/websocket/uta"
)

func init() {
	wsengine.SetMinPingInterval(time.Millisecond)
	fanOutGap = time.Millisecond // production spaces the requests of a fan-out by 50ms
}

var testCreds = transport.Credentials{APIKey: "test-key", APISecret: "test-secret", APIPassphrase: "test-passphrase"}

func fastOptions() []stream.Option {
	return []stream.Option{
		stream.WithReconnect(stream.ReconnectPolicy{MinDelay: 5 * time.Millisecond, MaxDelay: 20 * time.Millisecond, Jitter: 0.1, StableAfter: time.Hour}),
		stream.WithPingInterval(time.Hour),
	}
}

// harness wires a Service to a fake KuCoin that serves every host.
type harness struct {
	fake      *wstest.UTAFake
	svc       *Service
	snapshots func(ctx context.Context, tradeType, symbol string) (orderbook.Snapshot, error)
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	wstest.CheckLeaks(t)
	h := &harness{fake: wstest.NewUTAFake(t)}
	snap := func(ctx context.Context, tradeType, symbol string) (orderbook.Snapshot, error) {
		if h.snapshots == nil {
			return orderbook.Snapshot{}, errors.New("no snapshot scripted")
		}
		return h.snapshots(ctx, tradeType, symbol)
	}
	creds := testCreds
	url := h.fake.URL()
	h.svc = NewService(Hosts{Spot: url, Futures: url, Private: url}, &creds, snap, fastOptions()...)
	return h
}

func (h *harness) dial(t *testing.T, dial func(context.Context, ...stream.Option) (*Session, error)) *Session {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := dial(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func (h *harness) futures(t *testing.T) *Session { return h.dial(t, h.svc.DialFutures) }
func (h *harness) spot(t *testing.T) *Session    { return h.dial(t, h.svc.DialSpot) }
func (h *harness) private(t *testing.T) *Session { return h.dial(t, h.svc.DialPrivate) }

// frames returns the subscribe/unsubscribe frames the server received for a channel.
func (h *harness) frames(action, channel string) []map[string]any {
	var out []map[string]any
	for _, m := range h.fake.Frames() {
		if strings.EqualFold(wstest.Str(m, "action"), action) && wstest.Str(m, "channel") == channel {
			out = append(out, m)
		}
	}
	return out
}

func next[T any](t *testing.T, sub *stream.Subscription[T]) T {
	t.Helper()
	select {
	case v, ok := <-sub.C():
		if !ok {
			t.Fatalf("subscription closed (err=%v)", sub.Err())
		}
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an update")
		var zero T
		return zero
	}
}

func expectNothing[T any](t *testing.T, sub *stream.Subscription[T], what string) {
	t.Helper()
	select {
	case v := <-sub.C():
		t.Fatalf("%s: unexpected update %+v", what, v)
	case <-time.After(60 * time.Millisecond):
	}
}

func eventually(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met: %s", what)
		}
		time.Sleep(3 * time.Millisecond)
	}
}

func ctx5(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func equal(t *testing.T, name string, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s:\n got  %+v\n want %+v", name, got, want)
	}
}

func lv(price, size string) orderbook.Level {
	return orderbook.Level{Price: types.Decimal(price), Size: types.Decimal(size)}
}

func TestTicker(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("ticker", fxTickerFutures, fxTickerVariant)
	sub, err := h.futures(t).SubscribeTicker(ctx5(t), []string{"XBTUSDTM", "ETHUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	// Two symbols travel in one request for this channel.
	frames := h.frames("subscribe", "ticker")
	if len(frames) != 1 || frames[0]["tradeType"] != "FUTURES" || fmt.Sprint(frames[0]["symbols"]) != "[XBTUSDTM ETHUSDTM]" || frames[0]["symbol"] != nil {
		t.Fatalf("subscribe frames: %v", frames)
	}
	equal(t, "futures ticker", next(t, sub), Ticker{
		Symbol: "XBTUSDTM", TradeType: TradeTypeFutures, Sequence: 1905974001288,
		BestBidPrice: "90580.4", BestBidSize: "4852", BestAskPrice: "90580.5", BestAskSize: "36",
		LastPrice: "90580.5", LastSize: "3", Side: SideBuy,
		MatchTimestamp: 1768218267868000000, GatewayTimestamp: 1768218267869446269,
	})
	v := next(t, sub)
	equal(t, "variant ticker", v, Ticker{
		Symbol: "ETHUSDTM", TradeType: TradeTypeFutures, Sequence: 42,
		BestBidPrice: "2000.5", BestBidSize: "7", BestAskPrice: "2000.6", BestAskSize: "9",
		LastPrice: "2000.55", LastSize: "1.5", Side: SideSell, MatchTimestamp: 6, GatewayTimestamp: 5,
	})
	if v.Time().UnixNano() != 6 || v.GatewayTime().UnixNano() != 5 {
		t.Fatalf("times: %v %v", v.Time(), v.GatewayTime())
	}
	if sub.Key() != (uta.SubscribeSpec{Channel: "ticker", TradeType: "FUTURES", Symbols: []string{"XBTUSDTM", "ETHUSDTM"}}).Name() {
		t.Fatalf("key = %q", sub.Key())
	}
}

func TestTicker_Spot(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("ticker", fxTickerSpot)
	s := h.spot(t)
	if s.TradeType() != "SPOT" {
		t.Fatalf("trade type = %q", s.TradeType())
	}
	sub, err := s.SubscribeTicker(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	frame := h.frames("subscribe", "ticker")[0]
	if frame["tradeType"] != "SPOT" || frame["symbol"] != "BTC-USDT" || frame["symbols"] != nil {
		t.Fatalf("subscribe frame: %v", frame)
	}
	// The spot gateway writes the side in upper case; Side is upper case for both.
	equal(t, "spot ticker", next(t, sub), Ticker{
		Symbol: "BTC-USDT", TradeType: TradeTypeSpot, Sequence: 25958853459,
		BestBidPrice: "90968.1", BestBidSize: "0.02052839", BestAskPrice: "90968.2", BestAskSize: "0.97675941",
		LastPrice: "90968.2", LastSize: "0.00109929", Side: SideBuy,
		MatchTimestamp: 1768206966096000000, GatewayTimestamp: 1768206966101166007,
	})
}

func TestTrades(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("trade|XBTUSDTM", fxTradeFutures)
	h.fake.OnSubscribe("trade|ETHUSDTM", fxTradeVariant)
	h.fake.OnSubscribe("trade|BTC-USDT", fxTradeSpot)
	fut := h.futures(t)
	sub, err := fut.SubscribeTrades(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	equal(t, "futures trade", next(t, sub), Trade{
		Symbol: "XBTUSDTM", TradeType: TradeTypeFutures, Sequence: 1905973690439, TradeID: "1905973690439",
		Price: "90551.8", Size: "12", Side: SideBuy, RPI: false,
		MatchTimestamp: 1768218157097000000, GatewayTimestamp: 1768218157098477768,
	})
	rpi, err := fut.SubscribeTrades(ctx5(t), []string{"ETHUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, rpi)
	equal(t, "rpi trade", v, Trade{
		Symbol: "ETHUSDTM", TradeType: TradeTypeFutures, Sequence: 3, TradeID: "777",
		Price: "2000.1", Size: "4", Side: SideSell, RPI: true, MatchTimestamp: 2, GatewayTimestamp: 8,
	})
	if v.Time().UnixNano() != 2 || v.GatewayTime().UnixNano() != 8 {
		t.Fatalf("times: %v %v", v.Time(), v.GatewayTime())
	}

	spot, err := h.spot(t).SubscribeTrades(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	// The spot sequence is a 17-digit integer: it must not be rounded through float64.
	equal(t, "spot trade", next(t, spot), Trade{
		Symbol: "BTC-USDT", TradeType: TradeTypeSpot, Sequence: 20631804219899904, TradeID: "20631804219899904",
		Price: "92525.6", Size: "0.00008036", Side: SideSell,
		MatchTimestamp: 1768802538480000000, GatewayTimestamp: 1768802538491160904,
	})
}

func TestKlines(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("kline|XBTUSDTM|1min", fxKlineFutures)
	h.fake.OnSubscribe("kline|BTC-USDT|1min", fxKlineSpot)
	h.fake.OnSubscribe("kline|ETH-USDT|5min", fxKlineVariant)
	fut := h.futures(t)
	sub, err := fut.SubscribeKlines(ctx5(t), Interval1Min, []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	if f := h.frames("subscribe", "kline")[0]; f["interval"] != "1min" || f["symbol"] != "XBTUSDTM" || f["tradeType"] != "FUTURES" {
		t.Fatalf("subscribe frame: %v", f)
	}
	equal(t, "futures kline", next(t, sub), Kline{
		Symbol: "XBTUSDTM", TradeType: TradeTypeFutures, Interval: Interval1Min,
		StartTimestamp: 1768803900, EndTimestamp: 1768803960,
		Open: "92505.1", Close: "92551.6", High: "92551.7", Low: "92505.1", Volume: "734", Amount: "67919.2142",
		First: true, GatewayTimestamp: 1768803943947934451,
	})

	spot := h.spot(t)
	spotSub, err := spot.SubscribeKlines(ctx5(t), Interval1Min, []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	k := next(t, spotSub)
	equal(t, "spot kline", k, Kline{
		Symbol: "BTC-USDT", TradeType: TradeTypeSpot, Interval: Interval1Min,
		StartTimestamp: 1776090720, EndTimestamp: 1776090780,
		Open: "71725.1", Close: "71728.1", High: "71728.1", Low: "71725.1", Volume: "0.01504768", Amount: "1079.297822768",
		First: false, GatewayTimestamp: 1776090720846219590,
	})
	if k.Start().Unix() != 1776090720 || k.End().Unix() != 1776090780 || k.Time().UnixNano() != 1776090720846219590 {
		t.Fatalf("times: %v %v %v", k.Start(), k.End(), k.Time())
	}
	// Distinct open/close/high/low prove the mapping; the 6hour candle exists on spot.
	five, err := spot.SubscribeKlines(ctx5(t), Interval5Min, []string{"ETH-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	equal(t, "variant kline", next(t, five), Kline{
		Symbol: "ETH-USDT", TradeType: TradeTypeSpot, Interval: Interval5Min, StartTimestamp: 100, EndTimestamp: 160,
		Open: "1", Close: "2", High: "3", Low: "0.5", Volume: "11", Amount: "12", First: true, GatewayTimestamp: 9,
	})
	if _, err := spot.SubscribeKlines(ctx5(t), Interval6Hour, []string{"SOL-USDT"}); err != nil {
		t.Fatalf("6hour on spot: %v", err)
	}
}

func TestOrderBookUpdates_AllDepths(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("obu|XBTUSDTM|1", fxObuFuturesBBO)
	h.fake.OnSubscribe("obu|XBTUSDTM|increment", fxObuFuturesIncrement)
	h.fake.OnSubscribe("obu|XBTUSDTM|increment@10ms", fxObuFutures10ms)
	h.fake.OnSubscribe("obu|SOLUSDTM|50", fxObuFutures50Live)
	h.fake.OnSubscribe("obu|BTC-USDT|1", fxObuSpotBBO)
	h.fake.OnSubscribe("obu|BTC-USDT|increment", fxObuSpotIncrement)
	h.fake.OnSubscribe("obu|BTC-USDT|increment@10ms", fxObuSpot10ms)
	h.fake.OnSubscribe("obu|ETH-USDT|5", fxObuSpot5Live)
	fut := h.futures(t)
	ctx := ctx5(t)

	bbo, err := fut.SubscribeOrderBookUpdates(ctx, []string{"XBTUSDTM"}, Depth1)
	if err != nil {
		t.Fatal(err)
	}
	if f := h.frames("subscribe", "obu")[0]; f["depth"] != "1" || f["symbol"] != "XBTUSDTM" || f["tradeType"] != "FUTURES" || f["rpiFilter"] != nil {
		t.Fatalf("subscribe frame: %v", f)
	}
	u := next(t, bbo)
	equal(t, "futures bbo", u, OrderBookUpdate{
		Symbol: "XBTUSDTM", TradeType: TradeTypeFutures, Depth: Depth1, Kind: UpdateSnapshot,
		StartSequence: 1732420529826, EndSequence: 1732420529826, MatchTimestamp: 1768217994548000000, GatewayTimestamp: 1768217994549513415,
		Bids: []orderbook.Level{lv("90629.1", "301")}, Asks: []orderbook.Level{lv("90629.2", "2236")},
	})
	if !u.IsSnapshot() || u.Time().UnixNano() != 1768217994548000000 || u.GatewayTime().UnixNano() != 1768217994549513415 {
		t.Fatalf("snapshot flags/times: %v %v", u.Time(), u.GatewayTime())
	}
	snap := u.ToSnapshot()
	equal(t, "ToSnapshot", snap, orderbook.Snapshot{Symbol: "XBTUSDTM", Sequence: 1732420529826, Bids: u.Bids, Asks: u.Asks})
	snap.Bids[0].Size = "changed" // the snapshot owns its levels
	if u.Bids[0].Size != "301" {
		t.Fatal("ToSnapshot must copy the levels")
	}

	inc, err := fut.SubscribeOrderBookUpdates(ctx, []string{"XBTUSDTM"}, DepthIncrement)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, "futures increment", next(t, inc), OrderBookUpdate{
		Symbol: "XBTUSDTM", TradeType: TradeTypeFutures, Depth: DepthIncrement, Kind: UpdateDelta,
		StartSequence: 1732420557695, EndSequence: 1732420557695, MatchTimestamp: 1768218044321000000, GatewayTimestamp: 1768218044321756061,
		Bids: []orderbook.Level{}, Asks: []orderbook.Level{lv("90601.5", "0")},
	})

	ten, err := fut.SubscribeOrderBookUpdates(ctx, []string{"XBTUSDTM"}, DepthIncrement10ms)
	if err != nil {
		t.Fatal(err)
	}
	d := next(t, ten)
	equal(t, "futures 10ms", d, OrderBookUpdate{
		Symbol: "XBTUSDTM", TradeType: TradeTypeFutures, Depth: DepthIncrement10ms, Kind: UpdateDelta,
		StartSequence: 1743938538050, EndSequence: 1743938538056, MatchTimestamp: 1781666796658000000, GatewayTimestamp: 1781666796660937177,
		Bids: []orderbook.Level{lv("65739.9", "0"), lv("65739", "14")}, Asks: []orderbook.Level{},
	})
	if d.IsSnapshot() {
		t.Fatal("a delta is not a snapshot")
	}
	equal(t, "ToDelta", d.ToDelta(), orderbook.Delta{Symbol: "XBTUSDTM", Start: 1743938538050, End: 1743938538056, Changes: []orderbook.Change{
		{Side: orderbook.Bid, Price: "65739.9", Size: "0"}, {Side: orderbook.Bid, Price: "65739", Size: "14"},
	}})

	fifty, err := fut.SubscribeOrderBookUpdates(ctx, []string{"SOLUSDTM"}, Depth50)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, "futures 50", next(t, fifty).Bids, []orderbook.Level{lv("118.631", "293"), lv("118.63", "100")})

	spot := h.spot(t)
	spotBBO, err := spot.SubscribeOrderBookUpdates(ctx, []string{"BTC-USDT"}, Depth1)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, "spot bbo", next(t, spotBBO), OrderBookUpdate{
		Symbol: "BTC-USDT", TradeType: TradeTypeSpot, Depth: Depth1, Kind: UpdateSnapshot,
		StartSequence: 25984468414, EndSequence: 25984468414, MatchTimestamp: 1768217874986000000, GatewayTimestamp: 1768217874990007701,
		Bids: []orderbook.Level{lv("90701.1", "0.13918404")}, Asks: []orderbook.Level{lv("90701.2", "0.5771583")},
	})
	spotInc, err := spot.SubscribeOrderBookUpdates(ctx, []string{"BTC-USDT"}, DepthIncrement)
	if err != nil {
		t.Fatal(err)
	}
	if v := next(t, spotInc); v.Bids[0] != lv("1", "12996.24994153") || v.StartSequence != 25984544839 || v.EndSequence != 25984544840 {
		t.Fatalf("spot increment: %+v", v)
	}
	spot10, err := spot.SubscribeOrderBookUpdates(ctx, []string{"BTC-USDT"}, DepthIncrement10ms)
	if err != nil {
		t.Fatal(err)
	}
	if v := next(t, spot10); len(v.Bids) != 3 || v.Bids[2] != lv("65947", "0.06185847") || v.StartSequence != 33629695388 || v.EndSequence != 33629695393 {
		t.Fatalf("spot 10ms: %+v", v)
	}
	five, err := spot.SubscribeOrderBookUpdates(ctx, []string{"ETH-USDT"}, Depth5)
	if err != nil {
		t.Fatal(err)
	}
	if v := next(t, five); len(v.Bids) != 5 || len(v.Asks) != 5 || v.Asks[4] != lv("2669.71", "1.6184204") || v.Depth != Depth5 {
		t.Fatalf("spot 5: %+v", v)
	}
}

func TestOrderBookUpdatesRPI(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("obu|XBTUSDTM|5", fxObuFuturesRPI, fxObuFuturesRPILive)
	sub, err := h.futures(t).SubscribeOrderBookUpdatesRPI(ctx5(t), []string{"XBTUSDTM"}, Depth5)
	if err != nil {
		t.Fatal(err)
	}
	f := h.frames("subscribe", "obu")[0]
	if f["rpiFilter"] != float64(1) || f["depth"] != "5" || f["symbol"] != "XBTUSDTM" {
		t.Fatalf("subscribe frame: %v", f)
	}
	// The documentation's RPI example: three-element levels.
	equal(t, "rpi example", next(t, sub), OrderBookUpdate{
		Symbol: "XBTUSDTM", TradeType: TradeTypeFutures, Depth: Depth5, Kind: UpdateSnapshot,
		StartSequence: 1731931329201, EndSequence: 1731931329201, MatchTimestamp: 1767323800296000000, GatewayTimestamp: 1767323800305802104,
		Bids: []orderbook.Level{{Price: "88862.4", Size: "891", RPISize: "0"}},
		Asks: []orderbook.Level{{Price: "88862.5", Size: "1645", RPISize: "0"}},
	})
	live := next(t, sub)
	equal(t, "live bids", live.Bids, []orderbook.Level{{Price: "84516.2", Size: "178", RPISize: "156"}, {Price: "84516.1", Size: "33", RPISize: "0"}})
	equal(t, "live asks", live.Asks, []orderbook.Level{{Price: "84516.3", Size: "457", RPISize: "0"}, {Price: "84516.4", Size: "0", RPISize: "449"}})
	// The RPI size is kept in snapshots and dropped from deltas, which have no place for it.
	if live.ToSnapshot().Asks[1].RPISize != "449" {
		t.Fatal("ToSnapshot keeps the RPI size")
	}
	if c := live.ToDelta().Changes; len(c) != 4 || c[3] != (orderbook.Change{Side: orderbook.Ask, Price: "84516.4", Size: "0"}) {
		t.Fatalf("ToDelta: %+v", c)
	}
}

func TestMarkPrice(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("mark-price|XBTUSDTM", fxMarkPrice)
	h.fake.OnSubscribe("mark-price|ETHUSDTM", fxMarkPriceLive)
	sub, err := h.futures(t).SubscribeMarkPrice(ctx5(t), []string{"XBTUSDTM", "ETHUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	// KuCoin rejects "symbols" for this channel: one request per symbol, no trade type.
	frames := h.frames("subscribe", "mark-price")
	if len(frames) != 2 {
		t.Fatalf("subscribe frames: %v", frames)
	}
	for _, f := range frames {
		if f["symbols"] != nil || f["symbol"] == nil || f["tradeType"] != nil {
			t.Fatalf("subscribe frame: %v", f)
		}
	}
	got := map[string]MarkPrice{}
	for i := 0; i < 2; i++ {
		v := next(t, sub)
		got[v.Symbol] = v
	}
	equal(t, "published example", got["XBTUSDTM"], MarkPrice{
		Symbol: "XBTUSDTM", MarkPrice: "90445", IndexPrice: "90445", OpenInterest: "23445", Timestamp: 1731899128999, GatewayTimestamp: 1731899129000,
	})
	equal(t, "live frame", got["ETHUSDTM"], MarkPrice{
		Symbol: "ETHUSDTM", MarkPrice: "84517.2", IndexPrice: "84564.02", OpenInterest: "11793380", Timestamp: 1790984885000, GatewayTimestamp: 1790984885234256943,
	})
	// The published example's P is in milliseconds, the live one in nanoseconds.
	if ms := got["XBTUSDTM"]; ms.Time().UnixMilli() != 1731899128999 || ms.GatewayTime().UnixMilli() != 1731899129000 {
		t.Fatalf("times: %v %v", ms.Time(), ms.GatewayTime())
	}
	if ns := got["ETHUSDTM"]; ns.GatewayTime().UnixNano() != 1790984885234256943 {
		t.Fatalf("gateway time: %v", ns.GatewayTime())
	}
	if sub.Key() != (uta.SubscribeSpec{Channel: "mark-price", Symbols: []string{"XBTUSDTM", "ETHUSDTM"}}).Name() {
		t.Fatalf("key = %q", sub.Key())
	}
}

func TestFundingRate(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("funding-fee", fxFundingRate)
	sub, err := h.futures(t).SubscribeFundingRate(ctx5(t), []string{"XBTUSDTM", "ETHUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	// Several symbols travel in one request for this channel.
	frames := h.frames("subscribe", "funding-fee")
	if len(frames) != 1 || fmt.Sprint(frames[0]["symbols"]) != "[XBTUSDTM ETHUSDTM]" || frames[0]["tradeType"] != nil {
		t.Fatalf("subscribe frames: %v", frames)
	}
	v := next(t, sub)
	equal(t, "funding rate", v, FundingRate{
		Symbol: "XBTUSDTM", Rate: "0.000072", LastRate: "0.000069", MaxRate: "0.003", MinRate: "-0.003",
		LastSettlementTimestamp: 1786809600000, NextSettlementTimestamp: 1786838400000, IntervalMillis: 28800000,
		GatewayTimestamp: 1786809901214267234,
	})
	if v.LastSettlement().UnixMilli() != 1786809600000 || v.NextSettlement().UnixMilli() != 1786838400000 ||
		v.Interval() != 8*time.Hour || v.Time().UnixNano() != 1786809901214267234 {
		t.Fatalf("times: %v %v %v %v", v.LastSettlement(), v.NextSettlement(), v.Interval(), v.Time())
	}
}

func TestAllFundingRates(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("funding-fee-all-symbols", fxFundingAll)
	sub, err := h.futures(t).SubscribeAllFundingRates(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if f := h.frames("subscribe", "funding-fee-all-symbols")[0]; f["symbol"] != nil || f["symbols"] != nil || f["tradeType"] != nil {
		t.Fatalf("subscribe frame: %v", f)
	}
	all := next(t, sub)
	const pushed = 1786809901214267234
	equal(t, "all funding rates", all, AllFundingRates{
		GatewayTimestamp: pushed,
		Rates: []FundingRate{
			{Symbol: "XBTUSDTM", Rate: "0.000072", LastRate: "0.000069", MaxRate: "0.003", MinRate: "-0.003",
				LastSettlementTimestamp: 1786809600000, NextSettlementTimestamp: 1786838400000, IntervalMillis: 28800000, GatewayTimestamp: pushed},
			{Symbol: "ETHUSDTM", Rate: "0.000041", LastRate: "0.000039", MaxRate: "0.003", MinRate: "-0.003",
				LastSettlementTimestamp: 1786809600000, NextSettlementTimestamp: 1786838400000, IntervalMillis: 28800000, GatewayTimestamp: pushed},
		},
	})
	if all.Time().UnixNano() != pushed {
		t.Fatalf("Time() = %v", all.Time())
	}
}

func TestCallAuction(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("callAuctionInfo|GROVE-USDT", fxCallAuction, fxCallAuctionVariant)
	h.fake.OnSubscribe("callAuctionInfo|BTC-USDT")
	sub, err := h.spot(t).SubscribeCallAuction(ctx5(t), []string{"GROVE-USDT", "BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	// KuCoin rejects "symbols" for this channel: one request per symbol, no trade type.
	frames := h.frames("subscribe", "callAuctionInfo")
	if len(frames) != 2 {
		t.Fatalf("subscribe frames: %v", frames)
	}
	for _, f := range frames {
		if f["symbols"] != nil || f["symbol"] == nil || f["tradeType"] != nil {
			t.Fatalf("subscribe frame: %v", f)
		}
	}
	// The documented example: JSON numbers, and "eq" where the schema says "ep".
	first := next(t, sub)
	equal(t, "example", first, CallAuctionInfo{
		Symbol: "GROVE-USDT", Kind: UpdateSnapshot, EstimatedPrice: "0.05568", EstimatedSize: "9099.4",
		SellLowPrice: "0.05568", SellHighPrice: "0.19839", BuyLowPrice: "0.00006", BuyHighPrice: "0.19",
		Timestamp: 1783434842521, GatewayTimestamp: 1783434842522281439,
	})
	if first.Time().UnixMilli() != 1783434842521 || first.GatewayTime().UnixNano() != 1783434842522281439 {
		t.Fatalf("times: %v %v", first.Time(), first.GatewayTime())
	}
	equal(t, "schema spelling", next(t, sub), CallAuctionInfo{
		Symbol: "GROVE-USDT", Kind: UpdateSnapshot, EstimatedPrice: "0.06", EstimatedSize: "100.5",
		SellLowPrice: "0.05", SellHighPrice: "0.3", BuyLowPrice: "0.0001", BuyHighPrice: "0.2",
		Timestamp: 1783434842621, GatewayTimestamp: 1783434842622281439,
	})
}

func TestLocalValidationHappensBeforeAnythingIsSent(t *testing.T) {
	h := newHarness(t)
	fut, spot, priv := h.futures(t), h.spot(t), h.private(t)
	ctx := ctx5(t)
	tooMany := make([]string, MaxSymbolsPerSubscription+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("SYM%dUSDTM", i)
	}
	cases := []struct {
		name string
		call func() error
		want error
	}{
		{"no symbols", func() error { _, err := fut.SubscribeTicker(ctx, nil); return err }, ErrNoSymbols},
		{"too many symbols", func() error { _, err := fut.SubscribeTrades(ctx, tooMany); return err }, ErrTooManySymbols},
		{"empty symbol", func() error { _, err := fut.SubscribeTrades(ctx, []string{""}); return err }, ErrInvalidSymbol},
		{"comma in symbol", func() error { _, err := fut.SubscribeTicker(ctx, []string{"A,B"}); return err }, ErrInvalidSymbol},
		{"space in symbol", func() error { _, err := fut.SubscribeKlines(ctx, Interval1Min, []string{"A B"}); return err }, ErrInvalidSymbol},
		{"bad interval", func() error { _, err := fut.SubscribeKlines(ctx, "2min", []string{"XBTUSDTM"}); return err }, ErrInvalidInterval},
		{"empty interval", func() error { _, err := spot.SubscribeKlines(ctx, "", []string{"BTC-USDT"}); return err }, ErrInvalidInterval},
		{"6hour on futures", func() error { _, err := fut.SubscribeKlines(ctx, Interval6Hour, []string{"XBTUSDTM"}); return err }, ErrInvalidInterval},
		{"bad depth", func() error { _, err := fut.SubscribeOrderBookUpdates(ctx, []string{"XBTUSDTM"}, "7"); return err }, ErrInvalidDepth},
		{"rpi on spot", func() error {
			_, err := spot.SubscribeOrderBookUpdatesRPI(ctx, []string{"BTC-USDT"}, Depth5)
			return err
		}, ErrFuturesOnly},
		{"rpi with depth 1", func() error {
			_, err := fut.SubscribeOrderBookUpdatesRPI(ctx, []string{"XBTUSDTM"}, Depth1)
			return err
		}, ErrInvalidDepth},
		{"rpi with depth 10ms", func() error {
			_, err := fut.SubscribeOrderBookUpdatesRPI(ctx, []string{"XBTUSDTM"}, DepthIncrement10ms)
			return err
		}, ErrInvalidDepth},
		{"rpi bad depth", func() error { _, err := fut.SubscribeOrderBookUpdatesRPI(ctx, []string{"XBTUSDTM"}, "x"); return err }, ErrInvalidDepth},
		{"mark price on spot", func() error { _, err := spot.SubscribeMarkPrice(ctx, []string{"BTC-USDT"}); return err }, ErrFuturesOnly},
		{"funding rate on spot", func() error { _, err := spot.SubscribeFundingRate(ctx, []string{"BTC-USDT"}); return err }, ErrFuturesOnly},
		{"all funding rates on spot", func() error { _, err := spot.SubscribeAllFundingRates(ctx); return err }, ErrFuturesOnly},
		{"call auction on futures", func() error { _, err := fut.SubscribeCallAuction(ctx, []string{"GROVE-USDT"}); return err }, ErrSpotOnly},
		{"call auction without symbols", func() error { _, err := spot.SubscribeCallAuction(ctx, nil); return err }, ErrNoSymbols},
		{"mark price without symbols", func() error { _, err := fut.SubscribeMarkPrice(ctx, nil); return err }, ErrNoSymbols},
		{"funding rate without symbols", func() error { _, err := fut.SubscribeFundingRate(ctx, nil); return err }, ErrNoSymbols},
		{"order book without symbols", func() error { _, err := fut.SubscribeOrderBookUpdates(ctx, nil, Depth5); return err }, ErrNoSymbols},
		{"order book bad symbol", func() error { _, err := fut.SubscribeOrderBook(ctx, "A,B"); return err }, ErrInvalidSymbol},
		{"increment book bad symbol", func() error { _, err := fut.SubscribeOrderBookIncrement(ctx, ""); return err }, ErrInvalidSymbol},
		{"increment book on a private session", func() error { _, err := priv.SubscribeOrderBookIncrement(ctx, "X"); return err }, ErrWrongSession},
		{"balance bad account", func() error { _, err := priv.SubscribeBalance(ctx, "SPOT"); return err }, ErrInvalidAccountType},
		{"balance empty account", func() error { _, err := priv.SubscribeBalance(ctx, ""); return err }, ErrInvalidAccountType},
		{"orders bad symbol", func() error { _, err := priv.SubscribeOrders(ctx, "A B"); return err }, ErrInvalidSymbol},
		{"positions bad symbol", func() error { _, err := priv.SubscribePositions(ctx, "A,B"); return err }, ErrInvalidSymbol},

		// private channels on public sessions
		{"orders on futures", func() error { _, err := fut.SubscribeOrders(ctx, ""); return err }, ErrPrivateSessionRequired},
		{"orders on spot", func() error { _, err := spot.SubscribeOrders(ctx, "BTC-USDT"); return err }, ErrPrivateSessionRequired},
		{"executions on futures", func() error { _, err := fut.SubscribeExecutions(ctx); return err }, ErrPrivateSessionRequired},
		{"executions lite on futures", func() error { _, err := fut.SubscribeExecutionsLite(ctx); return err }, ErrPrivateSessionRequired},
		{"balance on futures", func() error { _, err := fut.SubscribeBalance(ctx, AccountTypeUnified); return err }, ErrPrivateSessionRequired},
		{"positions on futures", func() error { _, err := fut.SubscribePositions(ctx, "XBTUSDTM"); return err }, ErrPrivateSessionRequired},
		{"liquidation warning on futures", func() error { _, err := fut.SubscribeLiquidationWarning(ctx); return err }, ErrPrivateSessionRequired},
		{"leverage on futures", func() error { _, err := fut.SubscribeLeverage(ctx); return err }, ErrPrivateSessionRequired},

		// market data on a private session
		{"ticker on private", func() error { _, err := priv.SubscribeTicker(ctx, []string{"X"}); return err }, ErrWrongSession},
		{"trades on private", func() error { _, err := priv.SubscribeTrades(ctx, []string{"X"}); return err }, ErrWrongSession},
		{"klines on private", func() error { _, err := priv.SubscribeKlines(ctx, Interval1Min, []string{"X"}); return err }, ErrWrongSession},
		{"order book updates on private", func() error { _, err := priv.SubscribeOrderBookUpdates(ctx, []string{"X"}, Depth5); return err }, ErrWrongSession},
		{"order book on private", func() error { _, err := priv.SubscribeOrderBook(ctx, "X"); return err }, ErrWrongSession},
		{"order book tuned on private", func() error {
			_, err := priv.SubscribeOrderBookTuned(ctx, "X", orderbook.Tuning{})
			return err
		}, ErrWrongSession},
		{"mark price on private", func() error { _, err := priv.SubscribeMarkPrice(ctx, []string{"X"}); return err }, ErrWrongSession},
		{"funding rate on private", func() error { _, err := priv.SubscribeFundingRate(ctx, []string{"X"}); return err }, ErrWrongSession},
		{"all funding rates on private", func() error { _, err := priv.SubscribeAllFundingRates(ctx); return err }, ErrWrongSession},
		{"call auction on private", func() error { _, err := priv.SubscribeCallAuction(ctx, []string{"X"}); return err }, ErrWrongSession},
	}
	for _, tc := range cases {
		if err := tc.call(); !errors.Is(err, tc.want) {
			t.Errorf("%s: error = %v, want %v", tc.name, err, tc.want)
		}
	}
	if n := h.fake.Count("subscribe", ""); n != 0 {
		t.Fatalf("%d subscribe frames reached the server; validation must happen locally", n)
	}
}

func TestSymbolsAreDeduplicated(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("trade")
	h.fake.OnSubscribe("ticker")
	fut := h.futures(t)
	// A duplicate would be routed, and therefore delivered, twice.
	trades, err := fut.SubscribeTrades(ctx5(t), []string{"XBTUSDTM", "XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	tickers, err := fut.SubscribeTicker(ctx5(t), []string{"XBTUSDTM", "ETHUSDTM", "XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(h.frames("subscribe", "trade")); n != 1 {
		t.Fatalf("%d trade requests for one distinct symbol", n)
	}
	if f := h.frames("subscribe", "ticker")[0]; fmt.Sprint(f["symbols"]) != "[XBTUSDTM ETHUSDTM]" {
		t.Fatalf("ticker frame: %v", f)
	}
	if err := h.fake.Push(fxTradeFutures); err != nil {
		t.Fatal(err)
	}
	next(t, trades)
	expectNothing(t, trades, "a duplicate symbol")
	if err := h.fake.Push(fxTickerFutures); err != nil {
		t.Fatal(err)
	}
	next(t, tickers)
	expectNothing(t, tickers, "a duplicate symbol")
}

func TestServerRejectionIsTyped(t *testing.T) {
	h := newHarness(t)
	h.fake.Reject("ticker|NOSUCH", "invalid request data")
	h.fake.Reject("balance", "private topic=Balance is not allowed")
	_, err := h.futures(t).SubscribeTicker(ctx5(t), []string{"NOSUCH"})
	var se *stream.ServerError
	if !errors.Is(err, uta.ErrSubscriptionFailed) || !errors.As(err, &se) || se.Message != "invalid request data" {
		t.Fatalf("error = %v", err)
	}
	if _, err := h.private(t).SubscribeBalance(ctx5(t), AccountTypeUnified); !errors.Is(err, uta.ErrSubscriptionFailed) {
		t.Fatalf("error = %v", err)
	}
}

func TestOverlappingSubscriptionsAreRejectedLocally(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("trade")
	fut := h.futures(t)
	if _, err := fut.SubscribeTrades(ctx5(t), []string{"XBTUSDTM", "ETHUSDTM"}); err != nil {
		t.Fatal(err)
	}
	before := h.fake.Count("subscribe", "trade")
	// SOL would be subscribed first, then ETH overlaps: the rollback unsubscribes SOL.
	_, err := fut.SubscribeTrades(ctx5(t), []string{"SOLUSDTM", "ETHUSDTM"})
	if !errors.Is(err, stream.ErrAlreadySubscribed) {
		t.Fatalf("error = %v", err)
	}
	if n := h.fake.Count("subscribe", "trade") - before; n != 1 {
		t.Fatalf("%d requests sent for the overlapping subscription; only the symbol before the overlap may be", n)
	}
	eventually(t, func() bool { return h.fake.Count("unsubscribe", "trade") == 1 }, "the symbol subscribed before the overlap is unsubscribed again")
	if got := fut.Stats().Subscriptions; got != 2 {
		t.Fatalf("subscriptions = %d, want only the first call's two", got)
	}
}

func TestSessionReconnectsAndRestoresSubscriptions(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("ticker")
	s := h.futures(t)
	sub, err := s.SubscribeTicker(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	h.fake.Server.DropAll()
	eventually(t, func() bool {
		return s.State() == stream.StateConnected && h.fake.Server.Connections() == 2 && h.fake.Count("subscribe", "") == 2
	}, "reconnect and resubscribe")
	if err := h.fake.Push(fxTickerFutures); err != nil {
		t.Fatal(err)
	}
	if v := next(t, sub); v.Symbol != "XBTUSDTM" {
		t.Fatalf("ticker after reconnect: %+v", v)
	}
	var kinds []stream.EventType
	for len(kinds) < 4 {
		select {
		case ev := <-s.Events():
			kinds = append(kinds, ev.Type)
		case <-time.After(5 * time.Second):
			t.Fatalf("events: %v", kinds)
		}
	}
	want := []stream.EventType{stream.EventConnected, stream.EventDisconnected, stream.EventReconnecting, stream.EventReconnected}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("events = %v, want %v", kinds, want)
	}
	if st := s.Stats(); st.Reconnects != 1 || st.Subscriptions != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestSessionCloseEndsEverythingWithoutLeaks(t *testing.T) {
	h := newHarness(t)
	s := h.futures(t)
	a, err := s.SubscribeTicker(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.SubscribeTrades(ctx5(t), []string{"XBTUSDTM", "ETHUSDTM"}) // fanned out and merged
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	for name, closed := range map[string]func() bool{
		"ticker": func() bool { _, ok := <-a.C(); return !ok },
		"trades": func() bool { _, ok := <-b.C(); return !ok },
	} {
		done := make(chan bool, 1)
		go func() { done <- closed() }()
		select {
		case ok := <-done:
			if !ok {
				t.Fatalf("%s channel stayed open after Close", name)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s channel not closed", name)
		}
	}
	if a.Err() != nil || b.Err() != nil {
		t.Fatalf("a requested Close is not an error: %v %v", a.Err(), b.Err())
	}
	select {
	case <-s.Done():
	default:
		t.Fatal("Done must be closed")
	}
	if s.Err() != nil || s.Client() == nil {
		t.Fatalf("Err=%v", s.Err())
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubscribeTicker(ctx5(t), []string{"XBTUSDTM"}); !errors.Is(err, stream.ErrClosed) {
		t.Fatalf("subscribing on a closed session: %v", err)
	}
}

func TestSubscriptionCloseStopsDeliveryAndUnsubscribes(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("trade|XBTUSDTM", fxTradeFutures)
	s := h.futures(t)
	sub, err := s.SubscribeTrades(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	next(t, sub)
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if h.fake.Count("unsubscribe", "trade") != 1 {
		t.Fatal("the server never saw the unsubscribe")
	}
	if _, ok := <-sub.C(); ok {
		t.Fatal("C must be closed")
	}
	if _, err := s.SubscribeTrades(ctx5(t), []string{"XBTUSDTM"}); err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
}

func TestMalformedPushIsReportedAndTheStreamContinues(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("ticker|XBTUSDTM",
		`{"T":"ticker.FUTURES","P":1,"d":{"s":"XBTUSDTM","E":"not a number"}}`,
		`{"T":"ticker.FUTURES","P":1,"d":{"s":"XBTUSDTM","b":{"nested":true}}}`,
		fxTickerFutures)
	s := h.futures(t)
	sub, err := s.SubscribeTicker(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	if v := next(t, sub); v.BestBidPrice != "90580.4" {
		t.Fatalf("the valid update after two malformed ones must arrive: %+v", v)
	}
	eventually(t, func() bool { return s.Stats().DecodeErrors == 2 }, "two decode errors counted")
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-s.Events():
			var de *stream.DecodeError
			if ev.Type == stream.EventDecodeError && errors.As(ev.Err, &de) && strings.HasPrefix(de.Channel, "ticker.futures|") && len(de.Raw) > 0 {
				return
			}
		case <-deadline:
			t.Fatal("no decode error event")
		}
	}
}

func TestMalformedChannelSpecificPayloads(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("kline|XBTUSDTM|1min",
		`{"T":"kline.FUTURES","P":1,"d":{"s":"XBTUSDTM","i":"1min","S":"not a bool"}}`, fxKlineFutures)
	h.fake.OnSubscribe("funding-fee-all-symbols",
		`{"T":"funding-fee-all-symbols","P":1,"d":{"s":"XBTUSDTM"}}`, // an object where an array is documented
		`{"T":"funding-fee-all-symbols","P":2,"d":null}`,
		fxFundingAll)
	h.fake.OnSubscribe("obu|XBTUSDTM|5",
		`{"T":"obu.FUTURES","dp":"5","t":"snapshot","P":1,"d":{"s":"XBTUSDTM","b":[["1"]]}}`, // a level without a size
		fxObuFuturesRPI)
	h.fake.OnSubscribe("mark-price|XBTUSDTM", `{"T":"mark-price","P":1,"d":{"s":"XBTUSDTM","ts":{"x":1}}}`, fxMarkPrice)
	h.fake.OnSubscribe("callAuctionInfo|GROVE-USDT", `{"T":"callAuctionInfo.SPOT","t":"snapshot","P":1,"d":{"s":"GROVE-USDT","ts":[]}}`, fxCallAuction)
	fut := h.futures(t)
	ctx := ctx5(t)
	kl, err := fut.SubscribeKlines(ctx, Interval1Min, []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	all, err := fut.SubscribeAllFundingRates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ob, err := fut.SubscribeOrderBookUpdates(ctx, []string{"XBTUSDTM"}, Depth5)
	if err != nil {
		t.Fatal(err)
	}
	mp, err := fut.SubscribeMarkPrice(ctx, []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	if v := next(t, kl); v.Open != "92505.1" {
		t.Fatalf("kline: %+v", v)
	}
	if v := next(t, all); len(v.Rates) != 2 {
		t.Fatalf("all funding rates: %+v", v)
	}
	if v := next(t, ob); v.Bids[0].Price != "88862.4" {
		t.Fatalf("order book: %+v", v)
	}
	if v := next(t, mp); v.MarkPrice != "90445" {
		t.Fatalf("mark price: %+v", v)
	}
	spot := h.spot(t)
	ca, err := spot.SubscribeCallAuction(ctx, []string{"GROVE-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	if v := next(t, ca); v.EstimatedPrice != "0.05568" {
		t.Fatalf("call auction: %+v", v)
	}
	eventually(t, func() bool { return fut.Stats().DecodeErrors == 1+2+1+1 }, "every malformed payload on the futures session counted exactly once")
	eventually(t, func() bool { return spot.Stats().DecodeErrors == 1 }, "the malformed call-auction payload counted")
}

func TestSlowConsumerCountsDroppedUpdates(t *testing.T) {
	h := newHarness(t)
	frames := make([]string, 60)
	for i := range frames {
		frames[i] = strings.Replace(fxTickerFutures, `"E":1905974001288`, fmt.Sprintf(`"E":%d`, 1000+i), 1)
	}
	h.fake.OnSubscribe("ticker|XBTUSDTM", frames...)
	s := h.futures(t)
	sub, err := s.SubscribeTicker(ctx5(t), []string{"XBTUSDTM"}, stream.WithBuffer(3), stream.WithOverflow(stream.DropOldest))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return s.Stats().FramesReceived >= 62 }, "all frames read from the socket without a consumer")
	var got []int64
	for {
		select {
		case v := <-sub.C():
			got = append(got, int64(v.Sequence))
			continue
		case <-time.After(300 * time.Millisecond):
		}
		break
	}
	if len(got) == 0 || got[len(got)-1] != 1059 {
		t.Fatalf("the newest update must survive, got %v", got)
	}
	if sub.Dropped() == 0 || sub.Dropped()+uint64(len(got)) != 60 {
		t.Fatalf("dropped=%d delivered=%d, want 60 in total", sub.Dropped(), len(got))
	}
}

func TestFailSubscriptionPolicyEndsOnlyTheSlowSubscription(t *testing.T) {
	h := newHarness(t)
	frames := make([]string, 30)
	for i := range frames {
		frames[i] = fxTradeFutures
	}
	h.fake.OnSubscribe("trade|XBTUSDTM", frames...)
	h.fake.OnSubscribe("ticker|XBTUSDTM", fxTickerFutures)
	s := h.futures(t)
	slow, err := s.SubscribeTrades(ctx5(t), []string{"XBTUSDTM"}, stream.WithBuffer(2), stream.WithOverflow(stream.FailSubscription))
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := s.SubscribeTicker(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-slow.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the slow subscription was not failed")
	}
	if !errors.Is(slow.Err(), stream.ErrSlowConsumer) {
		t.Fatalf("Err = %v", slow.Err())
	}
	if v := next(t, healthy); v.Symbol != "XBTUSDTM" {
		t.Fatalf("the healthy subscription must keep working: %+v", v)
	}
	if s.State() != stream.StateConnected {
		t.Fatal("the connection must stay up")
	}
}

func TestHostsDefaultToTheDocumentedEndpoints(t *testing.T) {
	got := Hosts{}.withDefaults()
	if got.Spot != uta.PublicSpotWSURL || got.Futures != uta.PublicFuturesWSURL || got.Private != uta.PrivateWSURL {
		t.Fatalf("defaults: %+v", got)
	}
	custom := Hosts{Spot: "ws://spot", Futures: "ws://fut", Private: "ws://priv"}
	if custom.withDefaults() != custom {
		t.Fatal("explicit hosts must be kept")
	}
	if NewService(Hosts{}, nil, nil).hosts != got {
		t.Fatal("NewService applies the defaults")
	}
}

func TestDialFailsWhenTheHostIsUnreachable(t *testing.T) {
	wstest.CheckLeaks(t)
	svc := NewService(Hosts{Futures: "ws://127.0.0.1:1"}, nil, nil, append(fastOptions(), stream.WithConnectTimeout(time.Second))...)
	if s, err := svc.DialFutures(ctx5(t)); err == nil {
		_ = s.Close()
		t.Fatal("dialling an unreachable host must fail")
	}
	svc = NewService(Hosts{Spot: "://bad"}, nil, nil, fastOptions()...)
	if _, err := svc.DialSpot(ctx5(t)); !stream.IsPermanent(err) {
		t.Fatalf("an invalid host is a permanent error: %v", err)
	}
}

func TestTradeTypeOfPushType(t *testing.T) {
	for in, want := range map[string]TradeType{
		"ticker.FUTURES": TradeTypeFutures, "obu.spot": TradeTypeSpot, "execution.lite.UNIFIED": TradeTypeUnified,
		"mark-price": "", "": "",
	} {
		if got := tradeTypeOf(in); got != want {
			t.Errorf("tradeTypeOf(%q) = %q, want %q", in, got, want)
		}
	}
	if accountTypeOf("balance.FUNDING") != AccountTypeFunding {
		t.Error("accountTypeOf")
	}
}
