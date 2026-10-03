package streaming

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	spotstreaming "github.com/tigusigalpa/kucoin-go/classic/spot/streaming"
	classicws "github.com/tigusigalpa/kucoin-go/classic/ws"
	"github.com/tigusigalpa/kucoin-go/internal/wsengine"
	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
	"github.com/tigusigalpa/kucoin-go/websocket/classic"
)

func init() { wsengine.SetMinPingInterval(time.Millisecond) }

func fastOptions() []stream.Option {
	return []stream.Option{
		stream.WithReconnect(stream.ReconnectPolicy{MinDelay: 5 * time.Millisecond, MaxDelay: 20 * time.Millisecond, Jitter: 0.1, StableAfter: time.Hour}),
		stream.WithPingInterval(time.Hour),
	}
}

// harness wires a Service to a fake KuCoin.
type harness struct {
	fake      *wstest.ClassicFake
	svc       *Service
	publicN   atomic.Int32
	privateN  atomic.Int32
	snapshots func(ctx context.Context, symbol string) (orderbook.Snapshot, error)
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	wstest.CheckLeaks(t)
	h := &harness{fake: wstest.NewClassicFake(t)}
	pub := func(context.Context) (*classicws.Token, error) { h.publicN.Add(1); return h.fake.Token(), nil }
	priv := func(context.Context) (*classicws.Token, error) { h.privateN.Add(1); return h.fake.Token(), nil }
	snap := func(ctx context.Context, symbol string) (orderbook.Snapshot, error) {
		if h.snapshots == nil {
			return orderbook.Snapshot{}, errors.New("no snapshot scripted")
		}
		return h.snapshots(ctx, symbol)
	}
	h.svc = NewService(pub, priv, snap, fastOptions()...)
	return h
}

func (h *harness) public(t *testing.T) *Session {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := h.svc.DialPublic(ctx)
	if err != nil {
		t.Fatalf("DialPublic: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func (h *harness) private(t *testing.T) *Session {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := h.svc.DialPrivate(ctx)
	if err != nil {
		t.Fatalf("DialPrivate: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
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

// eq reports every mismatching field of a payload, not just the first.
func eq[T comparable](t *testing.T, what string, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

func TestFixturesAreValidJSON(t *testing.T) {
	for name, frame := range allFixtures {
		if !json.Valid([]byte(frame)) {
			t.Errorf("%s is not valid JSON", name)
		}
	}
}

func TestIndexPrice(t *testing.T) {
	h := newHarness(t)
	topic := "/indicator/index:USDT-BTC,ETH-BTC"
	h.fake.OnSubscribe(topic, mgIndexLive, mgIndex)
	sub, err := h.public(t).SubscribeIndexPrice(ctx5(t), []string{"USDT-BTC", "ETH-BTC"})
	if err != nil {
		t.Fatal(err)
	}
	if h.fake.Last("subscribe")["privateChannel"] != false {
		t.Fatalf("a public topic must not be subscribed as private: %v", h.fake.Last("subscribe"))
	}
	live := next(t, sub)
	eq(t, "live Symbol", live.Symbol, "ETH-BTC")
	eq(t, "live Value keeps its 15 decimals", live.Value, "0.031560000000000")
	eq(t, "live Granularity", live.Granularity, 1000)
	eq(t, "live Timestamp", live.Timestamp, 1790984308000)
	doc := next(t, sub)
	eq(t, "Symbol", doc.Symbol, "USDT-BTC")
	eq(t, "Value", doc.Value, "0.0001092")
	eq(t, "Granularity", doc.Granularity, 5000)
	eq(t, "Timestamp", doc.Timestamp, 1551770400000)
	eq(t, "Time()", doc.Time().UnixMilli(), 1551770400000)
	eq(t, "Key", sub.Key(), topic)
}

func TestMarkPrice(t *testing.T) {
	h := newHarness(t)
	topic := "/indicator/markPrice:USDT-BTC,ETH-BTC"
	h.fake.OnSubscribe(topic, mgMark, mgMarkLive)
	sub, err := h.public(t).SubscribeMarkPrice(ctx5(t), []string{"USDT-BTC", "ETH-BTC"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, sub)
	eq(t, "Symbol", v.Symbol, "USDT-BTC")
	eq(t, "Value keeps its 15 decimals", v.Value, "0.000011820000000")
	eq(t, "Granularity", v.Granularity, 1000)
	eq(t, "Timestamp", v.Timestamp, 1740840036000)
	eq(t, "Time()", v.Time().UnixMilli(), 1740840036000)
	live := next(t, sub)
	eq(t, "live Symbol", live.Symbol, "ETH-BTC")
	eq(t, "live Value", live.Value, "0.031560000000000")
	eq(t, "Key", sub.Key(), topic)
}

func TestCrossMarginPosition_DebtRatioAndPositionStatus(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/margin/position", mgCrossDebtRatio, mgCrossStatus, mgCrossStatusAsPrinted, mgCrossDebtRatioVariant)
	sub, err := h.private(t).SubscribeCrossMarginPosition(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if h.fake.Last("subscribe")["privateChannel"] != true {
		t.Fatal("private topics must be subscribed with privateChannel=true")
	}

	ratio := next(t, sub)
	eq(t, "Subject", ratio.Subject, SubjectDebtRatio)
	eq(t, "UserID", ratio.UserID, "633559791e1cbc0001f319bc")
	eq(t, "ChannelType", ratio.ChannelType, "private")
	eq(t, "IsDebtRatio", ratio.IsDebtRatio(), true)
	eq(t, "IsPositionStatus", ratio.IsPositionStatus(), false)
	eq(t, "DebtRatio", ratio.DebtRatio, "0")
	eq(t, "TotalAsset keeps its 23 decimals", ratio.TotalAsset, "0.00052431772284080000000")
	eq(t, "MarginCoefficientTotalAsset", ratio.MarginCoefficientTotalAsset, "0.0005243177228408")
	eq(t, "TotalDebt", ratio.TotalDebt, "0")
	eq(t, "len(AssetList)", len(ratio.AssetList), 2)
	eq(t, "BTC asset", ratio.AssetList["BTC"], CrossMarginAsset{Total: "0.00002", Available: "0", Hold: "0.00002"})
	eq(t, "USDT asset", ratio.AssetList["USDT"], CrossMarginAsset{Total: "33.68855864", Available: "15.01916691", Hold: "18.66939173"})
	eq(t, "len(DebtList)", len(ratio.DebtList), 2)
	eq(t, "BTC debt", ratio.DebtList["BTC"], "0")
	eq(t, "USDT debt", ratio.DebtList["USDT"], "0")
	eq(t, "Type of a debt-ratio update", ratio.Type, "")
	eq(t, "Timestamp", ratio.Timestamp, 1729912435657)
	eq(t, "Time()", ratio.Time().UnixMilli(), 1729912435657)

	status := next(t, sub)
	eq(t, "status Subject", status.Subject, SubjectPositionStatus)
	eq(t, "status IsPositionStatus", status.IsPositionStatus(), true)
	eq(t, "status IsDebtRatio", status.IsDebtRatio(), false)
	eq(t, "status Type", status.Type, PositionStatusFrozenFL)
	eq(t, "status Timestamp as printed", status.Timestamp, 15538460812100)
	eq(t, "status DebtRatio", status.DebtRatio, "")
	eq(t, "status AssetList", len(status.AssetList), 0)

	// The documentation prints the status payload under the subject debt.ratio; the
	// content decides.
	printed := next(t, sub)
	eq(t, "printed Subject", printed.Subject, SubjectDebtRatio)
	eq(t, "printed Type", printed.Type, PositionStatusFrozenFL)
	eq(t, "printed IsPositionStatus", printed.IsPositionStatus(), true)
	eq(t, "printed IsDebtRatio", printed.IsDebtRatio(), false)

	variant := next(t, sub)
	eq(t, "variant DebtRatio (a string)", variant.DebtRatio, "0.42")
	eq(t, "variant TotalAsset", variant.TotalAsset, "1.5")
	eq(t, "variant MarginCoefficientTotalAsset (a number)", variant.MarginCoefficientTotalAsset, "1.25")
	eq(t, "variant TotalDebt", variant.TotalDebt, "0.63")
	eq(t, "variant ETH asset", variant.AssetList["ETH"], CrossMarginAsset{Total: "10", Available: "4", Hold: "6"})
	eq(t, "variant ETH debt", variant.DebtList["ETH"], "3.5")
	eq(t, "variant USDT debt", variant.DebtList["USDT"], "120.25")
	eq(t, "variant IsDebtRatio", variant.IsDebtRatio(), true)
}

func TestCrossMarginPositionStatusTypes(t *testing.T) {
	h := newHarness(t)
	var frames []string
	kinds := []string{PositionStatusFrozenFL, PositionStatusUnfrozenFL, PositionStatusFrozenRenew, PositionStatusUnfrozenRenew, PositionStatusLiability, PositionStatusUnliability}
	for _, k := range kinds {
		frames = append(frames, strings.ReplaceAll(mgCrossStatus, "FROZEN_FL", k))
	}
	h.fake.OnSubscribe("/margin/position", frames...)
	sub, err := h.private(t).SubscribeCrossMarginPosition(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range kinds {
		if ev := next(t, sub); ev.Type != k || !ev.IsPositionStatus() {
			t.Fatalf("event %+v, want status %s", ev, k)
		}
	}
	for want, got := range map[string]string{"FROZEN_FL": PositionStatusFrozenFL, "UNFROZEN_FL": PositionStatusUnfrozenFL, "FROZEN_RENEW": PositionStatusFrozenRenew,
		"UNFROZEN_RENEW": PositionStatusUnfrozenRenew, "LIABILITY": PositionStatusLiability, "UNLIABILITY": PositionStatusUnliability} {
		eq(t, "constant "+want, got, want)
	}
}

func TestIsolatedMarginPosition(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/margin/isolatedPosition:BTC-USDT", mgIsolated)
	h.fake.OnSubscribe("/margin/isolatedPosition:ETH-USDT", mgIsolatedVariant)
	s := h.private(t)
	btc, err := s.SubscribeIsolatedMarginPosition(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	if h.fake.Last("subscribe")["privateChannel"] != true {
		t.Fatal("private topics must be subscribed with privateChannel=true")
	}
	eth, err := s.SubscribeIsolatedMarginPosition(ctx5(t), "ETH-USDT")
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, btc)
	eq(t, "Subject", v.Subject, SubjectIsolatedPositionChange)
	eq(t, "UserID", v.UserID, "633559791e1cbc0001f319bc")
	eq(t, "ChannelType", v.ChannelType, "private")
	eq(t, "Tag", v.Tag, "BTC-USDT")
	eq(t, "Status", v.Status, IsolatedStatusDebt)
	eq(t, "StatusBizType", v.StatusBizType, StatusBizDefaultDebt)
	eq(t, "AccumulatedPrincipal", v.AccumulatedPrincipal, "5.01")
	eq(t, "len(ChangeAssets)", len(v.ChangeAssets), 2)
	eq(t, "BTC asset", v.ChangeAssets["BTC"], IsolatedMarginAsset{Total: "0.00043478", Hold: "0", LiabilityPrincipal: "0", LiabilityInterest: "0"})
	eq(t, "USDT asset", v.ChangeAssets["USDT"], IsolatedMarginAsset{Total: "0.98092004", Hold: "0", LiabilityPrincipal: "26", LiabilityInterest: "0.00025644"})
	eq(t, "Timestamp", v.Timestamp, 1730121097742)
	eq(t, "Time()", v.Time().UnixMilli(), 1730121097742)

	w := next(t, eth)
	eq(t, "variant Tag", w.Tag, "ETH-USDT")
	eq(t, "variant Status", w.Status, IsolatedStatusInLiquidation)
	eq(t, "variant StatusBizType", w.StatusBizType, StatusBizForceLiquidation)
	eq(t, "variant AccumulatedPrincipal (a number)", w.AccumulatedPrincipal, "12.5")
	eq(t, "variant ETH asset", w.ChangeAssets["ETH"], IsolatedMarginAsset{Total: "1", Hold: "0.5", LiabilityPrincipal: "0.25", LiabilityInterest: "0.001"})
}

func TestIsolatedMarginPositionConstants(t *testing.T) {
	for want, got := range map[string]string{
		"DEBT": IsolatedStatusDebt, "CLEAR": IsolatedStatusClear, "IN_BORROW": IsolatedStatusInBorrow, "IN_REPAY": IsolatedStatusInRepay,
		"IN_LIQUIDATION": IsolatedStatusInLiquidation, "IN_AUTO_RENEW": IsolatedStatusInAutoRenew,
		"FORCE_LIQUIDATION": StatusBizForceLiquidation, "USER_BORROW": StatusBizUserBorrow, "TRADE_AUTO_BORROW": StatusBizTradeAutoBorrow,
		"USER_REPAY": StatusBizUserRepay, "AUTO_REPAY": StatusBizAutoRepay, "DEFAULT_DEBT": StatusBizDefaultDebt, "DEFAULT_CLEAR": StatusBizDefaultClear,
		"ONE_CLICK_LIQUIDATION": StatusBizOneClickLiquidation, "B2C_INTEREST_SETTLE_LIQUIDATION": StatusBizB2CInterestSettleLiquidation,
		"AIR_DROP_LIQUIDATION": StatusBizAirDropLiquidation,
	} {
		eq(t, "constant "+want, got, want)
	}
}

// The Margin documentation reuses the Spot specifications for orders, balance and
// stop orders: a Margin session has the same four methods, backed by the same code.

func TestMarginOrdersV2(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/spotMarket/tradeOrdersV2", mgOrderV2Open)
	sub, err := h.private(t).SubscribeOrdersV2(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if h.fake.Last("subscribe")["privateChannel"] != true {
		t.Fatal("private topics must be subscribed with privateChannel=true")
	}
	v := next(t, sub)
	eq(t, "Type", v.Type, spotstreaming.OrderEventOpen)
	eq(t, "Status", v.Status, spotstreaming.OrderStatusOpen)
	eq(t, "OrderID", v.OrderID, "6720ecd9ec71f4000747731a")
	eq(t, "Price", v.Price, "50000")
	eq(t, "UserID", v.UserID, "633559791e1cbc0001f319bc")
	eq(t, "Timestamp", v.Timestamp, 1730211033335000000)
}

func TestMarginOrdersV1(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/spotMarket/tradeOrders", mgOrderV1Match)
	sub, err := h.private(t).SubscribeOrdersV1(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, sub)
	eq(t, "Type", v.Type, spotstreaming.OrderEventMatch)
	eq(t, "Status", v.Status, spotstreaming.OrderStatusMatch)
	eq(t, "Liquidity", v.Liquidity, spotstreaming.LiquidityTaker)
	eq(t, "MatchPrice", v.MatchPrice, "71171.9")
	eq(t, "TradeID", v.TradeID, "11116472408358913")
}

func TestMarginBalance(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/account/balance", mgBalanceMargin, mgBalanceIsolated)
	sub, err := h.private(t).SubscribeBalance(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	cross := next(t, sub)
	eq(t, "RelationEvent", cross.RelationEvent, "margin.hold")
	eq(t, "ID", cross.ID, "354689988084002")
	eq(t, "Total", cross.Total, "21.133773386762")
	eq(t, "Timestamp", cross.Timestamp, 1730269283892)
	iso := next(t, sub)
	eq(t, "isolated RelationEvent", iso.RelationEvent, "isolatedV2_BTC-USDT.transfer")
	eq(t, "isolated Currency", iso.Currency, "BTC")
	eq(t, "isolated RelationContext.Symbol", iso.RelationContext.Symbol, "BTC-USDT")
	eq(t, "isolated Timestamp", iso.Timestamp, 1730269283893)
}

func TestMarginStopOrders(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/spotMarket/advancedOrders", mgStopOrderMargin, mgStopOrderIsolated)
	sub, err := h.private(t).SubscribeStopOrders(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	cross := next(t, sub)
	eq(t, "TradeType", cross.TradeType, spotstreaming.TradeTypeMargin)
	eq(t, "Type", cross.Type, spotstreaming.StopEventTriggered)
	eq(t, "IsTriggered", cross.IsTriggered(), true)
	eq(t, "Symbol", cross.Symbol, "ETH-USDT")
	eq(t, "Size", cross.Size, "1.5")
	iso := next(t, sub)
	eq(t, "isolated TradeType", iso.TradeType, spotstreaming.TradeTypeIsolatedMargin)
	eq(t, "isolated Type", iso.Type, spotstreaming.StopEventOpen)
	eq(t, "isolated StopPrice", iso.StopPrice, "71000")
}

func TestMarginSessionServesSpotPublicChannelsToo(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/market/ticker:BTC-USDT", mgSpotTicker)
	h.snapshots = func(_ context.Context, symbol string) (orderbook.Snapshot, error) {
		return orderbook.Snapshot{Symbol: symbol, Sequence: 16, Bids: []orderbook.Level{{Price: "3988.51", Size: "56"}}, Asks: []orderbook.Level{{Price: "3988.59", Size: "3"}}}, nil
	}
	s := h.public(t)
	ticker, err := s.SubscribeTicker(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, ticker)
	eq(t, "ticker Symbol", v.Symbol, "BTC-USDT")
	eq(t, "ticker BestBid", v.BestBid, "0.049")
	eq(t, "ticker Timestamp", v.Timestamp, 1704873323416)
	// The managed order book comes with the embedded Session, snapshot function and all.
	book, err := s.SubscribeOrderBook(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	<-book.Ready()
	if bid, _ := book.Book().BestBid(); bid.Price != "3988.51" || book.Book().Sequence() != 16 {
		t.Fatalf("best bid = %v at sequence %d", bid, book.Book().Sequence())
	}
	// Margin and Spot subscriptions share one connection and one set of counters.
	if st := s.Stats(); st.Subscriptions != 2 || s.State() != stream.StateConnected || h.fake.Server.Connections() != 1 {
		t.Fatalf("stats %+v connections=%d", s.Stats(), h.fake.Server.Connections())
	}
	// And the Margin channels work next to them.
	marks, err := s.SubscribeMarkPrice(ctx5(t), []string{"USDT-BTC"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.fake.Push(mgMark); err != nil {
		t.Fatal(err)
	}
	eq(t, "mark price Value", next(t, marks).Value, "0.000011820000000")
	var _ *classic.Client = s.Client() // the embedded Session's accessor is promoted
}

// Every Spot subscribe method is a method of the Margin session as well, and
// subscribes the Spot topic, privately where the channel is private.
func TestMarginSessionExposesEverySpotChannel(t *testing.T) {
	h := newHarness(t)
	h.snapshots = func(_ context.Context, symbol string) (orderbook.Snapshot, error) {
		return orderbook.Snapshot{Symbol: symbol, Sequence: 16, Bids: []orderbook.Level{{Price: "1", Size: "1"}}}, nil
	}
	s := h.private(t)
	ctx := ctx5(t)
	btc := []string{"BTC-USDT"}
	cases := []struct {
		method, topic string
		private       bool
		subscribe     func() error
	}{
		{"SubscribeTicker", "/market/ticker:BTC-USDT", false, func() error { _, err := s.SubscribeTicker(ctx, btc); return err }},
		{"SubscribeAllTickers", "/market/ticker:all", false, func() error { _, err := s.SubscribeAllTickers(ctx); return err }},
		{"SubscribeSymbolSnapshot", "/market/snapshot:BTC-USDT", false, func() error { _, err := s.SubscribeSymbolSnapshot(ctx, btc); return err }},
		{"SubscribeMarketSnapshot", "/market/snapshot:BTC", false, func() error { _, err := s.SubscribeMarketSnapshot(ctx, "BTC"); return err }},
		{"SubscribeLevel1", "/spotMarket/level1:BTC-USDT", false, func() error { _, err := s.SubscribeLevel1(ctx, btc); return err }},
		{"SubscribeDepth5", "/spotMarket/level2Depth5:BTC-USDT", false, func() error { _, err := s.SubscribeDepth5(ctx, btc); return err }},
		{"SubscribeDepth50", "/spotMarket/level2Depth50:BTC-USDT", false, func() error { _, err := s.SubscribeDepth50(ctx, btc); return err }},
		{"SubscribeOrderBookChanges", "/market/level2:BTC-USDT", false, func() error { _, err := s.SubscribeOrderBookChanges(ctx, btc); return err }},
		{"SubscribeOrderBook", "/market/level2:ETH-USDT", false, func() error { _, err := s.SubscribeOrderBook(ctx, "ETH-USDT"); return err }},
		{"SubscribeOrderBookTuned", "/market/level2:SOL-USDT", false, func() error {
			_, err := s.SubscribeOrderBookTuned(ctx, "SOL-USDT", orderbook.Tuning{MaxAttempts: 2})
			return err
		}},
		{"SubscribeKlines", "/market/candles:BTC-USDT_1min", false, func() error {
			_, err := s.SubscribeKlines(ctx, spotstreaming.Interval1Min, btc)
			return err
		}},
		{"SubscribeTrades", "/market/match:BTC-USDT", false, func() error { _, err := s.SubscribeTrades(ctx, btc); return err }},
		{"SubscribeCallAuctionDepth50", "/callauction/level2Depth50:BTC-USDT", false, func() error { _, err := s.SubscribeCallAuctionDepth50(ctx, btc); return err }},
		{"SubscribeCallAuctionData", "/callauction/callauctionData:BTC-USDT", false, func() error { _, err := s.SubscribeCallAuctionData(ctx, btc); return err }},
		{"SubscribeOrdersV2", "/spotMarket/tradeOrdersV2", true, func() error { _, err := s.SubscribeOrdersV2(ctx); return err }},
		{"SubscribeOrdersV1", "/spotMarket/tradeOrders", true, func() error { _, err := s.SubscribeOrdersV1(ctx); return err }},
		{"SubscribeBalance", "/account/balance", true, func() error { _, err := s.SubscribeBalance(ctx); return err }},
		{"SubscribeStopOrders", "/spotMarket/advancedOrders", true, func() error { _, err := s.SubscribeStopOrders(ctx); return err }},
	}
	for _, tc := range cases {
		if err := tc.subscribe(); err != nil {
			t.Errorf("%s: %v", tc.method, err)
			continue
		}
		last := h.fake.Last("subscribe")
		if last["topic"] != tc.topic || last["privateChannel"] != tc.private {
			t.Errorf("%s subscribed %v, want topic %s with privateChannel=%v", tc.method, last, tc.topic, tc.private)
		}
	}
	if n := h.fake.Count("subscribe", ""); n != len(cases) {
		t.Fatalf("%d subscribe frames for %d channels", n, len(cases))
	}
}

func TestLocalValidationHappensBeforeAnythingIsSent(t *testing.T) {
	h := newHarness(t)
	pub := h.public(t)
	ctx := ctx5(t)
	tooMany := make([]string, MaxSymbolsPerSubscription+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("SYM%d-BTC", i)
	}
	cases := []struct {
		name string
		call func() error
		want error
	}{
		{"index price without symbols", func() error { _, err := pub.SubscribeIndexPrice(ctx, nil); return err }, ErrNoSymbols},
		{"mark price without symbols", func() error { _, err := pub.SubscribeMarkPrice(ctx, []string{}); return err }, ErrNoSymbols},
		{"too many index symbols", func() error { _, err := pub.SubscribeIndexPrice(ctx, tooMany); return err }, ErrTooManySymbols},
		{"too many mark symbols", func() error { _, err := pub.SubscribeMarkPrice(ctx, tooMany); return err }, ErrTooManySymbols},
		{"empty symbol", func() error { _, err := pub.SubscribeIndexPrice(ctx, []string{""}); return err }, ErrInvalidSymbol},
		{"comma in symbol", func() error { _, err := pub.SubscribeIndexPrice(ctx, []string{"A,B"}); return err }, ErrInvalidSymbol},
		{"colon in symbol", func() error { _, err := pub.SubscribeMarkPrice(ctx, []string{"A:B"}); return err }, ErrInvalidSymbol},
		{"underscore in symbol", func() error { _, err := pub.SubscribeMarkPrice(ctx, []string{"A_B"}); return err }, ErrInvalidSymbol},
		{"slash in symbol", func() error { _, err := pub.SubscribeIndexPrice(ctx, []string{"USDT/BTC"}); return err }, ErrInvalidSymbol},
		{"space in symbol", func() error { _, err := pub.SubscribeIndexPrice(ctx, []string{"USDT BTC"}); return err }, ErrInvalidSymbol},
		{"one bad symbol among good ones", func() error { _, err := pub.SubscribeMarkPrice(ctx, []string{"USDT-BTC", ""}); return err }, ErrInvalidSymbol},
		{"cross margin position on a public session", func() error { _, err := pub.SubscribeCrossMarginPosition(ctx); return err }, ErrPrivateConnectionRequired},
		{"isolated margin position on a public session", func() error { _, err := pub.SubscribeIsolatedMarginPosition(ctx, "BTC-USDT"); return err }, ErrPrivateConnectionRequired},
		{"isolated margin position without a symbol", func() error { _, err := pub.SubscribeIsolatedMarginPosition(ctx, ""); return err }, ErrInvalidSymbol},
		{"isolated margin position with a bad symbol", func() error { _, err := pub.SubscribeIsolatedMarginPosition(ctx, "BTC,USDT"); return err }, ErrInvalidSymbol},
		// The embedded Spot channels validate the same way.
		{"spot private channel on a public session", func() error { _, err := pub.SubscribeOrdersV2(ctx); return err }, ErrPrivateConnectionRequired},
		{"spot ticker without symbols", func() error { _, err := pub.SubscribeTicker(ctx, nil); return err }, spotstreaming.ErrNoSymbols},
	}
	for _, tc := range cases {
		if err := tc.call(); !errors.Is(err, tc.want) {
			t.Errorf("%s: error = %v, want %v", tc.name, err, tc.want)
		}
	}
	if n := h.fake.Count("subscribe", ""); n != 0 {
		t.Fatalf("%d subscribe frames reached the server; validation must happen locally", n)
	}
	// The errors are the very values of the Spot package.
	if ErrNoSymbols != spotstreaming.ErrNoSymbols || ErrTooManySymbols != spotstreaming.ErrTooManySymbols ||
		ErrInvalidSymbol != spotstreaming.ErrInvalidSymbol || ErrPrivateConnectionRequired != spotstreaming.ErrPrivateConnectionRequired ||
		MaxSymbolsPerSubscription != spotstreaming.MaxSymbolsPerSubscription {
		t.Fatal("the Margin package must re-export the validation errors of the Spot package")
	}
}

func TestSubscribingASymbolTwiceIsRejected(t *testing.T) {
	h := newHarness(t)
	s := h.public(t)
	if _, err := s.SubscribeIndexPrice(ctx5(t), []string{"USDT-BTC"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubscribeIndexPrice(ctx5(t), []string{"ETH-BTC", "USDT-BTC"}); !errors.Is(err, stream.ErrAlreadySubscribed) {
		t.Fatalf("error = %v, want %v", err, stream.ErrAlreadySubscribed)
	}
	// The same symbol on the mark-price channel is a different topic.
	if _, err := s.SubscribeMarkPrice(ctx5(t), []string{"USDT-BTC"}); err != nil {
		t.Fatalf("mark price: %v", err)
	}
}

// The connection hands a push to every occurrence of its topic in a subscription,
// so a symbol that is listed twice must be subscribed once or each of its updates
// would arrive twice.
func TestASymbolListedTwiceIsSubscribedOnce(t *testing.T) {
	h := newHarness(t)
	topic := "/indicator/index:USDT-BTC,ETH-BTC"
	h.fake.OnSubscribe(topic, mgIndex)
	s := h.public(t)
	sub, err := s.SubscribeIndexPrice(ctx5(t), []string{"USDT-BTC", "ETH-BTC", "USDT-BTC"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "Key names every symbol once", sub.Key(), topic)
	eq(t, "Symbol", next(t, sub).Symbol, "USDT-BTC")
	select {
	case v := <-sub.C():
		t.Fatalf("one push arrived twice: %+v", v)
	case <-time.After(100 * time.Millisecond):
	}
	// A repeat does not count against the limit of a hundred symbols.
	hundred := make([]string, 0, MaxSymbolsPerSubscription+1)
	for i := 0; i < MaxSymbolsPerSubscription; i++ {
		hundred = append(hundred, fmt.Sprintf("SYM%d-BTC", i))
	}
	hundred = append(hundred, "SYM0-BTC")
	if _, err := s.SubscribeMarkPrice(ctx5(t), hundred); err != nil {
		t.Fatalf("a hundred distinct symbols and a repeat: %v", err)
	}
}

func TestServerRejectionIsTyped(t *testing.T) {
	h := newHarness(t)
	h.fake.Reject("/indicator/index:NOSUCH-BTC", 404)
	h.fake.Reject("/margin/position", 403)
	var se *stream.ServerError
	_, err := h.public(t).SubscribeIndexPrice(ctx5(t), []string{"NOSUCH-BTC"})
	if !errors.Is(err, stream.ErrTopicNotFound) || !errors.As(err, &se) || se.Code != 404 {
		t.Fatalf("error = %v", err)
	}
	if _, err := h.private(t).SubscribeCrossMarginPosition(ctx5(t)); !errors.Is(err, stream.ErrLoginRequired) {
		t.Fatalf("error = %v", err)
	}
}

func TestDialPrivateWithoutCredentialsFailsBeforeAnyNetworkAccess(t *testing.T) {
	wstest.CheckLeaks(t)
	fake := wstest.NewClassicFake(t)
	svc := NewService(
		func(context.Context) (*classicws.Token, error) { return fake.Token(), nil },
		func(context.Context) (*classicws.Token, error) { return nil, transport.ErrCredentialsRequired },
		nil, fastOptions()...)
	_, err := svc.DialPrivate(ctx5(t))
	if !errors.Is(err, transport.ErrCredentialsRequired) || !stream.IsPermanent(err) || !errors.Is(err, stream.ErrTokenUnavailable) {
		t.Fatalf("error = %v", err)
	}
	if fake.Server.Connections() != 0 {
		t.Fatal("no connection may be attempted without credentials")
	}
	// The public side works without the private token.
	s, err := svc.DialPublic(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.SubscribeCrossMarginPosition(ctx5(t)); !errors.Is(err, ErrPrivateConnectionRequired) {
		t.Fatalf("error = %v", err)
	}
	// A service without token sources refuses to dial at all.
	empty := NewService(nil, nil, nil)
	if _, err := empty.DialPublic(ctx5(t)); err == nil {
		t.Fatal("a service without a public token source must refuse to dial")
	}
	if _, err := empty.DialPrivate(ctx5(t)); err == nil {
		t.Fatal("a service without a private token source must refuse to dial")
	}
	// Without a snapshot source the embedded managed order book is refused.
	if _, err := s.SubscribeOrderBook(ctx5(t), "BTC-USDT"); !errors.Is(err, spotstreaming.ErrNoSnapshotSource) {
		t.Fatalf("error = %v", err)
	}
}

func TestSessionReconnectsWithAFreshPrivateTokenAndRestoresMarginSubscriptions(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/margin/position")
	h.fake.OnSubscribe("/margin/isolatedPosition:BTC-USDT")
	h.fake.OnSubscribe("/spotMarket/tradeOrdersV2")
	s := h.private(t)
	cross, err := s.SubscribeCrossMarginPosition(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	iso, err := s.SubscribeIsolatedMarginPosition(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	orders, err := s.SubscribeOrdersV2(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	h.fake.Server.DropAll()
	eventually(t, func() bool {
		return s.State() == stream.StateConnected && h.fake.Server.Connections() == 2 && h.fake.Count("subscribe", "") == 6
	}, "reconnect and resubscribe")
	if h.privateN.Load() != 2 || h.publicN.Load() != 0 {
		t.Fatalf("private token requested %d times, public %d; a reconnect must fetch a fresh private token", h.privateN.Load(), h.publicN.Load())
	}
	if h.fake.Last("subscribe")["privateChannel"] != true {
		t.Fatal("the restored subscriptions must stay private channels")
	}
	for _, frame := range []string{mgCrossDebtRatio, mgIsolated, mgOrderV2Open} {
		if err := h.fake.Push(frame); err != nil {
			t.Fatal(err)
		}
	}
	eq(t, "cross margin after the reconnect", next(t, cross).TotalDebt, "0")
	eq(t, "isolated margin after the reconnect", next(t, iso).Tag, "BTC-USDT")
	eq(t, "orders after the reconnect", next(t, orders).Type, spotstreaming.OrderEventOpen)
	if st := s.Stats(); st.Reconnects != 1 || st.Subscriptions != 3 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestSessionCloseEndsEveryMarginAndSpotSubscription(t *testing.T) {
	h := newHarness(t)
	s := h.private(t)
	subs := make([]func() (closed bool), 0, 4)
	idx, err := s.SubscribeIndexPrice(ctx5(t), []string{"USDT-BTC"})
	if err != nil {
		t.Fatal(err)
	}
	subs = append(subs, func() bool { _, ok := <-idx.C(); return !ok })
	pos, err := s.SubscribeCrossMarginPosition(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	subs = append(subs, func() bool { _, ok := <-pos.C(); return !ok })
	bal, err := s.SubscribeBalance(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	subs = append(subs, func() bool { _, ok := <-bal.C(); return !ok })
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	for i, closed := range subs {
		done := make(chan bool, 1)
		go func() { done <- closed() }()
		select {
		case ok := <-done:
			if !ok {
				t.Fatalf("subscription %d stayed open after Close", i)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("subscription %d channel not closed", i)
		}
	}
	if idx.Err() != nil || pos.Err() != nil || bal.Err() != nil {
		t.Fatalf("a requested Close is not an error: %v %v %v", idx.Err(), pos.Err(), bal.Err())
	}
	select {
	case <-s.Done():
	default:
		t.Fatal("Done must be closed")
	}
	if s.Err() != nil {
		t.Fatalf("Err = %v", s.Err())
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSubscriptionCloseUnsubscribesAMarginTopic(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/margin/isolatedPosition:BTC-USDT", mgIsolated)
	s := h.private(t)
	sub, err := s.SubscribeIsolatedMarginPosition(ctx5(t), "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	next(t, sub)
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if h.fake.Count("unsubscribe", "/margin/isolatedPosition:BTC-USDT") != 1 {
		t.Fatal("the server never saw the unsubscribe")
	}
	if _, ok := <-sub.C(); ok {
		t.Fatal("C must be closed")
	}
	if _, err := s.SubscribeIsolatedMarginPosition(ctx5(t), "BTC-USDT"); err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
}

func TestMalformedMarginPushesAreReportedAndTheStreamContinues(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/margin/position",
		wstest.Message("/margin/position", "debt.ratio", `{"assetList":{"BTC":"not an object"}}`),
		wstest.Message("/margin/position", "debt.ratio", `{"debtList":{"BTC":{}}}`),
		wstest.Message("/margin/position", "position.status", `"just a string"`),
		mgCrossDebtRatio)
	h.fake.OnSubscribe("/margin/isolatedPosition:BTC-USDT",
		wstest.Message("/margin/isolatedPosition:BTC-USDT", "positionChange", `{"timestamp":"x"}`),
		mgIsolated)
	h.fake.OnSubscribe("/indicator/index:USDT-BTC",
		wstest.Message("/indicator/index:USDT-BTC", "tick", `{"symbol":"USDT-BTC","value":"1","granularity":"x"}`),
		wstest.Message("/indicator/index:USDT-BTC", "tick", `[1]`),
		mgIndex)
	h.fake.OnSubscribe("/indicator/markPrice:USDT-BTC",
		wstest.Message("/indicator/markPrice:USDT-BTC", "tick", `{"value":true}`),
		mgMark)
	s := h.private(t)
	ctx := ctx5(t)
	cross, err := s.SubscribeCrossMarginPosition(ctx)
	if err != nil {
		t.Fatal(err)
	}
	iso, err := s.SubscribeIsolatedMarginPosition(ctx, "BTC-USDT")
	if err != nil {
		t.Fatal(err)
	}
	idx, err := s.SubscribeIndexPrice(ctx, []string{"USDT-BTC"})
	if err != nil {
		t.Fatal(err)
	}
	mark, err := s.SubscribeMarkPrice(ctx, []string{"USDT-BTC"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "cross margin", next(t, cross).TotalAsset, "0.00052431772284080000000")
	eq(t, "isolated margin", next(t, iso).Tag, "BTC-USDT")
	eq(t, "index price", next(t, idx).Value, "0.0001092")
	eq(t, "mark price", next(t, mark).Value, "0.000011820000000")
	eventually(t, func() bool { return s.Stats().DecodeErrors == 3+1+2+1 }, "every malformed payload counted exactly once")
	var found bool
	deadline := time.After(2 * time.Second)
	for !found {
		select {
		case ev := <-s.Events():
			var de *stream.DecodeError
			if ev.Type == stream.EventDecodeError && errors.As(ev.Err, &de) && de.Channel == "/margin/position" && len(de.Raw) > 0 {
				found = true
			}
		case <-deadline:
			t.Fatal("no decode error event")
		}
	}
}

func TestSlowMarginConsumerCountsDroppedUpdates(t *testing.T) {
	h := newHarness(t)
	frames := make([]string, 40)
	for i := range frames {
		frames[i] = strings.Replace(mgMark, `"timestamp":1740840036000`, fmt.Sprintf(`"timestamp":%d`, 1740840036000+i), 1)
	}
	h.fake.OnSubscribe("/indicator/markPrice:USDT-BTC", frames...)
	s := h.public(t)
	sub, err := s.SubscribeMarkPrice(ctx5(t), []string{"USDT-BTC"}, stream.WithBuffer(3), stream.WithOverflow(stream.DropOldest))
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return s.Stats().FramesReceived >= 42 }, "all frames read from the socket without a consumer")
	var got []int64
	for {
		select {
		case v := <-sub.C():
			got = append(got, int64(v.Timestamp))
			continue
		case <-time.After(300 * time.Millisecond):
		}
		break
	}
	if len(got) == 0 || got[len(got)-1] != 1740840036039 {
		t.Fatalf("the newest update must survive, got %v", got)
	}
	if sub.Dropped() == 0 || sub.Dropped()+uint64(len(got)) != 40 {
		t.Fatalf("dropped=%d delivered=%d, want 40 in total", sub.Dropped(), len(got))
	}
}

func TestHelpers(t *testing.T) {
	for topic, want := range map[string]string{
		"/indicator/index:USDT-BTC":         "USDT-BTC",
		"/margin/isolatedPosition:ETH-USDT": "ETH-USDT",
		"/margin/position":                  "",
		"":                                  "",
	} {
		eq(t, "symbolFromTopic("+topic+")", symbolFromTopic(topic), want)
	}
	for _, ok := range []string{"USDT-BTC", "BTC", "1INCH-USDT"} {
		if !validName(ok) {
			t.Errorf("%q must be valid", ok)
		}
	}
	for _, bad := range []string{"", ",", "A,B", "A:B", "A_B", "A/B", "A B", "A\tB"} {
		if validName(bad) {
			t.Errorf("%q must not be valid", bad)
		}
	}
	// A payload without a symbol takes it from the topic; with one it keeps its own.
	idx, _, err := decodeIndexPrice(&classic.Message{Topic: "/indicator/index:USDT-BTC", Data: json.RawMessage(`{"value":1}`)})
	if err != nil || idx.Symbol != "USDT-BTC" {
		t.Fatalf("%+v err=%v", idx, err)
	}
	mark, _, err := decodeMarkPrice(&classic.Message{Topic: "/indicator/markPrice:USDT-BTC", Data: json.RawMessage(`{"symbol":"ETH-BTC","value":1}`)})
	if err != nil || mark.Symbol != "ETH-BTC" {
		t.Fatalf("%+v err=%v", mark, err)
	}
	mark, _, err = decodeMarkPrice(&classic.Message{Topic: "/indicator/markPrice:USDT-BTC", Data: json.RawMessage(`{"value":1}`)})
	if err != nil || mark.Symbol != "USDT-BTC" {
		t.Fatalf("%+v err=%v", mark, err)
	}
	for name, decode := range map[string]func(*classic.Message) error{
		"index price": func(m *classic.Message) error { _, _, err := decodeIndexPrice(m); return err },
		"mark price":  func(m *classic.Message) error { _, _, err := decodeMarkPrice(m); return err },
		"cross":       func(m *classic.Message) error { _, _, err := decodeCrossMarginPosition(m); return err },
		"isolated":    func(m *classic.Message) error { _, _, err := decodeIsolatedMarginPosition(m); return err },
	} {
		if err := decode(&classic.Message{Topic: "/x:y"}); err == nil {
			t.Errorf("%s: a push without data must be reported", name)
		}
	}
	// IsPositionStatus and IsDebtRatio on hand-made events.
	for _, tc := range []struct {
		ev                CrossMarginPositionEvent
		status, debtRatio bool
	}{
		{CrossMarginPositionEvent{}, false, false},
		{CrossMarginPositionEvent{Type: PositionStatusLiability}, true, false},
	} {
		if tc.ev.IsPositionStatus() != tc.status || tc.ev.IsDebtRatio() != tc.debtRatio {
			t.Errorf("%+v: status=%v debtRatio=%v", tc.ev, tc.ev.IsPositionStatus(), tc.ev.IsDebtRatio())
		}
	}
}
