package streaming

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/auth"
	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
	"github.com/tigusigalpa/kucoin-go/websocket/uta"
)

func TestOrders(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("orderAll", fxOrderSpotLive, fxOrderSpotFilled, fxOrderConditional, fxOrderTriggered, fxOrderCanceled, fxOrderUnified)
	h.fake.OnSubscribe("order|XBTUSDTM", fxOrderTPSL, fxOrderPartialCanceled)
	h.fake.OnSubscribe("order|ETHUSDTM", fxOrderPartialVariant)
	s := h.private(t)
	if s.TradeType() != "" {
		t.Fatalf("a private session has no trade type, got %q", s.TradeType())
	}
	all, err := s.SubscribeOrders(ctx5(t), "")
	if err != nil {
		t.Fatal(err)
	}
	btc, err := s.SubscribeOrders(ctx5(t), "XBTUSDTM")
	if err != nil {
		t.Fatal(err)
	}
	eth, err := s.SubscribeOrders(ctx5(t), "ETHUSDTM")
	if err != nil {
		t.Fatal(err)
	}
	if f := h.frames("subscribe", "orderAll")[0]; f["tradeType"] != "UNIFIED" || f["symbol"] != nil || f["symbols"] != nil {
		t.Fatalf("orderAll frame: %v", f)
	}
	if f := h.frames("subscribe", "order")[0]; f["tradeType"] != "UNIFIED" || f["symbol"] != "XBTUSDTM" {
		t.Fatalf("order frame: %v", f)
	}

	// Spot market order: live (status 2), then filled (status 3).
	live := next(t, all)
	equal(t, "live", live, OrderUpdate{
		PushType: "orderAll.UNIFIED", OrderID: "409225265957388288", ClientOrderID: "39350f44-56ef-4b91-9788-708d9ed83e95",
		Status: OrderStatusLive, EventType: OrderEventOpen, TradeType: TradeTypeSpot, Symbol: "BTC-USDT", Side: SideBuy,
		OrderType: OrderTypeMarket, Source: OrderSourceUser, Quantity: "0.00001", QuantityUnit: QuantityUnitBase,
		FilledQuantity: "0", LastFilledQuantity: "0", LastFilledPrice: "0", AveragePrice: "0", Fee: "0", FeeCurrency: "USDT", Tax: "0",
		CanceledQuantity: "0", RemainingQuantity: "0.00001", TimeInForce: TimeInForceGTC,
		CreationTimestamp: 1770380108442909926, UpdateTimestamp: 1770380108525522700, GatewayTimestamp: 1770380108525898382,
	})
	if !live.Price.IsEmpty() {
		t.Fatalf("a market order has no price: %q", live.Price)
	}
	if live.Time().UnixNano() != 1770380108525522700 || live.CreatedTime().UnixNano() != 1770380108442909926 || live.GatewayTime().UnixNano() != 1770380108525898382 {
		t.Fatalf("times: %v %v %v", live.Time(), live.CreatedTime(), live.GatewayTime())
	}
	equal(t, "filled", next(t, all), OrderUpdate{
		PushType: "orderAll.UNIFIED", OrderID: "409225265957388288", ClientOrderID: "39350f44-56ef-4b91-9788-708d9ed83e95",
		Status: OrderStatusFilled, EventType: OrderEventFill, TradeType: TradeTypeSpot, Symbol: "BTC-USDT", Side: SideBuy,
		OrderType: OrderTypeMarket, LiquidityRole: LiquidityTaker, Source: OrderSourceUser, TradeID: "21105572357619712",
		Quantity: "0.00001", QuantityUnit: QuantityUnitBase, FilledQuantity: "0.00001", LastFilledQuantity: "0.00001", LastFilledPrice: "66421",
		AveragePrice: "66421", Fee: "0.00066421", FeeCurrency: "USDT", Tax: "0", CanceledQuantity: "0", RemainingQuantity: "0",
		TimeInForce: TimeInForceGTC, CreationTimestamp: 1770380108442909926, UpdateTimestamp: 1770380108526000000, GatewayTimestamp: 1770380108536802787,
	})

	// Conditional order: not triggered (0), then triggered (1).
	cond := next(t, all)
	if cond.Status != OrderStatusNotTriggered || cond.EventType != OrderEventOpen || cond.TriggerDirection != TriggerUp ||
		cond.TriggerPrice != "66220" || cond.TriggerPriceType != TriggerPriceTrade || cond.OrderID != "409226359718625280" {
		t.Fatalf("not triggered: %+v", cond)
	}
	trig := next(t, all)
	if trig.Status != OrderStatusTriggered || trig.EventType != OrderEventTrigger || trig.TriggerDirection != TriggerUp || trig.OrderID != cond.OrderID {
		t.Fatalf("triggered: %+v", trig)
	}
	canceled := next(t, all)
	if canceled.Status != OrderStatusCanceled || canceled.EventType != OrderEventCancel || canceled.CancelReason != CancelReasonSystemCancel ||
		canceled.CanceledQuantity != "1" || !canceled.ReduceOnly || canceled.TriggerDirection != TriggerDown ||
		canceled.TriggeredOrderID != "409094459008032768" || canceled.TradeType != TradeTypeFutures || canceled.QuantityUnit != QuantityUnitUnit {
		t.Fatalf("canceled: %+v", canceled)
	}
	// The "UTA - UNIFIED" example: another key order, an empty margin mode and a numeric cS.
	unified := next(t, all)
	if unified.OrderID != "433439467777417216" || unified.Status != OrderStatusLive || unified.Symbol != "XRP-USDT" || unified.Side != SideBuy ||
		unified.MarginMode != "" || unified.CanceledQuantity != "0" || unified.RemainingQuantity != "0.1" || unified.Quantity != "0.1" {
		t.Fatalf("unified: %+v", unified)
	}

	// Single-symbol channel: a futures order with take-profit and stop-loss.
	tpsl := next(t, btc)
	if tpsl.PushType != "order.UNIFIED" || tpsl.Symbol != "XBTUSDTM" || tpsl.TakeProfitPrice != "64880" || tpsl.TakeProfitPriceType != TriggerPriceTrade ||
		tpsl.StopLossPrice != "64800" || tpsl.StopLossPriceType != TriggerPriceTrade || tpsl.Quantity != "1" || tpsl.QuantityUnit != QuantityUnitUnit {
		t.Fatalf("tpsl: %+v", tpsl)
	}
	// Statuses 6 and 4 are not in the documentation's examples; these frames carry
	// distinct values for every field so that no mapping can go unnoticed.
	equal(t, "partially canceled", next(t, btc), OrderUpdate{
		PushType: "order.UNIFIED", OrderID: "901", Status: OrderStatusPartiallyCanceled, EventType: OrderEventCancel, TradeType: TradeTypeFutures,
		Symbol: "XBTUSDTM", Side: SideBuy, PositionSide: PositionSideLong, OrderType: OrderTypeLimit, Source: OrderSourceUser,
		Price: "64750", MarginMode: MarginModeIsolated, Quantity: "5", FilledQuantity: "2", LastFilledQuantity: "1", LastFilledPrice: "64750.5",
		CancelReason: CancelReasonUser, CanceledQuantity: "3", RemainingQuantity: "0",
		TakeProfitPrice: "65000", TakeProfitPriceType: TriggerPriceMark, TakeProfitOrderPrice: "65010",
		StopLossPrice: "64000", StopLossPriceType: TriggerPriceIndex, StopLossOrderPrice: "63990",
		CreationTimestamp: 7, UpdateTimestamp: 8, GatewayTimestamp: 12,
	})
	equal(t, "partially filled", next(t, eth), OrderUpdate{
		PushType: "order.UNIFIED", OrderID: "900", ClientOrderID: "c-1", Status: OrderStatusPartiallyFilled, EventType: OrderEventMatch,
		TradeType: TradeTypeFutures, Symbol: "ETHUSDTM", Side: SideSell, PositionSide: PositionSideShort, OrderType: OrderTypeLimit,
		LiquidityRole: LiquidityMaker, Source: OrderSourceUser, Price: "2002", MarginMode: MarginModeCross, TradeID: "4242",
		Quantity: "10", QuantityUnit: QuantityUnitUnit, FilledQuantity: "3", LastFilledQuantity: "3", LastFilledPrice: "2001.5",
		AveragePrice: "2001.2", Fee: "0.5", FeeCurrency: "USDT", Tax: "0.01", CanceledQuantity: "0", RemainingQuantity: "7",
		SelfTradePrevention: STPCancelBoth, ReduceOnly: true, TimeInForce: TimeInForceGTT, PostOnly: true,
		CreationTimestamp: 5, UpdateTimestamp: 6, Broker: "broker", GatewayTimestamp: 11,
	})
}

func TestOrderStatusAndEnums(t *testing.T) {
	for status, name := range map[OrderStatus]string{
		OrderStatusNotTriggered: "notTriggered", OrderStatusTriggered: "triggered", OrderStatusLive: "live", OrderStatusFilled: "filled",
		OrderStatusPartiallyFilled: "partialFilled", OrderStatusCanceled: "canceled", OrderStatusPartiallyCanceled: "partialCanceled", 9: "unknown",
	} {
		if status.String() != name {
			t.Errorf("OrderStatus(%d) = %q, want %q", int(status), status.String(), name)
		}
	}
	for in, want := range map[string]OrderStatus{`3`: 3, `"4"`: 4, `null`: 0} {
		var s OrderStatus
		if err := s.UnmarshalJSON([]byte(in)); err != nil || s != want {
			t.Errorf("UnmarshalJSON(%s) = %v, %v", in, s, err)
		}
	}
	var s OrderStatus
	if err := s.UnmarshalJSON([]byte(`{"x":1}`)); err == nil {
		t.Error("a JSON object is not a status")
	}
	var c CollateralStatus
	for in, want := range map[string]CollateralStatus{`1`: CollateralNoRestriction, `"2"`: CollateralNearingRestriction, `3`: CollateralRestrictionTrigger} {
		if err := c.UnmarshalJSON([]byte(in)); err != nil || c != want {
			t.Errorf("CollateralStatus %s = %v, %v", in, c, err)
		}
	}
	before := c
	if err := c.UnmarshalJSON([]byte(`null`)); err != nil || c != before {
		t.Errorf("null must leave the status alone: %v %v", c, err)
	}
	if err := c.UnmarshalJSON([]byte(`[]`)); err == nil {
		t.Error("an array is not a collateral status")
	}
	for _, a := range []AccountType{AccountTypeUnified, AccountTypeFunding, AccountTypeIsolated} {
		if !a.Valid() {
			t.Errorf("%q must be valid", a)
		}
	}
	if AccountType("SPOT").Valid() || AccountType("").Valid() {
		t.Error("SPOT and the empty account type are not offered")
	}
}

func TestExecutions(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("execution", fxExecutionNormal, fxExecutionADL, fxExecutionLiquid, fxExecutionSettlement, fxExecutionVariant)
	sub, err := h.private(t).SubscribeExecutions(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if f := h.frames("subscribe", "execution")[0]; f["tradeType"] != "UNIFIED" || f["symbol"] != nil {
		t.Fatalf("subscribe frame: %v", f)
	}
	normal := next(t, sub)
	equal(t, "normal", normal, Execution{
		OrderID: "476907831686172672", ClientOrderID: "9b5620d9-ef1c-4ab5-89d1-717b2602d445", TradeID: "22221471895799808",
		Symbol: "XRP-USDT", Side: SideBuy, OrderType: OrderTypeMarket, Price: "1.01972", Size: "0.9806", LiquidityRole: LiquidityTaker,
		Fee: "0.000999937432", FeeCurrency: "USDT", FillType: FillTypeNormal, ExecutionTimestamp: 1786516889638000000, GatewayTimestamp: 1786516889648920721,
	})
	if normal.Time().UnixNano() != 1786516889638000000 || normal.GatewayTime().UnixNano() != 1786516889648920721 {
		t.Fatalf("times: %v %v", normal.Time(), normal.GatewayTime())
	}
	// Fills nobody ordered: the order ID is "0" and the order type and role may be empty.
	equal(t, "adl", next(t, sub), Execution{
		OrderID: "0", TradeID: "1785856966900", Symbol: "XBTUSDTM", Side: SideBuy, Price: "81580.1469961881", Size: "34",
		Fee: "0", FeeCurrency: "USDT", FillType: FillTypeADL, ExecutionTimestamp: 1786353005359000000, GatewayTimestamp: 1786353005793808741,
	})
	equal(t, "liquid", next(t, sub), Execution{
		OrderID: "0", TradeID: "1780500473345", Symbol: "XBTUSDTM", Side: SideBuy, Price: "63962.4799219875", Size: "8", LiquidityRole: LiquidityTaker,
		Fee: "0", FeeCurrency: "USDT", FillType: FillTypeLiquid, ExecutionTimestamp: 1785393975227000000, GatewayTimestamp: 1785393975535846294,
	})
	equal(t, "settlement", next(t, sub), Execution{
		OrderID: "0", TradeID: "1780499594698", Symbol: "ETHUSDTM", Side: SideSell, Price: "3800", Size: "40", LiquidityRole: LiquidityTaker,
		Fee: "0", FeeCurrency: "USDT", FillType: FillTypeSettlement, ExecutionTimestamp: 1785399943899000000, GatewayTimestamp: 1785399944158708818,
	})
	equal(t, "closing fill", next(t, sub), Execution{
		OrderID: "55", ClientOrderID: "c-9", TradeID: "9", Symbol: "ETHUSDTM", Side: SideSell, OrderType: OrderTypeLimit, Price: "2000.5", Size: "2",
		LiquidityRole: LiquidityMaker, Fee: "0.004", FeeCurrency: "USDT", FillType: FillTypeNormal, ClosedPnL: "-1.25",
		ExecutionTimestamp: 99, GatewayTimestamp: 13,
	})
}

func TestExecutionsLite(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("execution.lite", fxExecutionLiteSpot, fxExecutionLiteFutures, fxExecutionLiteMargin, fxExecutionLiteVariant)
	sub, err := h.private(t).SubscribeExecutionsLite(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if f := h.frames("subscribe", "execution.lite")[0]; f["tradeType"] != "UNIFIED" {
		t.Fatalf("subscribe frame: %v", f)
	}
	// The trade ID is a number on spot (17 digits: it must stay exact) and a string on futures.
	spot := next(t, sub)
	equal(t, "spot", spot, ExecutionLite{
		OrderID: "427992448220876800", TradeID: "22075878662488064", Symbol: "BTC-USDT", Side: SideBuy, OrderType: OrderTypeMarket,
		Price: "67396.6", Size: "0.00001483", LiquidityRole: LiquidityTaker, ExecutionTimestamp: 1774854553397000000, GatewayTimestamp: 1774854553398639926,
	})
	if spot.Time().UnixNano() != 1774854553397000000 || spot.GatewayTime().UnixNano() != 1774854553398639926 {
		t.Fatalf("times: %v %v", spot.Time(), spot.GatewayTime())
	}
	equal(t, "futures", next(t, sub), ExecutionLite{
		OrderID: "427993476303507456", TradeID: "1928560862264", Symbol: "XBTUSDTM", Side: SideBuy, OrderType: OrderTypeMarket,
		Price: "67391.6", Size: "1", LiquidityRole: LiquidityTaker, ExecutionTimestamp: 1774854798514000000, GatewayTimestamp: 1774854798514737265,
	})
	equal(t, "margin", next(t, sub), ExecutionLite{
		OrderID: "433799474222030848", TradeID: "20939120797435904", Symbol: "XRP-USDT", Side: SideBuy, OrderType: OrderTypeMarket,
		Price: "1.35294", Size: "0.7391", LiquidityRole: LiquidityTaker, ExecutionTimestamp: 1776239056263000000, GatewayTimestamp: 1776239056265949041,
	})
	equal(t, "variant", next(t, sub), ExecutionLite{
		OrderID: "66", ClientOrderID: "c-7", TradeID: "88", Symbol: "SOL-USDT", Side: SideSell, OrderType: OrderTypeLimit,
		Price: "118.7", Size: "3", LiquidityRole: LiquidityMaker, ExecutionTimestamp: 77, GatewayTimestamp: 14,
	})
}

func TestBalance(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("balance", fxBalanceUnified, fxBalanceVariant, fxBalanceVariant2, fxBalanceFunding)
	s := h.private(t)
	unified, err := s.SubscribeBalance(ctx5(t), AccountTypeUnified)
	if err != nil {
		t.Fatal(err)
	}
	funding, err := s.SubscribeBalance(ctx5(t), AccountTypeFunding)
	if err != nil {
		t.Fatal(err)
	}
	frames := h.frames("subscribe", "balance")
	if len(frames) != 2 || frames[0]["accountType"] != "UNIFIED" || frames[1]["accountType"] != "FUNDING" || frames[0]["tradeType"] != nil {
		t.Fatalf("subscribe frames: %v", frames)
	}
	first := next(t, unified)
	equal(t, "unified", first, BalanceUpdate{
		AccountType: AccountTypeUnified, Currency: "BTC", Equity: "0.0000517000", Balance: "0.0000517000", Available: "0.0000517000",
		Hold: "0.0000000000", Liability: "0.0000000000", CollateralStatus: CollateralNoRestriction,
		UpdateTimestamp: 1770116995058000000, GatewayTimestamp: 1770116995060810093,
	})
	if first.Time().UnixNano() != 1770116995058000000 || first.GatewayTime().UnixNano() != 1770116995060810093 {
		t.Fatalf("times: %v %v", first.Time(), first.GatewayTime())
	}
	// Every optional field; the equity is null, and the sequence E comes before it.
	equal(t, "all fields", next(t, unified), BalanceUpdate{
		AccountType: AccountTypeUnified, Currency: "USDT", Balance: "100", Available: "60", Hold: "40", Liability: "5",
		TotalCrossMargin: "1", CrossPosMargin: "2", CrossOrderMargin: "3", CrossUnrealisedPnL: "4",
		IsolatedPosMargin: "5", IsolatedOrderMargin: "6", IsolatedFundingFeeMargin: "7", IsolatedUnrealisedPnL: "8",
		CollateralStatus: CollateralRestrictionTrigger, Sequence: 3, UpdateTimestamp: 1770116995059000000, GatewayTimestamp: 15,
	})
	equal(t, "equity then sequence", next(t, unified), BalanceUpdate{
		AccountType: AccountTypeUnified, Currency: "ETH", Equity: "11.5", Balance: "10", Available: "9", Hold: "1",
		Sequence: 4, UpdateTimestamp: 1770116995059000000, GatewayTimestamp: 16,
	})
	// The funding account sends U as a numeric string, in milliseconds.
	fund := next(t, funding)
	equal(t, "funding", fund, BalanceUpdate{
		AccountType: AccountTypeFunding, Currency: "USDT", Balance: "531.42853547", Available: "531.42853547", Hold: "0",
		UpdateTimestamp: 1780543242933, GatewayTimestamp: 1780543242950333125,
	})
	if fund.Time().UnixMilli() != 1780543242933 {
		t.Fatalf("a millisecond timestamp must convert as milliseconds: %v", fund.Time())
	}
}

func TestPositions(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("positionAll", fxPosition)
	h.fake.OnSubscribe("position|XBTUSDTM", fxPositionVariant)
	s := h.private(t)
	all, err := s.SubscribePositions(ctx5(t), "")
	if err != nil {
		t.Fatal(err)
	}
	one, err := s.SubscribePositions(ctx5(t), "XBTUSDTM")
	if err != nil {
		t.Fatal(err)
	}
	if f := h.frames("subscribe", "positionAll")[0]; f["tradeType"] != "UNIFIED" || f["symbol"] != nil {
		t.Fatalf("positionAll frame: %v", f)
	}
	if f := h.frames("subscribe", "position")[0]; f["tradeType"] != "UNIFIED" || f["symbol"] != "XBTUSDTM" {
		t.Fatalf("position frame: %v", f)
	}
	v := next(t, all)
	equal(t, "position", v, PositionUpdate{
		PushType: "positionAll.UNIFIED", PositionID: "70000000000000010", Symbol: "TRUMPUSDTM", MarginMode: MarginModeCross, Size: "-46",
		EntryPrice: "4.8855208333333333333", PositionValue: "19.4396", MarkPrice: "4.226", LiquidationPrice: "5.2913432001",
		BankruptcyPrice: "5.3580141244", Leverage: "2", UnrealisedPnL: "3.03379583333333333332", RealisedPnL: "-0.20267048333333333332",
		InitialMargin: "9.7198", MaintenanceMarginRate: "0.012", MaintenanceMargin: "0.2332752", RiskRatio: "0.2001", ADL: "0.46",
		UpdateTimestamp: 1770117071740000000, CreationTimestamp: 1768876206493000000, GatewayTimestamp: 1770117071746459555,
	})
	if v.Time().UnixNano() != 1770117071740000000 || v.CreatedTime().UnixNano() != 1768876206493000000 || v.GatewayTime().UnixNano() != 1770117071746459555 {
		t.Fatalf("times: %v %v %v", v.Time(), v.CreatedTime(), v.GatewayTime())
	}
	equal(t, "single symbol", next(t, one), PositionUpdate{
		PushType: "position.UNIFIED", PositionID: "71", Symbol: "XBTUSDTM", MarginMode: MarginModeIsolated, Size: "3", EntryPrice: "90000.5",
		PositionValue: "270001.5", PositionMargin: "13500", MarkPrice: "90001", LiquidationPrice: "85000", BankruptcyPrice: "84000", Leverage: "20",
		UnrealisedPnL: "1.5", RealisedPnL: "-2.5", InitialMargin: "13500.1", MaintenanceMarginRate: "0.004", MaintenanceMargin: "1080",
		RiskRatio: "0.65", ADL: "0.12", UpdateTimestamp: 9, CreationTimestamp: 4, GatewayTimestamp: 17,
	})
}

func TestLiquidationWarning(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("lw", fxLiquidationWarning, fxLiquidationRisk)
	sub, err := h.private(t).SubscribeLiquidationWarning(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if f := h.frames("subscribe", "lw")[0]; f["tradeType"] != "UNIFIED" {
		t.Fatalf("subscribe frame: %v", f)
	}
	w := next(t, sub)
	equal(t, "lw", w, LiquidationWarning{
		PushType: "lw.UNIFIED", Event: RiskEventMarginCall, RiskRatio: "0.9378", AdjustedEquity: "12456.32", InitialMargin: "58720.00",
		MaintenanceMargin: "5284.80", AvailableMargin: "1890.45", Equity: "63710.77", Liability: "51254.45",
		UpdateTimestamp: 1729842192785100000, GatewayTimestamp: 1729842192785164840,
	})
	if w.Time().UnixNano() != 1729842192785100000 || w.GatewayTime().UnixNano() != 1729842192785164840 {
		t.Fatalf("times: %v %v", w.Time(), w.GatewayTime())
	}
	// KuCoin documents the push type as lw.UNIFIED and as risk.UNIFIED; both arrive.
	equal(t, "risk", next(t, sub), LiquidationWarning{
		PushType: "risk.UNIFIED", Event: RiskEventForceLiquidation, RiskRatio: "1.0123", AdjustedEquity: "1", InitialMargin: "2",
		MaintenanceMargin: "3", AvailableMargin: "4", Equity: "5", Liability: "6",
		UpdateTimestamp: 1729842252785100000, GatewayTimestamp: 1729842252785164840,
	})
}

func TestLeverage(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("leverage", fxLeverageFutures, fxLeverageMargin)
	sub, err := h.private(t).SubscribeLeverage(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if f := h.frames("subscribe", "leverage")[0]; f["tradeType"] != "UNIFIED" {
		t.Fatalf("subscribe frame: %v", f)
	}
	fut := next(t, sub)
	equal(t, "futures", fut, LeverageUpdate{
		Symbol: "XBTUSDTM", Leverage: "5.00", MarginMode: MarginModeCross, TradeType: TradeTypeFutures, GatewayTimestamp: 1764570290237940700,
	})
	if fut.Time().UnixNano() != 1764570290237940700 {
		t.Fatalf("Time() = %v", fut.Time())
	}
	// A margin update names the currency and no symbol, so it is routed by the wildcard.
	equal(t, "margin", next(t, sub), LeverageUpdate{
		Currency: "BTC", Leverage: "5.00", MarginMode: MarginModeCross, TradeType: TradeTypeMargin, GatewayTimestamp: 1764570290237940710,
	})
}

func TestSingleSymbolAndAllSymbolsOrdersAreNotCrossRouted(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("order")
	h.fake.OnSubscribe("orderAll")
	s := h.private(t)
	one, err := s.SubscribeOrders(ctx5(t), "XBTUSDTM")
	if err != nil {
		t.Fatal(err)
	}
	all, err := s.SubscribeOrders(ctx5(t), "")
	if err != nil {
		t.Fatal(err)
	}
	// The single-symbol channel pushes "order.UNIFIED", the all-symbols one "orderAll.UNIFIED".
	if err := h.fake.Push(fxOrderTPSL); err != nil {
		t.Fatal(err)
	}
	if err := h.fake.Push(fxOrderCanceled); err != nil {
		t.Fatal(err)
	}
	if v := next(t, one); v.OrderID != "409094459008032768" {
		t.Fatalf("single-symbol: %+v", v)
	}
	if v := next(t, all); v.OrderID != "409094459027223421" {
		t.Fatalf("all symbols: %+v", v)
	}
	expectNothing(t, one, "the all-symbols push on the single-symbol subscription")
	expectNothing(t, all, "the single-symbol push on the all-symbols subscription")
}

// fixedClock is a transport.Clock that always reads the same instant.
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func TestDialPrivateWithoutCredentialsFailsBeforeAnyNetworkAccess(t *testing.T) {
	wstest.CheckLeaks(t)
	fake := wstest.NewUTAFake(t)
	for name, creds := range map[string]*transport.Credentials{
		"nil":            nil,
		"empty":          {},
		"only a key":     {APIKey: "k"},
		"no passphrase":  {APIKey: "k", APISecret: "s"},
		"no secret":      {APIKey: "k", APIPassphrase: "p"},
		"no key":         {APISecret: "s", APIPassphrase: "p"},
		"only a version": {APIKeyVersion: "3"},
	} {
		svc := NewService(Hosts{Private: fake.URL()}, creds, nil, fastOptions()...)
		_, err := svc.DialPrivate(ctx5(t))
		if !errors.Is(err, uta.ErrIncompleteCredentials) || !stream.IsPermanent(err) {
			t.Errorf("%s: error = %v, want a permanent ErrIncompleteCredentials", name, err)
		}
	}
	if fake.Server.Connections() != 0 {
		t.Fatalf("%d connection(s) were attempted without credentials", fake.Server.Connections())
	}
}

func TestDialPrivateAuthenticatesWithAValidSignature(t *testing.T) {
	h := newHarness(t)
	now := time.UnixMilli(1_742_175_983_882)
	got := make(chan map[string]any, 4)
	h.fake.OnAuth(func(m map[string]any) map[string]any {
		got <- m
		return map[string]any{"id": m["id"], "result": true}
	})
	svc := h.svc.WithClock(fixedClock{now})
	if svc == h.svc || h.svc.clock != nil {
		t.Fatal("WithClock must return a copy and leave the original alone")
	}
	s, err := svc.DialPrivate(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	m := <-got
	signer := auth.NewSigner(testCreds.APISecret)
	ts := auth.TimestampMillis(now)
	if m["op"] != "auth" || m["kc-api-key"] != testCreds.APIKey || m["kc-api-timestamp"] != ts || wstest.Str(m, "id") == "" {
		t.Fatalf("auth frame: %v", m)
	}
	if want := signer.Sign(ts, "POST", "/api/websocket/users/verify", ""); m["kc-api-sign"] != want {
		t.Fatalf("signature = %v, want %v", m["kc-api-sign"], want)
	}
	if want := signer.SignPassphrase(testCreds.APIPassphrase); m["kc-api-passphrase"] != want {
		t.Fatalf("passphrase signature = %v, want %v", m["kc-api-passphrase"], want)
	}
	if text := fmt.Sprint(m); strings.Contains(text, testCreds.APISecret) || strings.Contains(text, testCreds.APIPassphrase) {
		t.Fatalf("neither the secret nor the raw passphrase may be sent: %s", text)
	}
}

func TestPublicSessionsNeverSendCredentials(t *testing.T) {
	h := newHarness(t)
	h.futures(t)
	h.spot(t)
	for _, m := range h.fake.Frames() {
		if m["op"] == "auth" {
			t.Fatalf("a public session authenticated: %v", m)
		}
	}
}

func TestServiceCopiesItsCredentials(t *testing.T) {
	wstest.CheckLeaks(t)
	fake := wstest.NewUTAFake(t)
	got := make(chan map[string]any, 1)
	fake.OnAuth(func(m map[string]any) map[string]any {
		got <- m
		return map[string]any{"id": m["id"], "result": true}
	})
	creds := testCreds
	svc := NewService(Hosts{Private: fake.URL()}, &creds, nil, fastOptions()...)
	creds.APIKey, creds.APISecret, creds.APIPassphrase = "", "", "" // the caller reuses its variable
	s, err := svc.DialPrivate(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if m := <-got; m["kc-api-key"] != testCreds.APIKey {
		t.Fatalf("auth frame used %v", m["kc-api-key"])
	}
}

func TestDialPrivateRejectedKeyIsTypedAndPermanent(t *testing.T) {
	h := newHarness(t)
	h.fake.OnAuth(func(m map[string]any) map[string]any {
		return map[string]any{"id": m["id"], "result": false, "message": "auth failed"}
	})
	_, err := h.svc.DialPrivate(ctx5(t))
	var se *stream.ServerError
	if !errors.Is(err, uta.ErrAuthenticationFailed) || !stream.IsPermanent(err) || !errors.As(err, &se) || se.Message != "auth failed" {
		t.Fatalf("error = %v, want a permanent ErrAuthenticationFailed carrying KuCoin's reason", err)
	}
	time.Sleep(100 * time.Millisecond)
	if n := h.fake.Server.Connections(); n != 1 {
		t.Fatalf("connections = %d; a rejected key must not be retried", n)
	}
	if n := h.fake.Count("subscribe", ""); n != 0 {
		t.Fatalf("%d subscribe frames after a failed authentication", n)
	}
}

func TestPrivateSessionReauthenticatesAfterAReconnect(t *testing.T) {
	h := newHarness(t)
	var mu sync.Mutex
	auths := 0
	h.fake.OnAuth(func(m map[string]any) map[string]any {
		mu.Lock()
		auths++
		mu.Unlock()
		return map[string]any{"id": m["id"], "result": true}
	})
	h.fake.OnSubscribe("orderAll")
	s := h.private(t)
	sub, err := s.SubscribeOrders(ctx5(t), "")
	if err != nil {
		t.Fatal(err)
	}
	h.fake.Server.DropAll()
	eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return h.fake.Server.Connections() == 2 && auths == 2 && h.fake.Count("subscribe", "orderAll") == 2 && s.State() == stream.StateConnected
	}, "re-authentication and resubscription on the new connection")
	if err := h.fake.Push(fxOrderSpotFilled); err != nil {
		t.Fatal(err)
	}
	if v := next(t, sub); v.Status != OrderStatusFilled {
		t.Fatalf("order after reconnect: %+v", v)
	}
}

func TestPrivateAuthenticationFailureAfterAReconnectEndsTheSession(t *testing.T) {
	h := newHarness(t)
	var mu sync.Mutex
	auths := 0
	h.fake.OnAuth(func(m map[string]any) map[string]any {
		mu.Lock()
		auths++
		n := auths
		mu.Unlock()
		if n == 1 {
			return map[string]any{"id": m["id"], "result": true}
		}
		return map[string]any{"id": m["id"], "result": false, "message": "key revoked"}
	})
	s := h.private(t)
	sub, err := s.SubscribeBalance(ctx5(t), AccountTypeUnified)
	if err != nil {
		t.Fatal(err)
	}
	h.fake.Server.DropAll()
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the session did not end after the key was rejected")
	}
	if err := s.Err(); !errors.Is(err, uta.ErrAuthenticationFailed) {
		t.Fatalf("Err = %v", err)
	}
	select {
	case <-sub.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the subscription did not end with the session")
	}
	if !errors.Is(sub.Err(), uta.ErrAuthenticationFailed) {
		t.Fatalf("subscription Err = %v", sub.Err())
	}
	mu.Lock()
	defer mu.Unlock()
	if auths != 2 {
		t.Fatalf("auth attempts = %d, want exactly 2", auths)
	}
}

func TestDialPrivateAfterTheContextIsCanceled(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if s, err := h.svc.DialPrivate(ctx); err == nil {
		_ = s.Close()
		t.Fatal("a canceled context must fail the dial")
	}
}
