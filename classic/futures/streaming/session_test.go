package streaming

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	classicws "github.com/tigusigalpa/kucoin-go/classic/ws"
	"github.com/tigusigalpa/kucoin-go/internal/wsengine"
	"github.com/tigusigalpa/kucoin-go/internal/wstest"
	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
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

func TestTickerV2(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractMarket/tickerV2:XBTUSDTM", fxTickerV2)
	sub, err := h.public(t).SubscribeTickerV2(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, sub)
	if v.Symbol != "XBTUSDTM" || v.Sequence != 1713516609293 || v.BestBidPrice != "86454.5" || v.BestBidSize != 5044 ||
		v.BestAskPrice != "86454.6" || v.BestAskSize != 73 || v.Timestamp != 1740641976241000000 {
		t.Fatalf("ticker: %+v", v)
	}
	if v.Time().UnixNano() != 1740641976241000000 {
		t.Fatalf("Time() = %v", v.Time())
	}
}

func TestTickerV2_MultipleSymbolsAreRoutedAndNamed(t *testing.T) {
	h := newHarness(t)
	topic := "/contractMarket/tickerV2:XBTUSDTM,ETHUSDTM"
	h.fake.OnSubscribe(topic,
		strings.ReplaceAll(fxTickerV2, "XBTUSDTM", "ETHUSDTM"),
		fxTickerV2)
	sub, err := h.public(t).SubscribeTickerV2(ctx5(t), []string{"XBTUSDTM", "ETHUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	if a, b := next(t, sub), next(t, sub); a.Symbol != "ETHUSDTM" || b.Symbol != "XBTUSDTM" {
		t.Fatalf("symbols: %s, %s", a.Symbol, b.Symbol)
	}
	if sub.Key() != topic {
		t.Fatalf("key = %q", sub.Key())
	}
}

func TestTickerV1(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractMarket/ticker:XBTUSDTM", fxTickerV1)
	sub, err := h.public(t).SubscribeTickerV1(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, sub)
	if v.Symbol != "XBTUSDTM" || v.Side != SideBuy || v.Size != 2 || v.Price != "86429.7" || v.TradeID != "1828964168748" ||
		v.BestBidPrice != "86429.6" || v.BestBidSize != 112 || v.BestAskPrice != "86429.7" || v.BestAskSize != 1578 || v.Timestamp != 1740642161735000000 {
		t.Fatalf("ticker v1: %+v", v)
	}
	_ = v.Time()
}

func TestDepth5AndDepth50(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractMarket/level2Depth5:XBTUSDTM", fxDepth5)
	h.fake.OnSubscribe("/contractMarket/level2Depth50:XBTUSDTM", fxDepth50)
	s := h.public(t)
	d5, err := s.SubscribeDepth5(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	d50, err := s.SubscribeDepth50(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, d5)
	if v.Symbol != "XBTUSDTM" || v.Sequence != 1709294294670 || len(v.Bids) != 5 || len(v.Asks) != 5 ||
		v.Bids[0].Price != "89720.9" || v.Bids[0].Size != "513" || v.Asks[0].Price != "89721" || v.Asks[4].Size != "113" ||
		v.Timestamp != 1731680019100 || v.Ts != 1731680019100 {
		t.Fatalf("depth5: %+v", v)
	}
	if v.Time().UnixMilli() != 1731680019100 {
		t.Fatalf("Time() = %v", v.Time())
	}
	w := next(t, d50)
	if len(w.Bids) != 2 || len(w.Asks) != 2 || w.Bids[1].Price != "89778.2" || w.Asks[1].Size != "4" {
		t.Fatalf("depth50: %+v", w)
	}
}

func TestOrderBookChanges(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractMarket/level2:XBTUSDTM", fxLevel2)
	sub, err := h.public(t).SubscribeOrderBookChanges(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, sub)
	if v.Symbol != "XBTUSDTM" || v.Sequence != 1709400450243 || v.Side != orderbook.Ask || v.Price != "90631.2" || v.Size != "2" ||
		v.Timestamp != 1731897467182 || v.Change != "90631.2,sell,2" {
		t.Fatalf("change: %+v", v)
	}
	_ = v.Time()
}

func TestKlines_FieldOrderIsOpenCloseHighLow(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractMarket/limitCandle:XBTUSDTM_1min", fxKline)
	h.fake.OnSubscribe("/contractMarket/limitCandle:ETHUSDTM_5min", fxKlineVariant)
	s := h.public(t)
	one, err := s.SubscribeKlines(ctx5(t), Interval1Min, []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	five, err := s.SubscribeKlines(ctx5(t), Interval5Min, []string{"ETHUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, one)
	if v.Symbol != "XBTUSDTM" || v.Interval != Interval1Min || v.StartTime != 1790993280 || v.Open != "84568.3" || v.Close != "84556.7" || v.High != "84568.3" || v.Low != "84544.2" ||
		v.Timestamp != 1790993360463 || len(v.Candles) != 7 {
		t.Fatalf("kline: %+v", v)
	}
	// The live push of a minute that REST reports as volume 12785 (contracts) and
	// turnover 1080991.7836 (quote currency): the wire order is turnover, volume.
	if v.Turnover != "1080991.7836" || v.Volume != "12785" {
		t.Fatalf("turnover/volume = %q/%q, want 1080991.7836/12785", v.Turnover, v.Volume)
	}
	w := next(t, five)
	if w.Symbol != "ETHUSDTM" || w.Interval != Interval5Min || w.Open != "2.1" || w.Close != "2.4" || w.High != "2.9" || w.Low != "2.0" || w.Turnover != "71.5" || w.Volume != "30" {
		t.Fatalf("variant kline: %+v", w)
	}
	if w.Start().Unix() != 1731898200 || w.Time().UnixMilli() != 1731898208357 {
		t.Fatalf("times: %v %v", w.Start(), w.Time())
	}
}

func TestTrades(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractMarket/execution:XBTUSDTM", fxTrade)
	sub, err := h.public(t).SubscribeTrades(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, sub)
	if v.Symbol != "XBTUSDTM" || v.Sequence != 1794100537695 || v.Side != SideBuy || v.Size != 2 || v.Price != "90503.9" ||
		v.TakerOrderID != "247822202957807616" || v.MakerOrderID != "247822167163555840" || v.TradeID != "1794100537695" || v.Timestamp != 1731898619520000000 {
		t.Fatalf("trade: %+v", v)
	}
	_ = v.Time()
}

func TestInstrument_MarkIndexPriceAndFundingRate(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contract/instrument:XBTUSDTM", fxInstrumentMark, fxInstrumentFunding)
	sub, err := h.public(t).SubscribeInstrument(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	m := next(t, sub)
	if !m.IsMarkIndexPrice() || m.IsFundingRate() || m.Symbol != "XBTUSDTM" || m.MarkPrice != "90445.02" || m.IndexPrice != "90445.02" || m.Granularity != 1000 || m.Timestamp != 1731899129000 || !m.FundingRate.IsEmpty() {
		t.Fatalf("mark/index: %+v", m)
	}
	f := next(t, sub)
	if !f.IsFundingRate() || f.IsMarkIndexPrice() || f.FundingRate != "-0.002966" || f.Granularity != 60000 || !f.MarkPrice.IsEmpty() {
		t.Fatalf("funding: %+v", f)
	}
	_ = f.Time()
}

func TestInstrument_FundingRateCarriesThePeriodTheLiveFeedSends(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contract/instrument:XBTUSDTM", fxInstrumentFundingLive, fxInstrumentFunding)
	sub, err := h.public(t).SubscribeInstrument(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	live := next(t, sub)
	if !live.IsFundingRate() || live.Period != 1 || live.FundingRate != "-0.000002" || live.Granularity != 60000 {
		t.Fatalf("live funding rate: %+v", live)
	}
	if documented := next(t, sub); documented.Period != 0 {
		t.Fatalf("a push without period must leave it zero: %+v", documented)
	}
}

func TestFundingSettlement(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contract/announcement", fxFundingBegin, fxFundingEnd)
	sub, err := h.public(t).SubscribeFundingSettlement(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	b, e := next(t, sub), next(t, sub)
	if b.Subject != SubjectFundingBegin || e.Subject != SubjectFundingEnd || b.Symbol != "XBTUSDTM" || b.FundingTime != 1551770400000 || b.FundingRate != "-0.002966" || e.Timestamp != 1551770410000 {
		t.Fatalf("begin=%+v end=%+v", b, e)
	}
	_ = b.Time()
}

func TestSymbolSnapshot_KeepsEveryDigit(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractMarket/snapshot:XBTUSDTM", fxSnapshot)
	sub, err := h.public(t).SubscribeSnapshot(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, sub)
	if v.Symbol != "XBTUSDTM" || v.HighPrice != "89299.9" || v.LastPrice != "86262.6" || v.LowPrice != "82205.2" || v.Price24HoursBefore != "88762.5" ||
		v.PriceChg != "-2499.9" || v.PriceChgPct != "-0.0281" || v.Volume != "12062.039" || v.Timestamp != 1740643185017646670 {
		t.Fatalf("snapshot: %+v", v)
	}
	if v.Turnover != "1033552780.2532196044" {
		t.Fatalf("turnover lost precision: %q", v.Turnover)
	}
	_ = v.Time()
}

func TestSymbolSnapshot_CarriesTheFundingRateTheLiveFeedSends(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractMarket/snapshot:XBTUSDTM", fxSnapshotLive)
	sub, err := h.public(t).SubscribeSnapshot(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, sub)
	if v.FundingRate != "-0.000002" || v.Volume != "3938.189" || v.Turnover != "337376435.2988" || v.Timestamp != 1790993335003748866 {
		t.Fatalf("live snapshot: %+v", v)
	}
}

func TestSubscribe_ARepeatedSymbolIsListedAndDeliveredOnce(t *testing.T) {
	h := newHarness(t)
	topic := "/contractMarket/tickerV2:XBTUSDTM,ETHUSDTM" // the repeat of XBTUSDTM is dropped
	h.fake.OnSubscribe(topic, strings.ReplaceAll(fxTickerV2, "XBTUSDTM", "ETHUSDTM"), fxTickerV2)
	sub, err := h.public(t).SubscribeTickerV2(ctx5(t), []string{"XBTUSDTM", "ETHUSDTM", "XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	if a, b := next(t, sub), next(t, sub); a.Symbol != "ETHUSDTM" || b.Symbol != "XBTUSDTM" {
		t.Fatalf("symbols: %s, %s", a.Symbol, b.Symbol)
	}
	select {
	case extra := <-sub.C():
		t.Fatalf("a repeated symbol delivered an update twice: %+v", extra)
	case <-time.After(100 * time.Millisecond):
	}
	if n := h.fake.Count("subscribe", topic); n != 1 {
		t.Fatalf("server saw %d subscribe frames for %s, want 1", n, topic)
	}
}

func TestJoinSymbols_RepeatsDoNotCountAgainstTheLimit(t *testing.T) {
	symbols := make([]string, 0, MaxSymbolsPerSubscription+3)
	for i := 0; i < MaxSymbolsPerSubscription; i++ {
		symbols = append(symbols, fmt.Sprintf("S%dUSDTM", i))
	}
	symbols = append(symbols, "S0USDTM", "S1USDTM", "S2USDTM")
	list, err := joinSymbols(symbols, identity)
	if err != nil {
		t.Fatalf("%d symbols with repeats must fit the limit of %d distinct ones: %v", len(symbols), MaxSymbolsPerSubscription, err)
	}
	if got := strings.Count(list, ",") + 1; got != MaxSymbolsPerSubscription {
		t.Fatalf("%d symbols listed, want %d", got, MaxSymbolsPerSubscription)
	}
	if _, err := joinSymbols(append(symbols, "ONEMOREUSDTM"), identity); !errors.Is(err, ErrTooManySymbols) {
		t.Fatalf("a %dst distinct symbol must be refused, got %v", MaxSymbolsPerSubscription+1, err)
	}
}

func TestOrders_AllEventKinds(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractMarket/tradeOrders:XBTUSDTM", fxOrderOpen, fxOrderMatch, fxOrderLiquid, fxOrderFilled, fxOrderCanceled)
	h.fake.OnSubscribe("/contractMarket/tradeOrders", fxOrderUpdate, fxOrderADL)
	s := h.private(t)
	one, err := s.SubscribeOrders(ctx5(t), "XBTUSDTM")
	if err != nil {
		t.Fatal(err)
	}
	all, err := s.SubscribeOrders(ctx5(t), "")
	if err != nil {
		t.Fatal(err)
	}
	if h.fake.Last("subscribe")["privateChannel"] != true {
		t.Fatal("private topics must be subscribed with privateChannel=true")
	}

	open := next(t, one)
	if open.Subject != "symbolOrderChange" || open.UserID != "633559791e1cbc0001f319bc" || open.ChannelType != "private" || open.Type != OrderEventOpen ||
		open.Status != OrderStatusOpen || open.Liquidity != "maker" || open.MarginMode != MarginModeIsolated || open.OrderID != "247899236673269761" ||
		open.Price != "91670" || open.Size != "1" || open.RemainSize != "1" || open.OrderTime != 1731916985768138917 || open.Timestamp != 1731916985789000000 {
		t.Fatalf("open: %+v", open)
	}
	match := next(t, one)
	if match.Type != OrderEventMatch || match.Status != OrderStatusDone || match.FeeType != "makerFee" || match.MatchPrice != "91670" || match.MatchSize != "1" || match.TradeID != "1794175373644" || match.FilledSize != "1" {
		t.Fatalf("match: %+v", match)
	}
	liquid := next(t, one)
	if liquid.TradeType != "liquid" || liquid.Status != OrderStatusMatch || liquid.Liquidity != "taker" || liquid.MatchSize != "1000" || liquid.RemainSize != "2724" || liquid.Price != "84603.44" {
		t.Fatalf("liquid: %+v", liquid)
	}
	filled := next(t, one)
	if filled.Type != OrderEventFilled || filled.MarginMode != MarginModeCross || filled.ClientOid != "5c52e11203aa677f33e493fb" || filled.PositionSide != "BOTH" || filled.AllCanceledSize != "0" {
		t.Fatalf("filled: %+v", filled)
	}
	canceled := next(t, one)
	if canceled.Type != OrderEventCanceled || canceled.CanceledSize != "1" || canceled.RemainSize != "0" || canceled.Status != OrderStatusDone {
		t.Fatalf("canceled: %+v", canceled)
	}
	update := next(t, all)
	if update.Subject != "orderChange" || update.Type != OrderEventUpdate || update.Symbol != "RUNEUSDTM" || update.OldSize != "19982" || update.OrderType != "limit" || update.ClientOid != "10496pp066R679264" || update.CanceledSize != "1037" {
		t.Fatalf("update: %+v", update)
	}
	adl := next(t, all)
	if adl.TradeType != "adl" || adl.Symbol != "10PEPEUSDTM" || adl.Price != "0.0000126" || adl.PositionSide != "BOTH" {
		t.Fatalf("adl: %+v", adl)
	}
	_ = adl.Time()
}

func TestStopOrders(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractMarket/advancedOrders", fxStopOrder)
	sub, err := h.private(t).SubscribeStopOrders(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, sub)
	if v.Subject != "stopOrder" || v.ID != "6720ab1ea52a9b0001734392" || v.UserID != "66f12e8befb04d0001882b49" || v.Type != "open" || v.Stop != "down" ||
		v.StopPrice != "1000" || v.StopPriceType != "TP" || v.OrderPrice != "0.1" || v.Size != "1" || v.CreatedAt != 1730194206837 || v.Timestamp != 1730194206843133000 || v.MarginMode != MarginModeIsolated {
		t.Fatalf("stop order: %+v", v)
	}
}

func TestBalance_CurrentAndDeprecatedSubjects(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractAccount/wallet", fxWallet, fxWalletOrderMargin, fxWalletAvailable, fxWalletWithdraw)
	sub, err := h.private(t).SubscribeBalance(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	w := next(t, sub)
	if w.Subject != SubjectWalletBalanceChange || w.IsDeprecatedSubject() || w.ID != "67c811885b87be0001a4880e" || w.Currency != "USDT" || w.Equity != "387.224858816" ||
		w.WalletBalance != "371.394298816" || w.AvailableBalance != "285.652001096" || w.MaxWithdrawAmount != "285.645841096" || w.CrossUnPnl != "2.95996" ||
		w.IsolatedFundingFeeMargin != "2.89220199" || w.Version != "2118" || w.Timestamp != 1741164936624 || w.TotalCrossMargin != "310.370835226" {
		t.Fatalf("wallet: %+v", w)
	}
	om := next(t, sub)
	if om.Subject != SubjectOrderMarginChange || !om.IsDeprecatedSubject() || om.OrderMargin != "5923" || om.Currency != "USDT" {
		t.Fatalf("order margin: %+v", om)
	}
	av := next(t, sub)
	if av.Subject != SubjectAvailableBalanceChange || av.AvailableBalance != "5923" || av.HoldBalance != "2312" {
		t.Fatalf("available: %+v", av)
	}
	wd := next(t, sub)
	if wd.Subject != SubjectWithdrawHoldChange || wd.WithdrawHold != "5923" {
		t.Fatalf("withdraw hold: %+v", wd)
	}
	_ = w.Time()
}

func TestPositions_AllThreeKinds(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contract/positionAll", fxPositionIsolated, fxPositionRisk)
	h.fake.OnSubscribe("/contract/position:XBTUSDTM", fxPositionCross, fxPositionSettle)
	s := h.private(t)
	all, err := s.SubscribePositions(ctx5(t), "")
	if err != nil {
		t.Fatal(err)
	}
	one, err := s.SubscribePositions(ctx5(t), "XBTUSDTM")
	if err != nil {
		t.Fatal(err)
	}
	iso := next(t, all)
	if !iso.IsChange() || iso.MarginMode != MarginModeIsolated || iso.CrossMode || iso.Symbol != "XBTUSDTM" || iso.CurrentQty != -1 || iso.AvgEntryPrice != "68094.2" ||
		iso.LiquidationPrice != "70130.5725363" || iso.RealLeverage != "19.1376874933" || iso.PositionSide != "SHORT" || iso.ChangeReason != "changeRiskLimit" ||
		iso.RiskLimit != "50000000" || iso.RiskLimitLevel != 5 || iso.AutoDeposit || iso.PosCross != "1" || iso.AggRate != "0.0046" || iso.OpeningTimestamp != 1771400783360 || !iso.IsOpen {
		t.Fatalf("isolated position: %+v", iso)
	}
	risk := next(t, all)
	if !risk.IsRiskLimitAdjustment() || risk.Success || risk.Msg != "Insufficient balance. Cannot increase margin." || risk.RiskLimitLevel != 9 {
		t.Fatalf("risk limit: %+v", risk)
	}
	cross := next(t, one)
	if !cross.IsChange() || !cross.CrossMode || cross.MarginMode != MarginModeCross || cross.CurrentQty != -2 || cross.Leverage != "24.95" || cross.UnrealisedPnl != "-5.55408" || cross.ChangeReason != "positionChange" {
		t.Fatalf("cross position: %+v", cross)
	}
	settle := next(t, one)
	if settle.Symbol != "XBTUSDTM" { // the settlement payload has no symbol: it comes from the topic
		t.Fatalf("settlement symbol = %q, want XBTUSDTM", settle.Symbol)
	}
	if !settle.IsSettlement() || settle.Qty != -2 || settle.FundingFee != "-0.00309113" || settle.FundingRate != "-2.3e-05" || settle.FundingTime != 1771488000000 || settle.Timestamp != 1771488018495030863 || settle.MarkPrice != "67198.3" {
		t.Fatalf("settlement: %+v", settle)
	}
}

func TestMarginModeAndCrossLeverage(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contract/marginMode", fxMarginMode)
	h.fake.OnSubscribe("/contract/crossLeverage", fxCrossLeverage)
	s := h.private(t)
	mm, err := s.SubscribeMarginMode(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	cl, err := s.SubscribeCrossLeverage(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	m := next(t, mm)
	if m.Modes["XBTUSDTM"] != "CROSS" || len(m.Modes) != 1 || m.Subject != "user.config" || m.UserID != "633559791e1cbc0001f319bc" {
		t.Fatalf("margin mode: %+v", m)
	}
	c := next(t, cl)
	if c.Leverages["XBTUSDTM"] != "51" || len(c.Leverages) != 1 || c.Subject != "user.config" {
		t.Fatalf("cross leverage: %+v", c)
	}
}

func TestLocalValidationHappensBeforeAnythingIsSent(t *testing.T) {
	h := newHarness(t)
	pub := h.public(t)
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
		{"no symbols", func() error { _, err := pub.SubscribeTickerV2(ctx, nil); return err }, ErrNoSymbols},
		{"too many symbols", func() error { _, err := pub.SubscribeTickerV2(ctx, tooMany); return err }, ErrTooManySymbols},
		{"empty symbol", func() error { _, err := pub.SubscribeTrades(ctx, []string{""}); return err }, ErrInvalidSymbol},
		{"comma in symbol", func() error { _, err := pub.SubscribeTrades(ctx, []string{"A,B"}); return err }, ErrInvalidSymbol},
		{"underscore in symbol", func() error { _, err := pub.SubscribeKlines(ctx, Interval1Min, []string{"A_B"}); return err }, ErrInvalidSymbol},
		{"colon in symbol", func() error { _, err := pub.SubscribeSnapshot(ctx, []string{"A:B"}); return err }, ErrInvalidSymbol},
		{"bad interval", func() error { _, err := pub.SubscribeKlines(ctx, "2min", []string{"XBTUSDTM"}); return err }, ErrInvalidInterval},
		{"private on public", func() error { _, err := pub.SubscribeOrders(ctx, ""); return err }, ErrPrivateConnectionRequired},
		{"private positions on public", func() error { _, err := pub.SubscribePositions(ctx, "X"); return err }, ErrPrivateConnectionRequired},
		{"private balance on public", func() error { _, err := pub.SubscribeBalance(ctx); return err }, ErrPrivateConnectionRequired},
		{"private stop orders on public", func() error { _, err := pub.SubscribeStopOrders(ctx); return err }, ErrPrivateConnectionRequired},
		{"private margin mode on public", func() error { _, err := pub.SubscribeMarginMode(ctx); return err }, ErrPrivateConnectionRequired},
		{"private leverage on public", func() error { _, err := pub.SubscribeCrossLeverage(ctx); return err }, ErrPrivateConnectionRequired},
		{"order book bad symbol", func() error { _, err := pub.SubscribeOrderBook(ctx, "A,B"); return err }, ErrInvalidSymbol},
	}
	for _, tc := range cases {
		if err := tc.call(); !errors.Is(err, tc.want) {
			t.Errorf("%s: error = %v, want %v", tc.name, err, tc.want)
		}
	}
	if n := h.fake.Count("subscribe", ""); n != 0 {
		t.Fatalf("%d subscribe frames reached the server; validation must happen locally", n)
	}
	for _, i := range []Interval{Interval1Min, Interval3Min, Interval5Min, Interval15Min, Interval30Min, Interval1Hour, Interval2Hour, Interval4Hour, Interval8Hour, Interval12Hour, Interval1Day, Interval1Week, Interval1Month} {
		if !i.Valid() {
			t.Errorf("interval %q must be valid", i)
		}
	}
	if Interval("6hour").Valid() { // spot-only: futures has no 6hour candles
		t.Error("6hour does not exist on futures")
	}
}

func TestOrderBookWithoutASnapshotSourceIsRefused(t *testing.T) {
	wstest.CheckLeaks(t)
	fake := wstest.NewClassicFake(t)
	svc := NewService(func(context.Context) (*classicws.Token, error) { return fake.Token(), nil }, nil, nil, fastOptions()...)
	s, err := svc.DialPublic(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.SubscribeOrderBook(ctx5(t), "XBTUSDTM"); !errors.Is(err, ErrNoSnapshotSource) {
		t.Fatalf("error = %v", err)
	}
	if _, err := svc.DialPrivate(ctx5(t)); err == nil {
		t.Fatal("a service without a private token source cannot dial privately")
	}
}

func TestServerRejectionIsTyped(t *testing.T) {
	h := newHarness(t)
	h.fake.Reject("/contractMarket/tickerV2:NOSUCHSYM", 404)
	h.fake.Reject("/contractAccount/wallet", 403)
	_, err := h.public(t).SubscribeTickerV2(ctx5(t), []string{"NOSUCHSYM"})
	var se *stream.ServerError
	if !errors.Is(err, stream.ErrTopicNotFound) || !errors.As(err, &se) || se.Code != 404 {
		t.Fatalf("error = %v", err)
	}
	if _, err := h.private(t).SubscribeBalance(ctx5(t)); !errors.Is(err, stream.ErrLoginRequired) {
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
	// An authentication rejection from the REST token call is permanent too.
	svc = NewService(nil, func(context.Context) (*classicws.Token, error) {
		return nil, fmt.Errorf("%w: %w", transport.ErrUnauthorized, &transport.KucoinError{HTTPStatus: 401, Code: "400003", Message: "KC-API-KEY not exists"})
	}, nil, fastOptions()...)
	if _, err := svc.DialPrivate(ctx5(t)); !stream.IsPermanent(err) || !errors.Is(err, transport.ErrUnauthorized) {
		t.Fatalf("error = %v", err)
	}
	// A transient token failure is not permanent.
	svc = NewService(nil, func(context.Context) (*classicws.Token, error) { return nil, transport.ErrServiceUnavailable }, nil, fastOptions()...)
	if _, err := svc.DialPrivate(ctx5(t)); stream.IsPermanent(err) || !errors.Is(err, transport.ErrServiceUnavailable) {
		t.Fatalf("error = %v", err)
	}
	if _, err := NewService(nil, nil, nil).DialPublic(ctx5(t)); err == nil {
		t.Fatal("a service without a token source must refuse to dial")
	}
}

func TestSessionReconnectsWithAFreshTokenAndRestoresSubscriptions(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractMarket/tickerV2:XBTUSDTM")
	s := h.public(t)
	sub, err := s.SubscribeTickerV2(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	h.fake.Server.DropAll()
	eventually(t, func() bool {
		return s.State() == stream.StateConnected && h.fake.Server.Connections() == 2 && h.fake.Count("subscribe", "") == 2
	}, "reconnect and resubscribe")
	if h.publicN.Load() != 2 {
		t.Fatalf("public token requested %d times; a reconnect must fetch a fresh one", h.publicN.Load())
	}
	if err := h.fake.Push(fxTickerV2); err != nil {
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
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("events = %v, want %v", kinds, want)
		}
	}
	if st := s.Stats(); st.Reconnects != 1 || st.Subscriptions != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestSessionCloseEndsEverythingWithoutLeaks(t *testing.T) {
	h := newHarness(t)
	s := h.public(t)
	a, err := s.SubscribeTickerV2(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.SubscribeTrades(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	for _, closed := range []func() bool{
		func() bool { _, ok := <-a.C(); return !ok },
		func() bool { _, ok := <-b.C(); return !ok },
	} {
		done := make(chan bool, 1)
		go func() { done <- closed() }()
		select {
		case ok := <-done:
			if !ok {
				t.Fatal("a subscription channel stayed open after Close")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("subscription channel not closed")
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
}

func TestSubscriptionCloseStopsDeliveryAndUnsubscribes(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractMarket/execution:XBTUSDTM", fxTrade)
	s := h.public(t)
	sub, err := s.SubscribeTrades(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	next(t, sub)
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if h.fake.Count("unsubscribe", "/contractMarket/execution:XBTUSDTM") != 1 {
		t.Fatal("the server never saw the unsubscribe")
	}
	if _, ok := <-sub.C(); ok {
		t.Fatal("C must be closed")
	}
	// The same topic can be subscribed again.
	if _, err := s.SubscribeTrades(ctx5(t), []string{"XBTUSDTM"}); err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
}

func TestMalformedPushIsReportedAndTheStreamContinues(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractMarket/tickerV2:XBTUSDTM",
		wstest.Message("/contractMarket/tickerV2:XBTUSDTM", "tickerV2", `{"symbol":"XBTUSDTM","bestBidSize":"not a number"}`),
		wstest.Message("/contractMarket/tickerV2:XBTUSDTM", "tickerV2", `"just a string"`),
		fxTickerV2)
	s := h.public(t)
	sub, err := s.SubscribeTickerV2(ctx5(t), []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	if v := next(t, sub); v.BestBidPrice != "86454.5" {
		t.Fatalf("the valid update after two malformed ones must arrive: %+v", v)
	}
	eventually(t, func() bool { return s.Stats().DecodeErrors == 2 }, "two decode errors counted")
	var found bool
	deadline := time.After(2 * time.Second)
	for !found {
		select {
		case ev := <-s.Events():
			var de *stream.DecodeError
			if ev.Type == stream.EventDecodeError && errors.As(ev.Err, &de) && de.Channel == "/contractMarket/tickerV2:XBTUSDTM" && len(de.Raw) > 0 {
				found = true
			}
		case <-deadline:
			t.Fatal("no decode error event")
		}
	}
}

func TestMalformedChannelSpecificPayloads(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/contractMarket/limitCandle:XBTUSDTM_1min",
		wstest.Message("/contractMarket/limitCandle:XBTUSDTM_1min", "candle.stick", `{"symbol":"XBTUSDTM","candles":["1","2"],"time":1}`),
		wstest.Message("/contractMarket/limitCandle:XBTUSDTM_1min", "candle.stick", `{"symbol":"XBTUSDTM","candles":["x","2","3","4","5","6","7"],"time":1}`),
		fxKline)
	h.fake.OnSubscribe("/contractMarket/level2:XBTUSDTM",
		wstest.Message("/contractMarket/level2:XBTUSDTM", "level2", `{"sequence":1,"change":"1,2","timestamp":1}`),
		wstest.Message("/contractMarket/level2:XBTUSDTM", "level2", `{"sequence":1,"change":"1,sideways,2","timestamp":1}`),
		wstest.Message("/contractMarket/level2:XBTUSDTM", "level2", `{"sequence":1,"change":"abc,buy,2","timestamp":1}`),
		fxLevel2)
	h.fake.OnSubscribe("/contract/marginMode", wstest.Message("/contract/marginMode", "user.config", `[1,2]`), fxMarginMode)
	h.fake.OnSubscribe("/contract/crossLeverage", wstest.Message("/contract/crossLeverage", "user.config", `{"X":"nope"}`), fxCrossLeverage)
	s := h.private(t)
	ctx := ctx5(t)
	kl, err := s.SubscribeKlines(ctx, Interval1Min, []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.SubscribeOrderBookChanges(ctx, []string{"XBTUSDTM"})
	if err != nil {
		t.Fatal(err)
	}
	mm, err := s.SubscribeMarginMode(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := s.SubscribeCrossLeverage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v := next(t, kl); v.Open != "84568.3" {
		t.Fatalf("kline: %+v", v)
	}
	if v := next(t, ch); v.Sequence != 1709400450243 {
		t.Fatalf("change: %+v", v)
	}
	if v := next(t, mm); v.Modes["XBTUSDTM"] != "CROSS" {
		t.Fatalf("margin mode: %+v", v)
	}
	if v := next(t, cl); v.Leverages["XBTUSDTM"] != "51" {
		t.Fatalf("leverage: %+v", v)
	}
	eventually(t, func() bool { return s.Stats().DecodeErrors == 2+3+1+1 }, "every malformed payload counted exactly once")
}

func TestSlowConsumerCountsDroppedUpdates(t *testing.T) {
	h := newHarness(t)
	frames := make([]string, 60)
	for i := range frames {
		frames[i] = strings.Replace(fxTickerV2, `"sequence":1713516609293`, fmt.Sprintf(`"sequence":%d`, 1000+i), 1)
	}
	h.fake.OnSubscribe("/contractMarket/tickerV2:XBTUSDTM", frames...)
	s := h.public(t)
	sub, err := s.SubscribeTickerV2(ctx5(t), []string{"XBTUSDTM"}, stream.WithBuffer(3), stream.WithOverflow(stream.DropOldest))
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
		frames[i] = fxTrade
	}
	h.fake.OnSubscribe("/contractMarket/execution:XBTUSDTM", frames...)
	h.fake.OnSubscribe("/contractMarket/tickerV2:XBTUSDTM", fxTickerV2)
	s := h.public(t)
	slow, err := s.SubscribeTrades(ctx5(t), []string{"XBTUSDTM"}, stream.WithBuffer(2), stream.WithOverflow(stream.FailSubscription))
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := s.SubscribeTickerV2(ctx5(t), []string{"XBTUSDTM"})
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
