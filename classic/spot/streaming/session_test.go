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

func TestTicker(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/market/ticker:BTC-USDT", spTicker)
	sub, err := h.public(t).SubscribeTicker(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, sub)
	eq(t, "Symbol", v.Symbol, "BTC-USDT")
	eq(t, "Sequence", v.Sequence, 1545896668986)
	eq(t, "Price", v.Price, "0.08")
	eq(t, "Size", v.Size, "0.011")
	eq(t, "BestAsk", v.BestAsk, "0.08")
	eq(t, "BestAskSize", v.BestAskSize, "0.18")
	eq(t, "BestBid", v.BestBid, "0.049")
	eq(t, "BestBidSize", v.BestBidSize, "0.036")
	eq(t, "Timestamp (the example spells the key Time)", v.Timestamp, 1704873323416)
	eq(t, "Time()", v.Time().UnixMilli(), 1704873323416)
	if h.fake.Last("subscribe")["privateChannel"] != false {
		t.Fatalf("a public topic must not be subscribed as private: %v", h.fake.Last("subscribe"))
	}
}

func TestTicker_MultipleSymbolsAreRoutedAndNamed(t *testing.T) {
	h := newHarness(t)
	topic := "/market/ticker:BTC-USDT,ETH-USDT"
	// The first frame is a live capture with the lower-case "time" key, the second
	// the documentation's example with "Time": one field must take both.
	h.fake.OnSubscribe(topic, spTickerLive, spTicker)
	sub, err := h.public(t).SubscribeTicker(ctx5(t), []string{"BTC-USDT", "ETH-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	eth, btc := next(t, sub), next(t, sub)
	eq(t, "first symbol", eth.Symbol, "ETH-USDT")
	eq(t, "second symbol", btc.Symbol, "BTC-USDT")
	eq(t, "lower-case time key", eth.Timestamp, 1790984011673)
	eq(t, "capitalised Time key", btc.Timestamp, 1704873323416)
	eq(t, "ETH BestAsk", eth.BestAsk, "2667.61")
	eq(t, "ETH Sequence", eth.Sequence, 23242050174)
	eq(t, "Key", sub.Key(), topic)
}

func TestAllTickers(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/market/ticker:all", spAllTicker, spAllTickerLive)
	sub, err := h.public(t).SubscribeAllTickers(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	a := next(t, sub)
	eq(t, "Symbol (from the subject)", a.Symbol, "BTC-USDT")
	eq(t, "Sequence", a.Sequence, 14691455768)
	eq(t, "Price", a.Price, "67220")
	eq(t, "Size", a.Size, "0.00004316")
	eq(t, "BestAsk", a.BestAsk, "67218.7")
	eq(t, "BestAskSize", a.BestAskSize, "1.92318539")
	eq(t, "BestBid", a.BestBid, "67218.6")
	eq(t, "BestBidSize", a.BestBidSize, "0.01045638")
	eq(t, "Timestamp", a.Timestamp, 1729757723612)
	eq(t, "Time()", a.Time().UnixMilli(), 1729757723612)
	b := next(t, sub)
	eq(t, "second Symbol", b.Symbol, "MOG-USDT")
	eq(t, "small BestAsk", b.BestAsk, "0.0000001143")
	eq(t, "large BestAskSize", b.BestAskSize, "10171660581")
	eq(t, "Key", sub.Key(), "/market/ticker:all")
}

func TestSymbolSnapshot_KeepsEveryDigit(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/market/snapshot:BTC-USDT", spSymbolSnapshot)
	sub, err := h.public(t).SubscribeSymbolSnapshot(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, sub)
	eq(t, "Sequence", v.Sequence, 14691517895)
	eq(t, "SymbolSequence (absent in the example)", v.SymbolSequence, 0)
	eq(t, "Symbol", v.Symbol, "BTC-USDT")
	eq(t, "SymbolCode", v.SymbolCode, "BTC-USDT")
	eq(t, "BaseCurrency", v.BaseCurrency, "BTC")
	eq(t, "QuoteCurrency", v.QuoteCurrency, "USDT")
	eq(t, "Market", v.Market, "USDS")
	eq(t, "Markets", strings.Join(v.Markets, "|"), "USDS|PoW")
	eq(t, "SiteTypes", strings.Join(v.SiteTypes, "|"), "turkey|thailand|global")
	eq(t, "Board", v.Board, BoardKuCoinPlus)
	eq(t, "Mark", v.Mark, MarkDefault)
	eq(t, "Sort", v.Sort, 100)
	eq(t, "MarginTrade", v.MarginTrade, true)
	eq(t, "Trading", v.Trading, true)
	eq(t, "Open", v.Open, "66842.90000000000000000000")
	eq(t, "Close", v.Close, "67158.1")
	eq(t, "High", v.High, "67611.80000000000000000000")
	eq(t, "Low", v.Low, "65257.10000000000000000000")
	eq(t, "LastTradedPrice", v.LastTradedPrice, "67158.1")
	eq(t, "LastSize (not in the example)", v.LastSize, "")
	eq(t, "Buy", v.Buy, "67158.1")
	eq(t, "Sell", v.Sell, "67158.2")
	eq(t, "BidSize", v.BidSize, "0.81772627")
	eq(t, "AskSize", v.AskSize, "1.15955795")
	eq(t, "AveragePrice", v.AveragePrice, "66867.89967612")
	eq(t, "ChangePrice", v.ChangePrice, "315.20000000000000000000")
	eq(t, "ChangeRate", v.ChangeRate, "0.0047")
	eq(t, "Vol", v.Vol, "2227.69895852000000000000")
	eq(t, "VolValue", v.VolValue, "147972941.07857507300000000000")
	eq(t, "MakerFeeRate", v.MakerFeeRate, "0.001")
	eq(t, "TakerFeeRate", v.TakerFeeRate, "0.001")
	eq(t, "MakerCoefficient", v.MakerCoefficient, "1.000000")
	eq(t, "TakerCoefficient", v.TakerCoefficient, "1.000000")
	eq(t, "Datetime", v.Datetime, 1729758286011)
	eq(t, "Time()", v.Time().UnixMilli(), 1729758286011)

	h1 := v.MarketChange1h
	eq(t, "1h ChangePrice", h1.ChangePrice, "-102.10000000000000000000")
	eq(t, "1h ChangeRate", h1.ChangeRate, "-0.0015")
	eq(t, "1h High", h1.High, "67310.60000000000000000000")
	eq(t, "1h Low", h1.Low, "67051.80000000000000000000")
	eq(t, "1h Open", h1.Open, "67260.20000000000000000000")
	eq(t, "1h Vol", h1.Vol, "53.73698081000000000000")
	eq(t, "1h VolValue", h1.VolValue, "3609965.13819127700000000000")
	h4 := v.MarketChange4h
	eq(t, "4h ChangePrice", h4.ChangePrice, "-166.30000000000000000000")
	eq(t, "4h ChangeRate", h4.ChangeRate, "-0.0024")
	eq(t, "4h High", h4.High, "67476.60000000000000000000")
	eq(t, "4h Low", h4.Low, "67051.80000000000000000000")
	eq(t, "4h Open", h4.Open, "67324.40000000000000000000")
	eq(t, "4h Vol", h4.Vol, "173.76971188000000000000")
	eq(t, "4h VolValue", h4.VolValue, "11695949.43841656500000000000")
	h24 := v.MarketChange24h
	eq(t, "24h ChangePrice", h24.ChangePrice, "315.20000000000000000000")
	eq(t, "24h ChangeRate", h24.ChangeRate, "0.0047")
	eq(t, "24h High", h24.High, "67611.80000000000000000000")
	eq(t, "24h Low", h24.Low, "65257.10000000000000000000")
	eq(t, "24h Open", h24.Open, "66842.90000000000000000000")
	eq(t, "24h Vol", h24.Vol, "2227.69895852000000000000")
	eq(t, "24h VolValue", h24.VolValue, "147972941.07857507300000000000")
}

func TestSymbolSnapshot_LiveFrameHasUndocumentedFields(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/market/snapshot:BTC-USDT", spSymbolSnapshotLive)
	sub, err := h.public(t).SubscribeSymbolSnapshot(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, sub)
	eq(t, "Sequence", v.Sequence, 38110100812)
	eq(t, "SymbolSequence", v.SymbolSequence, 38110100812)
	eq(t, "LastSize", v.LastSize, "0.00149977")
	eq(t, "ChangeRate keeps its trailing zero", v.ChangeRate, "-0.0030")
	eq(t, "MakerCoefficient", v.MakerCoefficient, "1.0000")
	eq(t, "Markets", strings.Join(v.Markets, "|"), "USDS|Majors|PoW|Layer1")
	eq(t, "1h Vol keeps 18 digits", v.MarketChange1h.Vol, "30.4787595734952392")
	eq(t, "24h VolValue keeps 23 digits", v.MarketChange24h.VolValue, "345900144.63267467635328976")
	eq(t, "Datetime", v.Datetime, 1790984037207)
}

func TestMarketSnapshot_SymbolComesFromThePayload(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/market/snapshot:BTC", spMarketSnapshot, spMarketSnapshotLive)
	sub, err := h.public(t).SubscribeMarketSnapshot(ctx5(t), "BTC")
	if err != nil {
		t.Fatal(err)
	}
	a := next(t, sub)
	eq(t, "Symbol (the topic names the market)", a.Symbol, "CHR-BTC")
	eq(t, "Market", a.Market, "BTC")
	eq(t, "Sequence (a number here)", a.Sequence, 1729785948015)
	eq(t, "SymbolSequence", a.SymbolSequence, 0)
	eq(t, "BaseCurrency", a.BaseCurrency, "CHR")
	eq(t, "QuoteCurrency", a.QuoteCurrency, "BTC")
	eq(t, "Board", a.Board, BoardPrimary)
	eq(t, "MarginTrade", a.MarginTrade, false)
	eq(t, "AskSize", a.AskSize, "1375.1096")
	eq(t, "AveragePrice", a.AveragePrice, "0.00000262")
	eq(t, "ChangePrice", a.ChangePrice, "0.00000005300000000000")
	eq(t, "ChangeRate keeps its trailing zeros", a.ChangeRate, "0.0200")
	eq(t, "Markets", strings.Join(a.Markets, "|"), "BTC|DePIN|Layer 1")
	eq(t, "1h ChangePrice", a.MarketChange1h.ChangePrice, "-0.00000000900000000000")
	eq(t, "24h VolValue", a.MarketChange24h.VolValue, "0.01789509649520000000")
	eq(t, "4h VolValue", a.MarketChange4h.VolValue, "0.00024903875740000000")
	b := next(t, sub)
	eq(t, "second Symbol", b.Symbol, "EWT-BTC")
	eq(t, "second Sequence", b.Sequence, 1790984045980)
	eq(t, "second SymbolSequence", b.SymbolSequence, 352089952)
	eq(t, "second LastSize", b.LastSize, "16.1")
	eq(t, "Key", sub.Key(), "/market/snapshot:BTC")
}

func TestLevel1(t *testing.T) {
	h := newHarness(t)
	topic := "/spotMarket/level1:BTC-USDT,ETH-USDT"
	h.fake.OnSubscribe(topic, spLevel1NoAsks, spLevel1)
	sub, err := h.public(t).SubscribeLevel1(ctx5(t), []string{"BTC-USDT", "ETH-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	eth := next(t, sub)
	eq(t, "ETH Symbol", eth.Symbol, "ETH-USDT")
	eq(t, "ETH Ask price of an empty side", eth.Ask.Price, "")
	eq(t, "ETH Ask size of an empty side", eth.Ask.Size, "")
	eq(t, "ETH Bid price", eth.Bid.Price, "2000.5")
	eq(t, "ETH Bid size", eth.Bid.Size, "3")
	eq(t, "ETH Timestamp", eth.Timestamp, 1729816058767)
	btc := next(t, sub)
	eq(t, "BTC Symbol", btc.Symbol, "BTC-USDT")
	eq(t, "Ask price", btc.Ask.Price, "68145.8")
	eq(t, "Ask size", btc.Ask.Size, "0.51987471")
	eq(t, "Bid price", btc.Bid.Price, "68145.7")
	eq(t, "Bid size", btc.Bid.Size, "1.29267802")
	eq(t, "Timestamp", btc.Timestamp, 1729816058766)
	eq(t, "Time()", btc.Time().UnixMilli(), 1729816058766)
}

func TestDepth5AndDepth50(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/spotMarket/level2Depth5:BTC-USDT", spDepth5)
	h.fake.OnSubscribe("/spotMarket/level2Depth50:BTC-USDT", spDepth50)
	s := h.public(t)
	d5, err := s.SubscribeDepth5(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	d50, err := s.SubscribeDepth50(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, d5)
	eq(t, "Symbol", v.Symbol, "BTC-USDT")
	eq(t, "len(Asks)", len(v.Asks), 2)
	eq(t, "len(Bids)", len(v.Bids), 2)
	eq(t, "Asks[0].Price", v.Asks[0].Price, "67996.7")
	eq(t, "Asks[0].Size", v.Asks[0].Size, "1.14213262")
	eq(t, "Asks[1].Price", v.Asks[1].Price, "67996.8")
	eq(t, "Asks[1].Size", v.Asks[1].Size, "0.21748212")
	eq(t, "Bids[0].Price", v.Bids[0].Price, "67996.6")
	eq(t, "Bids[0].Size", v.Bids[0].Size, "0.37969491")
	eq(t, "Bids[1].Price", v.Bids[1].Price, "67995.3")
	eq(t, "Bids[1].Size", v.Bids[1].Size, "0.20779746")
	eq(t, "Timestamp", v.Timestamp, 1729822226746)
	eq(t, "Time()", v.Time().UnixMilli(), 1729822226746)
	w := next(t, d50)
	eq(t, "50: Symbol", w.Symbol, "BTC-USDT")
	eq(t, "50: Asks[0]", w.Asks[0].Price.String()+"/"+w.Asks[0].Size.String(), "95964.3/0.08168874")
	eq(t, "50: Asks[1]", w.Asks[1].Price.String()+"/"+w.Asks[1].Size.String(), "95967.9/0.00985094")
	eq(t, "50: Bids[0]", w.Bids[0].Price.String()+"/"+w.Bids[0].Size.String(), "95964.2/1.35483359")
	eq(t, "50: Bids[1]", w.Bids[1].Price.String()+"/"+w.Bids[1].Size.String(), "95964.1/0.01117492")
	eq(t, "50: Timestamp", w.Timestamp, 1733124805073)
}

func TestOrderBookChanges(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/market/level2:BTC-USDT", spLevel2, spLevel2Range)
	sub, err := h.public(t).SubscribeOrderBookChanges(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, sub)
	eq(t, "Symbol", v.Symbol, "BTC-USDT")
	eq(t, "SequenceStart", v.SequenceStart, 14701689783)
	eq(t, "SequenceEnd", v.SequenceEnd, 14701689783)
	eq(t, "len(Asks)", len(v.Asks), 1)
	eq(t, "len(Bids)", len(v.Bids), 0)
	eq(t, "Asks[0].Price", v.Asks[0].Price, "67993.3")
	eq(t, "Asks[0].Size", v.Asks[0].Size, "1.21427407")
	eq(t, "Asks[0].Sequence", v.Asks[0].Sequence, 14701689783)
	eq(t, "Timestamp", v.Timestamp, 1729816425625)
	eq(t, "Time()", v.Time().UnixMilli(), 1729816425625)

	r := next(t, sub)
	eq(t, "range Start", r.SequenceStart, 38110279535)
	eq(t, "range End", r.SequenceEnd, 38110279538)
	eq(t, "range len(Asks)", len(r.Asks), 1)
	eq(t, "range len(Bids)", len(r.Bids), 2)
	eq(t, "removal Price", r.Asks[0].Price, "84271.2")
	eq(t, "removal Size is reported as received", r.Asks[0].Size, "0")
	eq(t, "removal Sequence", r.Asks[0].Sequence, 38110279538)
	eq(t, "Bids[0].Price", r.Bids[0].Price, "1")
	eq(t, "Bids[0].Size", r.Bids[0].Size, "13329.23415421")
	eq(t, "Bids[0].Sequence", r.Bids[0].Sequence, 38110279535)
	eq(t, "Bids[1].Price", r.Bids[1].Price, "82003.8")
	eq(t, "Bids[1].Size", r.Bids[1].Size, "0.00073166")
	eq(t, "Bids[1].Sequence", r.Bids[1].Sequence, 38110279536)
}

func TestKlines_FieldOrderIsOpenCloseHighLow(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/market/candles:BTC-USDT_1hour", spKline)
	// Two symbols on one 5-minute subscription: every symbol gets the interval
	// suffix, and each push is routed by its own topic.
	btc5 := strings.ReplaceAll(spKline, "1hour", "5min")
	eth5 := strings.ReplaceAll(btc5, "BTC-USDT", "ETH-USDT")
	topic5 := "/market/candles:BTC-USDT_5min,ETH-USDT_5min"
	h.fake.OnSubscribe(topic5, eth5, btc5)
	s := h.public(t)
	hour, err := s.SubscribeKlines(ctx5(t), Interval1Hour, []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	five, err := s.SubscribeKlines(ctx5(t), Interval5Min, []string{"BTC-USDT", "ETH-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, hour)
	eq(t, "Symbol", v.Symbol, "BTC-USDT")
	eq(t, "Interval", v.Interval, Interval1Hour)
	eq(t, "StartTime", v.StartTime, 1729839600)
	eq(t, "Open", v.Open, "67644.9")
	eq(t, "Close", v.Close, "67437.6")
	eq(t, "High", v.High, "67724.8")
	eq(t, "Low", v.Low, "67243.8")
	eq(t, "Volume", v.Volume, "44.88321441")
	eq(t, "Turnover", v.Turnover, "3027558.991928447")
	eq(t, "Timestamp", v.Timestamp, 1729842192785164840)
	eq(t, "len(Candles)", len(v.Candles), 7)
	eq(t, "Candles[0]", v.Candles[0], "1729839600")
	eq(t, "Start()", v.Start().Unix(), 1729839600)
	eq(t, "Time()", v.Time().UnixNano(), 1729842192785164840)

	a, b := next(t, five), next(t, five)
	eq(t, "first symbol", a.Symbol, "ETH-USDT")
	eq(t, "second symbol", b.Symbol, "BTC-USDT")
	eq(t, "first interval", a.Interval, Interval5Min)
	eq(t, "second interval", b.Interval, Interval5Min)
	eq(t, "first Open", a.Open, "67644.9")
	eq(t, "Key", five.Key(), topic5)
}

func TestTrades(t *testing.T) {
	h := newHarness(t)
	topic := "/market/match:BTC-USDT,ETH-USDT"
	ethTrade := strings.ReplaceAll(spTrade, "BTC-USDT", "ETH-USDT")
	h.fake.OnSubscribe(topic, spTrade, ethTrade)
	sub, err := h.public(t).SubscribeTrades(ctx5(t), []string{"BTC-USDT", "ETH-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	v := next(t, sub)
	eq(t, "Symbol", v.Symbol, "BTC-USDT")
	eq(t, "Sequence", v.Sequence, 11067996711960577)
	eq(t, "Type", v.Type, "match")
	eq(t, "Side", v.Side, SideBuy)
	eq(t, "Price", v.Price, "67523")
	eq(t, "Size", v.Size, "0.003")
	eq(t, "TradeID", v.TradeID, "11067996711960577")
	eq(t, "TakerOrderID", v.TakerOrderID, "671b50161777ff00074c168d")
	eq(t, "MakerOrderID", v.MakerOrderID, "671b5007389355000701b1d3")
	eq(t, "Timestamp (a numeric string of nanoseconds)", v.Timestamp, 1729843222921000000)
	eq(t, "Time()", v.Time().UnixNano(), 1729843222921000000)
	eq(t, "second Symbol", next(t, sub).Symbol, "ETH-USDT")
}

func TestCallAuctionChannels(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/callauction/level2Depth50:BTC-USDT", spCallAuctionDepth50)
	dataTopic := "/callauction/callauctionData:BTC-USDT,ETH-USDT"
	h.fake.OnSubscribe(dataTopic, spCallAuctionData, spCallAuctionDataAbbreviated)
	s := h.public(t)
	depth, err := s.SubscribeCallAuctionDepth50(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := s.SubscribeCallAuctionData(ctx5(t), []string{"BTC-USDT", "ETH-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	d := next(t, depth)
	eq(t, "depth Symbol", d.Symbol, "BTC-USDT")
	eq(t, "depth Asks[0]", d.Asks[0].Price.String()+"/"+d.Asks[0].Size.String(), "95964.3/0.08168874")
	eq(t, "depth Bids[1]", d.Bids[1].Price.String()+"/"+d.Bids[1].Size.String(), "95964.1/0.01117492")
	eq(t, "depth Timestamp", d.Timestamp, 1733124805073)

	full := next(t, data)
	eq(t, "Symbol", full.Symbol, "BTC-USDT")
	eq(t, "EstimatedPrice", full.EstimatedPrice, "0.17")
	eq(t, "EstimatedSize", full.EstimatedSize, "0.03715004")
	eq(t, "SellOrderRangeLowPrice", full.SellOrderRangeLowPrice, "1.788")
	eq(t, "SellOrderRangeHighPrice", full.SellOrderRangeHighPrice, "2.788")
	eq(t, "BuyOrderRangeLowPrice", full.BuyOrderRangeLowPrice, "1.788")
	eq(t, "BuyOrderRangeHighPrice", full.BuyOrderRangeHighPrice, "2.788")
	eq(t, "Timestamp", full.Timestamp, 1550653727731)
	eq(t, "Time()", full.Time().UnixMilli(), 1550653727731)

	short := next(t, data)
	eq(t, "abbreviated Symbol", short.Symbol, "ETH-USDT")
	eq(t, "abbreviated EstimatedPrice", short.EstimatedPrice, "2001.5")
	eq(t, "abbreviated EstimatedSize", short.EstimatedSize, "3.5")
	eq(t, "abbreviated SellOrderRangeLowPrice", short.SellOrderRangeLowPrice, "1990")
	eq(t, "abbreviated SellOrderRangeHighPrice", short.SellOrderRangeHighPrice, "2010")
	eq(t, "abbreviated BuyOrderRangeLowPrice", short.BuyOrderRangeLowPrice, "1980")
	eq(t, "abbreviated BuyOrderRangeHighPrice", short.BuyOrderRangeHighPrice, "2020")
	eq(t, "abbreviated Timestamp", short.Timestamp, 1550653727732)
}

func TestOrdersV2_AllEventKinds(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/spotMarket/tradeOrdersV2", spOrderReceived, spOrderOpen, spOrderUpdate, spOrderMatch, spOrderFilled, spOrderCanceled)
	sub, err := h.private(t).SubscribeOrdersV2(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if h.fake.Last("subscribe")["privateChannel"] != true {
		t.Fatal("private topics must be subscribed with privateChannel=true")
	}

	received := next(t, sub)
	eq(t, "received Subject", received.Subject, "orderChange")
	eq(t, "received UserID", received.UserID, "633559791e1cbc0001f319bc")
	eq(t, "received ChannelType", received.ChannelType, "private")
	eq(t, "received Type", received.Type, OrderEventReceived)
	eq(t, "received Status", received.Status, OrderStatusNew)
	eq(t, "received Symbol", received.Symbol, "BTC-USDT")
	eq(t, "received Side", received.Side, SideBuy)
	eq(t, "received OrderType", received.OrderType, OrderTypeMarket)
	eq(t, "received ClientOid", received.ClientOid, "5c52e11203aa677f33e493fc")
	eq(t, "received OrderID", received.OrderID, "6720da3fa30a360007f5f832")
	eq(t, "received OriginSize", received.OriginSize, "0.00001")
	eq(t, "received Size (absent)", received.Size, "")
	eq(t, "received OrderTime", received.OrderTime, 1730206271588)
	eq(t, "received Timestamp", received.Timestamp, 1730206271616000000)
	eq(t, "received GatewayTime()", received.GatewayTime().UnixMilli(), 1730206271588)
	eq(t, "received Time()", received.Time().UnixNano(), 1730206271616000000)

	open := next(t, sub)
	eq(t, "open Type", open.Type, OrderEventOpen)
	eq(t, "open Status", open.Status, OrderStatusOpen)
	eq(t, "open OrderType", open.OrderType, OrderTypeLimit)
	eq(t, "open Price", open.Price, "50000")
	eq(t, "open OriginSize", open.OriginSize, "0.00001")
	eq(t, "open Size", open.Size, "0.00001")
	eq(t, "open FilledSize", open.FilledSize, "0")
	eq(t, "open CanceledSize", open.CanceledSize, "0")
	eq(t, "open RemainSize", open.RemainSize, "0.00001")
	eq(t, "open OrderID", open.OrderID, "6720ecd9ec71f4000747731a")
	eq(t, "open ClientOid", open.ClientOid, "5c52e11203aa677f33e493fb")
	eq(t, "open OrderTime", open.OrderTime, 1730211033305)
	eq(t, "open Timestamp", open.Timestamp, 1730211033335000000)

	update := next(t, sub)
	eq(t, "update Type", update.Type, OrderEventUpdate)
	eq(t, "update Status", update.Status, OrderStatusOpen)
	eq(t, "update OldSize", update.OldSize, "0.00002")
	eq(t, "update OriginSize", update.OriginSize, "0.00002")
	eq(t, "update Size", update.Size, "0.00001")
	eq(t, "update CanceledSize", update.CanceledSize, "0.00001")
	eq(t, "update RemainSize", update.RemainSize, "0.00001")
	eq(t, "update OrderID", update.OrderID, "6720df7640e6fe0007b57696")
	eq(t, "update Timestamp", update.Timestamp, 1730207616617000000)

	match := next(t, sub)
	eq(t, "match Type", match.Type, OrderEventMatch)
	eq(t, "match Status", match.Status, OrderStatusMatch)
	eq(t, "match OrderType", match.OrderType, OrderTypeMarket)
	eq(t, "match FeeType", match.FeeType, FeeTypeTaker)
	eq(t, "match Liquidity", match.Liquidity, LiquidityTaker)
	eq(t, "match MatchPrice", match.MatchPrice, "71171.9")
	eq(t, "match MatchSize", match.MatchSize, "0.00001")
	eq(t, "match TradeID", match.TradeID, "11116472408358913")
	eq(t, "match FilledSize", match.FilledSize, "0.00001")
	eq(t, "match RemainSize", match.RemainSize, "0")

	filled := next(t, sub)
	eq(t, "filled Type", filled.Type, OrderEventFilled)
	eq(t, "filled Status", filled.Status, OrderStatusDone)
	eq(t, "filled RemainFunds", filled.RemainFunds, "0")
	eq(t, "filled FilledSize", filled.FilledSize, "0.00001")

	canceled := next(t, sub)
	eq(t, "canceled Type", canceled.Type, OrderEventCanceled)
	eq(t, "canceled Status", canceled.Status, OrderStatusDone)
	eq(t, "canceled CanceledSize", canceled.CanceledSize, "0.00002")
	eq(t, "canceled Price", canceled.Price, "50000")
	eq(t, "canceled Size", canceled.Size, "0.00001")
	eq(t, "canceled OriginSize", canceled.OriginSize, "0.00002")
	eq(t, "canceled RemainFunds", canceled.RemainFunds, "0")
}

func TestOrdersV1_SameStructOnItsOwnTopic(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/spotMarket/tradeOrders", spOrderV1Open, spOrderV1Update, spOrderV1Match, spOrderV1Filled, spOrderV1Canceled)
	h.fake.OnSubscribe("/spotMarket/tradeOrdersV2", spOrderReceived)
	s := h.private(t)
	v1, err := s.SubscribeOrdersV1(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	v2, err := s.SubscribeOrdersV2(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "V1 Key", v1.Key(), "/spotMarket/tradeOrders")
	eq(t, "V2 Key", v2.Key(), "/spotMarket/tradeOrdersV2")
	want := []struct{ typ, status string }{
		{OrderEventOpen, OrderStatusOpen}, {OrderEventUpdate, OrderStatusOpen}, {OrderEventMatch, OrderStatusMatch},
		{OrderEventFilled, OrderStatusDone}, {OrderEventCanceled, OrderStatusDone},
	}
	for i, w := range want {
		ev := next(t, v1)
		if ev.Type != w.typ || ev.Status != w.status || ev.Subject != "orderChange" || ev.UserID != "633559791e1cbc0001f319bc" {
			t.Fatalf("V1 event %d: %+v, want type %s status %s", i, ev, w.typ, w.status)
		}
	}
	// V2 is the only one that announces an order that entered the matching system.
	if ev := next(t, v2); ev.Type != OrderEventReceived || ev.Status != OrderStatusNew {
		t.Fatalf("V2 event: %+v", ev)
	}
	select {
	case ev := <-v1.C():
		t.Fatalf("V1 must not carry the V2 event, got %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestBalance(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/account/balance", spBalance, spBalanceIsolated)
	sub, err := h.private(t).SubscribeBalance(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if h.fake.Last("subscribe")["privateChannel"] != true {
		t.Fatal("private topics must be subscribed with privateChannel=true")
	}
	v := next(t, sub)
	eq(t, "Subject", v.Subject, "account.balance")
	eq(t, "ID", v.ID, "354689988084000")
	eq(t, "UserID", v.UserID, "633559791e1cbc0001f319bc")
	eq(t, "ChannelType", v.ChannelType, "private")
	eq(t, "AccountID", v.AccountID, "548674591753")
	eq(t, "Currency", v.Currency, "USDT")
	eq(t, "Total", v.Total, "21.133773386762")
	eq(t, "Available", v.Available, "20.132773386762")
	eq(t, "Hold", v.Hold, "1.001")
	eq(t, "AvailableChange", v.AvailableChange, "-0.5005")
	eq(t, "HoldChange", v.HoldChange, "0.5005")
	eq(t, "RelationContext.Symbol", v.RelationContext.Symbol, "BTC-USDT")
	eq(t, "RelationContext.OrderID", v.RelationContext.OrderID, "6721d0632db25b0007071fdc")
	eq(t, "RelationContext.TradeID", v.RelationContext.TradeID, "11116472408358913")
	eq(t, "RelationEvent", v.RelationEvent, "trade.hold")
	eq(t, "RelationEventID", v.RelationEventID, "354689988084000")
	eq(t, "Timestamp (a numeric string)", v.Timestamp, 1730269283892)
	eq(t, "Time()", v.Time().UnixMilli(), 1730269283892)

	iso := next(t, sub)
	eq(t, "isolated Currency", iso.Currency, "BTC")
	eq(t, "isolated RelationEvent", iso.RelationEvent, "isolated_BTC-USDT.setted")
	eq(t, "isolated RelationContext", iso.RelationContext, RelationContext{})
	eq(t, "isolated Timestamp (a number)", iso.Timestamp, 1730269283893)
	eq(t, "isolated AvailableChange", iso.AvailableChange, "0.1")
}

func TestStopOrders(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/spotMarket/advancedOrders", spStopOrder, spStopOrderTriggered, spStopOrderCanceled)
	sub, err := h.private(t).SubscribeStopOrders(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	if h.fake.Last("subscribe")["privateChannel"] != true {
		t.Fatal("private topics must be subscribed with privateChannel=true")
	}
	v := next(t, sub)
	eq(t, "Subject", v.Subject, "stopOrder")
	eq(t, "UserID", v.UserID, "633559791e1cbc0001f319bc")
	eq(t, "ChannelType", v.ChannelType, "private")
	eq(t, "OrderID", v.OrderID, "vs93gpupfa48anof003u85mb")
	eq(t, "OrderPrice", v.OrderPrice, "70000")
	eq(t, "OrderType", v.OrderType, "stop")
	eq(t, "Side", v.Side, SideBuy)
	eq(t, "Size", v.Size, "0.00007142")
	eq(t, "Stop", v.Stop, "loss")
	eq(t, "StopPrice", v.StopPrice, "71000")
	eq(t, "Symbol", v.Symbol, "BTC-USDT")
	eq(t, "TradeType", v.TradeType, TradeTypeSpot)
	eq(t, "Type", v.Type, StopEventOpen)
	eq(t, "CreatedAt", v.CreatedAt, 1742305928064)
	eq(t, "Timestamp", v.Timestamp, 1742305928091268493)
	eq(t, "IsTriggered", v.IsTriggered(), false)
	eq(t, "CreatedTime()", v.CreatedTime().UnixMilli(), 1742305928064)
	eq(t, "Time()", v.Time().UnixNano(), 1742305928091268493)

	trig := next(t, sub)
	eq(t, "triggered Type", trig.Type, StopEventTriggered)
	eq(t, "triggered IsTriggered", trig.IsTriggered(), true)
	eq(t, "triggered TradeType", trig.TradeType, TradeTypeMargin)
	eq(t, "triggered Stop", trig.Stop, "entry")
	eq(t, "triggered Side", trig.Side, SideSell)
	eq(t, "triggered Size", trig.Size, "1.5")
	can := next(t, sub)
	eq(t, "cancelled Type", can.Type, StopEventCancel)
	eq(t, "cancelled TradeType", can.TradeType, TradeTypeIsolatedMargin)
	eq(t, "cancelled IsTriggered", can.IsTriggered(), false)

	if !(StopOrderUpdate{Type: "triggered"}).IsTriggered() || (StopOrderUpdate{Type: "open"}).IsTriggered() {
		t.Fatal("IsTriggered must ignore the case of the event type and reject other events")
	}
}

func TestPrivateSessionAlsoServesPublicChannels(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/market/match:BTC-USDT", spTrade)
	h.fake.OnSubscribe("/spotMarket/tradeOrdersV2", spOrderOpen)
	s := h.private(t)
	trades, err := s.SubscribeTrades(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	orders, err := s.SubscribeOrdersV2(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "trade Symbol", next(t, trades).Symbol, "BTC-USDT")
	eq(t, "order Type", next(t, orders).Type, OrderEventOpen)
	if h.privateN.Load() != 1 || h.publicN.Load() != 0 {
		t.Fatalf("a private session must use the private token only: private=%d public=%d", h.privateN.Load(), h.publicN.Load())
	}
}

func TestLocalValidationHappensBeforeAnythingIsSent(t *testing.T) {
	h := newHarness(t)
	pub := h.public(t)
	ctx := ctx5(t)
	tooMany := make([]string, MaxSymbolsPerSubscription+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("SYM%d-USDT", i)
	}
	cases := []struct {
		name string
		call func() error
		want error
	}{
		{"no symbols", func() error { _, err := pub.SubscribeTicker(ctx, nil); return err }, ErrNoSymbols},
		{"too many symbols", func() error { _, err := pub.SubscribeTicker(ctx, tooMany); return err }, ErrTooManySymbols},
		{"empty symbol", func() error { _, err := pub.SubscribeTrades(ctx, []string{""}); return err }, ErrInvalidSymbol},
		{"comma in symbol", func() error { _, err := pub.SubscribeTrades(ctx, []string{"A-B,C-D"}); return err }, ErrInvalidSymbol},
		{"underscore in symbol", func() error { _, err := pub.SubscribeKlines(ctx, Interval1Min, []string{"A_B"}); return err }, ErrInvalidSymbol},
		{"colon in symbol", func() error { _, err := pub.SubscribeSymbolSnapshot(ctx, []string{"A:B"}); return err }, ErrInvalidSymbol},
		{"slash in symbol", func() error { _, err := pub.SubscribeLevel1(ctx, []string{"BTC/USDT"}); return err }, ErrInvalidSymbol},
		{"space in symbol", func() error { _, err := pub.SubscribeDepth5(ctx, []string{"BTC USDT"}); return err }, ErrInvalidSymbol},
		{"tab in symbol", func() error { _, err := pub.SubscribeDepth50(ctx, []string{"BTC\tUSDT"}); return err }, ErrInvalidSymbol},
		{"one bad symbol among good ones", func() error { _, err := pub.SubscribeTrades(ctx, []string{"BTC-USDT", "ETH USDT"}); return err }, ErrInvalidSymbol},
		{"all-tickers name as a symbol", func() error { _, err := pub.SubscribeTicker(ctx, []string{"BTC-USDT", "all"}); return err }, ErrInvalidSymbol},
		{"empty market", func() error { _, err := pub.SubscribeMarketSnapshot(ctx, ""); return err }, ErrInvalidSymbol},
		{"market with a comma", func() error { _, err := pub.SubscribeMarketSnapshot(ctx, "BTC,USDS"); return err }, ErrInvalidSymbol},
		{"order changes without symbols", func() error { _, err := pub.SubscribeOrderBookChanges(ctx, []string{}); return err }, ErrNoSymbols},
		{"call auction depth without symbols", func() error { _, err := pub.SubscribeCallAuctionDepth50(ctx, nil); return err }, ErrNoSymbols},
		{"call auction data bad symbol", func() error { _, err := pub.SubscribeCallAuctionData(ctx, []string{"A,B"}); return err }, ErrInvalidSymbol},
		{"klines without symbols", func() error { _, err := pub.SubscribeKlines(ctx, Interval1Min, nil); return err }, ErrNoSymbols},
		{"1month is not in the documented set", func() error { _, err := pub.SubscribeKlines(ctx, "1month", []string{"BTC-USDT"}); return err }, ErrInvalidInterval},
		{"unknown interval", func() error { _, err := pub.SubscribeKlines(ctx, "2min", []string{"BTC-USDT"}); return err }, ErrInvalidInterval},
		{"empty interval", func() error { _, err := pub.SubscribeKlines(ctx, "", []string{"BTC-USDT"}); return err }, ErrInvalidInterval},
		{"interval is checked before symbols", func() error { _, err := pub.SubscribeKlines(ctx, "bogus", nil); return err }, ErrInvalidInterval},
		{"orders V2 on a public session", func() error { _, err := pub.SubscribeOrdersV2(ctx); return err }, ErrPrivateConnectionRequired},
		{"orders V1 on a public session", func() error { _, err := pub.SubscribeOrdersV1(ctx); return err }, ErrPrivateConnectionRequired},
		{"balance on a public session", func() error { _, err := pub.SubscribeBalance(ctx); return err }, ErrPrivateConnectionRequired},
		{"stop orders on a public session", func() error { _, err := pub.SubscribeStopOrders(ctx); return err }, ErrPrivateConnectionRequired},
		{"order book with a bad symbol", func() error { _, err := pub.SubscribeOrderBook(ctx, "A,B"); return err }, ErrInvalidSymbol},
		{"order book with no symbol", func() error { _, err := pub.SubscribeOrderBook(ctx, ""); return err }, ErrInvalidSymbol},
	}
	for _, tc := range cases {
		if err := tc.call(); !errors.Is(err, tc.want) {
			t.Errorf("%s: error = %v, want %v", tc.name, err, tc.want)
		}
	}
	if n := h.fake.Count("subscribe", ""); n != 0 {
		t.Fatalf("%d subscribe frames reached the server; validation must happen locally", n)
	}
	// Names that look unusual but are valid reach the server: a market is not a pair.
	h.fake.OnSubscribe("/market/snapshot:BTC")
	if _, err := pub.SubscribeMarketSnapshot(ctx, "BTC"); err != nil {
		t.Fatalf("market name BTC: %v", err)
	}
	if n := h.fake.Count("subscribe", ""); n != 1 {
		t.Fatalf("subscribe frames = %d, want 1", n)
	}
}

func TestIntervalValidity(t *testing.T) {
	valid := []Interval{Interval1Min, Interval3Min, Interval5Min, Interval15Min, Interval30Min, Interval1Hour, Interval2Hour,
		Interval4Hour, Interval6Hour, Interval8Hour, Interval12Hour, Interval1Day, Interval1Week}
	for _, i := range valid {
		if !i.Valid() {
			t.Errorf("interval %q must be valid", i)
		}
	}
	for _, i := range []Interval{"1month", "2min", "10min", "1hr", "1Hour", "", " 1min"} {
		if i.Valid() {
			t.Errorf("interval %q must not be valid on spot", i)
		}
	}
}

func TestSubscribingASymbolTwiceIsRejected(t *testing.T) {
	h := newHarness(t)
	s := h.public(t)
	if _, err := s.SubscribeTicker(ctx5(t), []string{"BTC-USDT"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubscribeTicker(ctx5(t), []string{"ETH-USDT", "BTC-USDT"}); !errors.Is(err, stream.ErrAlreadySubscribed) {
		t.Fatalf("error = %v, want %v", err, stream.ErrAlreadySubscribed)
	}
	// The same symbol on another channel is a different topic.
	if _, err := s.SubscribeTrades(ctx5(t), []string{"BTC-USDT"}); err != nil {
		t.Fatalf("another channel: %v", err)
	}
	if _, err := s.SubscribeAllTickers(ctx5(t)); err != nil {
		t.Fatalf("all tickers next to a symbol ticker: %v", err)
	}
}

// The connection hands a push to every occurrence of its topic in a subscription,
// so a symbol that is listed twice must be subscribed once or each of its updates
// would arrive twice.
func TestASymbolListedTwiceIsSubscribedOnce(t *testing.T) {
	h := newHarness(t)
	topic := "/market/ticker:BTC-USDT,ETH-USDT"
	h.fake.OnSubscribe(topic, spTicker)
	s := h.public(t)
	sub, err := s.SubscribeTicker(ctx5(t), []string{"BTC-USDT", "ETH-USDT", "BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "Key names every symbol once", sub.Key(), topic)
	eq(t, "Symbol", next(t, sub).Symbol, "BTC-USDT")
	select {
	case v := <-sub.C():
		t.Fatalf("one push arrived twice: %+v", v)
	case <-time.After(100 * time.Millisecond):
	}
	// The interval goes onto every distinct symbol once.
	kl, err := s.SubscribeKlines(ctx5(t), Interval1Hour, []string{"SOL-USDT", "SOL-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "kline Key", kl.Key(), "/market/candles:SOL-USDT_1hour")
	// A repeat does not count against the limit of a hundred symbols.
	hundred := make([]string, 0, MaxSymbolsPerSubscription+1)
	for i := 0; i < MaxSymbolsPerSubscription; i++ {
		hundred = append(hundred, fmt.Sprintf("SYM%d-USDT", i))
	}
	hundred = append(hundred, "SYM0-USDT")
	if _, err := s.SubscribeTrades(ctx5(t), hundred); err != nil {
		t.Fatalf("a hundred distinct symbols and a repeat: %v", err)
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
	if _, err := s.SubscribeOrderBook(ctx5(t), "BTC-USDT"); !errors.Is(err, ErrNoSnapshotSource) {
		t.Fatalf("error = %v", err)
	}
	if _, err := svc.DialPrivate(ctx5(t)); err == nil {
		t.Fatal("a service without a private token source cannot dial privately")
	}
	if fake.Count("subscribe", "") != 0 {
		t.Fatal("nothing may be sent without a snapshot source")
	}
}

func TestServerRejectionIsTyped(t *testing.T) {
	h := newHarness(t)
	h.fake.Reject("/market/level2:NOSUCH-USDT", 404)
	h.fake.Reject("/account/balance", 403)
	h.fake.Reject("/market/ticker:BAD-USDT", 400)
	_, err := h.public(t).SubscribeOrderBookChanges(ctx5(t), []string{"NOSUCH-USDT"})
	var se *stream.ServerError
	if !errors.Is(err, stream.ErrTopicNotFound) || !errors.As(err, &se) || se.Code != 404 {
		t.Fatalf("error = %v", err)
	}
	if _, err := h.public(t).SubscribeTicker(ctx5(t), []string{"BAD-USDT"}); !errors.Is(err, stream.ErrTopicInvalid) {
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
	h.fake.OnSubscribe("/market/ticker:BTC-USDT")
	s := h.public(t)
	sub, err := s.SubscribeTicker(ctx5(t), []string{"BTC-USDT"})
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
	if err := h.fake.Push(spTicker); err != nil {
		t.Fatal(err)
	}
	if v := next(t, sub); v.Symbol != "BTC-USDT" {
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

func TestPrivateSessionReconnectsWithAFreshPrivateTokenAndStaysPrivate(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/spotMarket/tradeOrdersV2")
	s := h.private(t)
	sub, err := s.SubscribeOrdersV2(ctx5(t))
	if err != nil {
		t.Fatal(err)
	}
	h.fake.Server.DropAll()
	eventually(t, func() bool {
		return s.State() == stream.StateConnected && h.fake.Server.Connections() == 2 && h.fake.Count("subscribe", "") == 2
	}, "reconnect and resubscribe")
	if h.privateN.Load() != 2 || h.publicN.Load() != 0 {
		t.Fatalf("private token requested %d times, public %d; a reconnect must fetch a fresh private token", h.privateN.Load(), h.publicN.Load())
	}
	if h.fake.Last("subscribe")["privateChannel"] != true {
		t.Fatal("the restored subscription must stay a private channel")
	}
	if err := h.fake.Push(spOrderOpen); err != nil {
		t.Fatal(err)
	}
	if v := next(t, sub); v.Type != OrderEventOpen {
		t.Fatalf("order after reconnect: %+v", v)
	}
}

func TestSessionCloseEndsEverythingWithoutLeaks(t *testing.T) {
	h := newHarness(t)
	s := h.public(t)
	a, err := s.SubscribeTicker(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.SubscribeTrades(ctx5(t), []string{"BTC-USDT"})
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
	h.fake.OnSubscribe("/market/match:BTC-USDT", spTrade)
	s := h.public(t)
	sub, err := s.SubscribeTrades(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	next(t, sub)
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	if h.fake.Count("unsubscribe", "/market/match:BTC-USDT") != 1 {
		t.Fatal("the server never saw the unsubscribe")
	}
	if _, ok := <-sub.C(); ok {
		t.Fatal("C must be closed")
	}
	// The same topic can be subscribed again.
	if _, err := s.SubscribeTrades(ctx5(t), []string{"BTC-USDT"}); err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
}

func TestMalformedPushIsReportedAndTheStreamContinues(t *testing.T) {
	h := newHarness(t)
	h.fake.OnSubscribe("/market/ticker:BTC-USDT",
		wstest.Message("/market/ticker:BTC-USDT", "trade.ticker", `{"sequence":"not a number","price":"1"}`),
		wstest.Message("/market/ticker:BTC-USDT", "trade.ticker", `"just a string"`),
		spTicker)
	s := h.public(t)
	sub, err := s.SubscribeTicker(ctx5(t), []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	if v := next(t, sub); v.BestBid != "0.049" {
		t.Fatalf("the valid update after two malformed ones must arrive: %+v", v)
	}
	eventually(t, func() bool { return s.Stats().DecodeErrors == 2 }, "two decode errors counted")
	var found bool
	deadline := time.After(2 * time.Second)
	for !found {
		select {
		case ev := <-s.Events():
			var de *stream.DecodeError
			if ev.Type == stream.EventDecodeError && errors.As(ev.Err, &de) && de.Channel == "/market/ticker:BTC-USDT" && len(de.Raw) > 0 {
				found = true
			}
		case <-deadline:
			t.Fatal("no decode error event")
		}
	}
}

func TestMalformedChannelSpecificPayloads(t *testing.T) {
	h := newHarness(t)
	candle := func(data string) string {
		return wstest.Message("/market/candles:BTC-USDT_1hour", "trade.candles.update", data)
	}
	h.fake.OnSubscribe("/market/candles:BTC-USDT_1hour",
		candle(`{"symbol":"BTC-USDT","candles":["1","2"],"time":1}`),
		candle(`{"symbol":"BTC-USDT","candles":["x","2","3","4","5","6","7"],"time":1}`),
		spKline)
	l2 := func(data string) string { return wstest.Message("/market/level2:BTC-USDT", "trade.l2update", data) }
	h.fake.OnSubscribe("/market/level2:BTC-USDT",
		l2(`{"changes":{"asks":[["1"]],"bids":[]},"sequenceStart":5,"sequenceEnd":5,"symbol":"BTC-USDT","time":1}`),
		l2(`{"changes":{"asks":[["abc","1","5"]],"bids":[]},"sequenceStart":5,"sequenceEnd":5,"symbol":"BTC-USDT","time":1}`),
		l2(`{"changes":{"asks":[],"bids":[]},"sequenceStart":6,"sequenceEnd":5,"symbol":"BTC-USDT","time":1}`),
		l2(`{"changes":{"asks":[],"bids":[]},"symbol":"BTC-USDT","time":1}`),
		spLevel2)
	level1 := func(data string) string { return wstest.Message("/spotMarket/level1:BTC-USDT", "level1", data) }
	h.fake.OnSubscribe("/spotMarket/level1:BTC-USDT",
		level1(`{"asks":"nope","bids":[],"timestamp":1}`),
		level1(`{"asks":["1"],"bids":[],"timestamp":1}`),
		spLevel1)
	h.fake.OnSubscribe("/market/snapshot:BTC-USDT",
		wstest.Message("/market/snapshot:BTC-USDT", "trade.snapshot", `{"sequence":"1"}`),
		spSymbolSnapshot)
	h.fake.OnSubscribe("/callauction/callauctionData:BTC-USDT",
		wstest.Message("/callauction/callauctionData:BTC-USDT", "callauction.callauctionData", `[1]`),
		spCallAuctionData)
	s := h.public(t)
	ctx := ctx5(t)
	kl, err := s.SubscribeKlines(ctx, Interval1Hour, []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.SubscribeOrderBookChanges(ctx, []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	l1, err := s.SubscribeLevel1(ctx, []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := s.SubscribeSymbolSnapshot(ctx, []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	ca, err := s.SubscribeCallAuctionData(ctx, []string{"BTC-USDT"})
	if err != nil {
		t.Fatal(err)
	}
	if v := next(t, kl); v.Open != "67644.9" {
		t.Fatalf("kline: %+v", v)
	}
	if v := next(t, ch); v.SequenceEnd != 14701689783 {
		t.Fatalf("change: %+v", v)
	}
	if v := next(t, l1); v.Bid.Price != "68145.7" {
		t.Fatalf("level1: %+v", v)
	}
	if v := next(t, snap); v.Symbol != "BTC-USDT" {
		t.Fatalf("snapshot: %+v", v)
	}
	if v := next(t, ca); v.EstimatedPrice != "0.17" {
		t.Fatalf("call auction: %+v", v)
	}
	eventually(t, func() bool { return s.Stats().DecodeErrors == 2+4+2+1+1 }, "every malformed payload counted exactly once")
}

func TestSlowConsumerCountsDroppedUpdates(t *testing.T) {
	h := newHarness(t)
	frames := make([]string, 60)
	for i := range frames {
		frames[i] = strings.Replace(spTicker, `"sequence":"1545896668986"`, fmt.Sprintf(`"sequence":"%d"`, 1000+i), 1)
	}
	h.fake.OnSubscribe("/market/ticker:BTC-USDT", frames...)
	s := h.public(t)
	sub, err := s.SubscribeTicker(ctx5(t), []string{"BTC-USDT"}, stream.WithBuffer(3), stream.WithOverflow(stream.DropOldest))
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
		frames[i] = spTrade
	}
	h.fake.OnSubscribe("/market/match:BTC-USDT", frames...)
	h.fake.OnSubscribe("/market/ticker:BTC-USDT", spTicker)
	s := h.public(t)
	slow, err := s.SubscribeTrades(ctx5(t), []string{"BTC-USDT"}, stream.WithBuffer(2), stream.WithOverflow(stream.FailSubscription))
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := s.SubscribeTicker(ctx5(t), []string{"BTC-USDT"})
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
	if v := next(t, healthy); v.Symbol != "BTC-USDT" {
		t.Fatalf("the healthy subscription must keep working: %+v", v)
	}
	if s.State() != stream.StateConnected {
		t.Fatal("the connection must stay up")
	}
}
