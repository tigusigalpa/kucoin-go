package streaming

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/types"
)

// Numeric conventions: prices, sizes, rates and every other value that can carry
// decimals are types.Decimal (exact text, never float64); sequences, counters and
// timestamps are types.Int64; identifiers are types.ID. All three accept the JSON
// string and number spellings KuCoin uses interchangeably (the same field is a
// string on one market and a number on another). Timestamp units are stated on
// every field because the UTA gateway mixes nanoseconds, milliseconds and seconds.
//
// Case-sensitive keys: UTA payloads use single-letter keys whose upper- and
// lower-case spellings mean different things (s symbol / S side, a ask price / A
// ask size, os status / oS source, ...). encoding/json falls back to a
// case-insensitive match when a key has no exact counterpart, so every struct that
// decodes such a payload declares BOTH spellings; otherwise the key that arrives
// last would silently overwrite the other. models_test.go proves that every key of
// every official example binds to a field with exactly that spelling.

// nanos, millis and secs convert KuCoin timestamps; a zero value (absent field)
// yields the zero time.Time so that IsZero reports it.
func nanos(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func millis(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

func secs(s int64) time.Time {
	if s == 0 {
		return time.Time{}
	}
	return time.Unix(s, 0)
}

// unixAuto converts a Unix timestamp whose unit is not fixed by the documentation
// by its magnitude: nanoseconds, microseconds, milliseconds or seconds. It is
// used where KuCoin's own examples disagree (balance updates carry nanoseconds on
// the unified account and milliseconds on the funding account).
func unixAuto(v int64) time.Time {
	switch {
	case v == 0:
		return time.Time{}
	case v >= 1e17 || v <= -1e17:
		return time.Unix(0, v)
	case v >= 1e14 || v <= -1e14:
		return time.UnixMicro(v)
	case v >= 1e11 || v <= -1e11:
		return time.UnixMilli(v)
	default:
		return time.Unix(v, 0)
	}
}

// TradeType is the product a push belongs to.
type TradeType string

// Trade types of the UTA gateway.
const (
	TradeTypeSpot    TradeType = "SPOT"
	TradeTypeFutures TradeType = "FUTURES"
	TradeTypeMargin  TradeType = "MARGIN"
	TradeTypeUnified TradeType = "UNIFIED"
)

// Side is the direction of a trade, order or execution. KuCoin writes it in
// upper case on some channels and in lower case on others (the spot ticker sends
// "BUY", the futures ticker "buy"); Side is always upper case, so a value read
// from any channel compares with SideBuy and SideSell.
type Side string

// Sides.
const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

// UnmarshalJSON accepts a JSON string in any case, or null.
func (s *Side) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	*s = Side(strings.ToUpper(text))
	return nil
}

// Interval is a candle interval of the kline channel.
type Interval string

// Candle intervals of the kline channel. Interval6Hour exists on spot only.
const (
	Interval1Min   Interval = "1min"
	Interval3Min   Interval = "3min"
	Interval5Min   Interval = "5min"
	Interval15Min  Interval = "15min"
	Interval30Min  Interval = "30min"
	Interval1Hour  Interval = "1hour"
	Interval2Hour  Interval = "2hour"
	Interval4Hour  Interval = "4hour"
	Interval6Hour  Interval = "6hour"
	Interval8Hour  Interval = "8hour"
	Interval12Hour Interval = "12hour"
	Interval1Day   Interval = "1day"
	Interval1Week  Interval = "1week"
	Interval1Month Interval = "1month"
)

// Valid reports whether the interval exists on at least one market.
func (i Interval) Valid() bool {
	switch i {
	case Interval1Min, Interval3Min, Interval5Min, Interval15Min, Interval30Min, Interval1Hour,
		Interval2Hour, Interval4Hour, Interval6Hour, Interval8Hour, Interval12Hour,
		Interval1Day, Interval1Week, Interval1Month:
		return true
	}
	return false
}

// ValidFor reports whether the interval exists on the given market: the 6hour
// candle is not offered for futures (KuCoin accepts the subscription but never
// pushes a candle).
func (i Interval) ValidFor(tradeType string) bool {
	if !i.Valid() {
		return false
	}
	return i != Interval6Hour || !strings.EqualFold(tradeType, string(TradeTypeFutures))
}

// Depth selects what an order-book subscription delivers.
type Depth string

// Order-book depths of the obu channel.
const (
	// Depth1 is the best bid and offer, pushed in real time; every push is a
	// complete snapshot.
	Depth1 Depth = "1"
	// Depth5 is the best five levels per side, pushed every 100ms; every push is a
	// complete snapshot.
	Depth5 Depth = "5"
	// Depth50 is the best fifty levels per side, pushed every 100ms; every push is a
	// complete snapshot.
	Depth50 Depth = "50"
	// DepthIncrement is the real-time incremental feed. It needs a REST snapshot
	// to build a book (SubscribeOrderBookIncrement does that).
	//
	// Deprecated: KuCoin announced the removal of this depth on 2026-07-15 and asks
	// for DepthIncrement10ms instead. It still works at the time of writing.
	DepthIncrement Depth = "increment"
	// DepthIncrement10ms is the incremental feed aggregated per 10ms window ("Increment
	// Best 500"): the server pushes a snapshot first and deltas afterwards, so no
	// REST snapshot is needed. SubscribeOrderBook maintains a book from it.
	DepthIncrement10ms Depth = "increment@10ms"
)

// Valid reports whether KuCoin offers the depth.
func (d Depth) Valid() bool {
	switch d {
	case Depth1, Depth5, Depth50, DepthIncrement, DepthIncrement10ms:
		return true
	}
	return false
}

// IsSnapshot reports whether every push at this depth is a complete snapshot
// (depths 1, 5 and 50); the incremental depths push deltas.
func (d Depth) IsSnapshot() bool { return d == Depth1 || d == Depth5 || d == Depth50 }

// UpdateKind tells a snapshot from a delta on an order-book push.
type UpdateKind string

// Order-book update kinds.
const (
	UpdateSnapshot UpdateKind = "snapshot"
	UpdateDelta    UpdateKind = "delta"
)

// Ticker is a ticker update of the ticker channel. KuCoin pushes it when a trade
// occurs; best-bid/offer changes alone do not trigger it.
//
// Sequence is increasing but not continuous. MatchTimestamp is the match-engine
// time: on spot the time the best bid or ask last changed, on futures the time
// of the trade that changed it.
//
// Docs: https://www.kucoin.com/docs-new/3470355w0
type Ticker struct {
	Symbol    string    `json:"s"`
	TradeType TradeType `json:"-"`
	// Sequence is the increasing (not continuous) sequence number.
	Sequence     types.Int64   `json:"E"`
	BestBidPrice types.Decimal `json:"b"`
	BestBidSize  types.Decimal `json:"B"`
	BestAskPrice types.Decimal `json:"a"`
	BestAskSize  types.Decimal `json:"A"`
	LastPrice    types.Decimal `json:"l"`
	LastSize     types.Decimal `json:"q"`
	// Side is the taker side of the last trade.
	Side Side `json:"S"`
	// MatchTimestamp is the match-engine time in nanoseconds.
	MatchTimestamp types.Int64 `json:"M"`
	// GatewayTimestamp is the gateway push time in nanoseconds.
	GatewayTimestamp types.Int64 `json:"-"`
}

// Time converts MatchTimestamp (nanoseconds) to a time.Time.
func (t Ticker) Time() time.Time { return nanos(int64(t.MatchTimestamp)) }

// GatewayTime converts GatewayTimestamp (nanoseconds) to a time.Time.
func (t Ticker) GatewayTime() time.Time { return nanos(int64(t.GatewayTimestamp)) }

// Trade is a match of the trade channel.
//
// Docs: https://www.kucoin.com/docs-new/3470359w0
type Trade struct {
	Symbol    string    `json:"s"`
	TradeType TradeType `json:"-"`
	// Sequence is the sequence number; on spot it equals the trade ID.
	Sequence types.Int64   `json:"E"`
	TradeID  types.ID      `json:"ti"`
	Price    types.Decimal `json:"p"`
	// Size is in base currency on spot and in contracts (lots) on futures.
	Size types.Decimal `json:"q"`
	// Side is the taker side.
	Side Side `json:"S"`
	// RPI reports whether the trade involved an RPI order (futures only).
	RPI bool `json:"rpi"`
	// MatchTimestamp is the match-engine time in nanoseconds.
	MatchTimestamp types.Int64 `json:"M"`
	// GatewayTimestamp is the gateway push time in nanoseconds.
	GatewayTimestamp types.Int64 `json:"-"`
}

// Time converts MatchTimestamp (nanoseconds) to a time.Time.
func (t Trade) Time() time.Time { return nanos(int64(t.MatchTimestamp)) }

// GatewayTime converts GatewayTimestamp (nanoseconds) to a time.Time.
func (t Trade) GatewayTime() time.Time { return nanos(int64(t.GatewayTimestamp)) }

// Kline is a candle update of the kline channel, pushed every second while the
// candle is open.
//
// Docs: https://www.kucoin.com/docs-new/3470356w0
type Kline struct {
	Symbol    string    `json:"s"`
	TradeType TradeType `json:"-"`
	Interval  Interval  `json:"i"`
	// StartTimestamp and EndTimestamp delimit the candle period in Unix seconds.
	StartTimestamp types.Int64   `json:"O"`
	EndTimestamp   types.Int64   `json:"C"`
	Open           types.Decimal `json:"o"`
	Close          types.Decimal `json:"c"`
	High           types.Decimal `json:"h"`
	Low            types.Decimal `json:"l"`
	// Volume is in base currency on spot and in contracts (lots) on futures.
	Volume types.Decimal `json:"v"`
	// Amount is the transaction amount (the quote-currency turnover).
	Amount types.Decimal `json:"a"`
	// First reports whether this push is the first tick of the candle period.
	First bool `json:"S"`
	// GatewayTimestamp is the gateway push time in nanoseconds.
	GatewayTimestamp types.Int64 `json:"-"`
}

// Start converts StartTimestamp (seconds) to a time.Time.
func (k Kline) Start() time.Time { return secs(int64(k.StartTimestamp)) }

// End converts EndTimestamp (seconds) to a time.Time.
func (k Kline) End() time.Time { return secs(int64(k.EndTimestamp)) }

// Time converts GatewayTimestamp (nanoseconds) to a time.Time, the moment the
// candle was pushed.
func (k Kline) Time() time.Time { return nanos(int64(k.GatewayTimestamp)) }

// OrderBookUpdate is one push of the obu channel. What it holds depends on the
// subscribed depth: Depth1, Depth5 and Depth50 pushes are complete snapshots that
// replace the previous one; DepthIncrement and DepthIncrement10ms push deltas
// (the latter starts with one snapshot). Bids are ordered best (highest) first,
// asks best (lowest) first; a size of zero in a delta removes the level.
//
// Sequence numbers: StartSequence and EndSequence delimit the range of changes the
// push covers; a snapshot has StartSequence == EndSequence. The rule used by
// orderbook.Book applies: a delta continues a book at sequence n when
// StartSequence <= n+1 and EndSequence > n.
//
// With the RPI filter (SubscribeOrderBookUpdatesRPI) every level has a third
// element: Level.Size is the non-RPI size and Level.RPISize the RPI size.
//
// SubscribeOrderBook maintains a complete book from these pushes for you.
//
// Docs: https://www.kucoin.com/docs-new/3470354w0
type OrderBookUpdate struct {
	Symbol    string     `json:"s"`
	TradeType TradeType  `json:"-"`
	Depth     Depth      `json:"-"`
	Kind      UpdateKind `json:"-"`
	// StartSequence and EndSequence are the first and last sequence number of the
	// changes in this push.
	StartSequence types.Int64 `json:"O"`
	EndSequence   types.Int64 `json:"C"`
	// MatchTimestamp is the match-engine time of the last change, in nanoseconds.
	MatchTimestamp types.Int64 `json:"M"`
	// GatewayTimestamp is the gateway push time in nanoseconds.
	GatewayTimestamp types.Int64       `json:"-"`
	Bids             []orderbook.Level `json:"b"`
	Asks             []orderbook.Level `json:"a"`
}

// Time converts MatchTimestamp (nanoseconds) to a time.Time.
func (u OrderBookUpdate) Time() time.Time { return nanos(int64(u.MatchTimestamp)) }

// GatewayTime converts GatewayTimestamp (nanoseconds) to a time.Time.
func (u OrderBookUpdate) GatewayTime() time.Time { return nanos(int64(u.GatewayTimestamp)) }

// IsSnapshot reports whether the push replaces the whole book.
func (u OrderBookUpdate) IsSnapshot() bool { return u.Kind == UpdateSnapshot }

// ToSnapshot converts the push to an orderbook.Snapshot at EndSequence. The levels
// are copied, including their RPI sizes.
func (u OrderBookUpdate) ToSnapshot() orderbook.Snapshot {
	return orderbook.Snapshot{
		Symbol:   u.Symbol,
		Sequence: int64(u.EndSequence),
		Bids:     append([]orderbook.Level(nil), u.Bids...),
		Asks:     append([]orderbook.Level(nil), u.Asks...),
	}
}

// ToDelta converts the push to an orderbook.Delta covering StartSequence to
// EndSequence: every bid and ask level becomes a price-level change (size zero
// removes the level). RPI sizes are not part of a Delta; for an RPI push the
// change carries the non-RPI size.
func (u OrderBookUpdate) ToDelta() orderbook.Delta {
	changes := make([]orderbook.Change, 0, len(u.Bids)+len(u.Asks))
	for _, l := range u.Bids {
		changes = append(changes, orderbook.Change{Side: orderbook.Bid, Price: l.Price, Size: l.Size})
	}
	for _, l := range u.Asks {
		changes = append(changes, orderbook.Change{Side: orderbook.Ask, Price: l.Price, Size: l.Size})
	}
	return orderbook.Delta{Symbol: u.Symbol, Start: int64(u.StartSequence), End: int64(u.EndSequence), Changes: changes}
}

// MarkPrice is an update of the mark-price channel (futures), pushed every second.
//
// Docs: https://www.kucoin.com/docs-new/3470358w0
type MarkPrice struct {
	Symbol       string        `json:"s"`
	MarkPrice    types.Decimal `json:"mp"`
	IndexPrice   types.Decimal `json:"ip"`
	OpenInterest types.Decimal `json:"oi"`
	// Timestamp is the time the mark price was calculated, in milliseconds.
	Timestamp types.Int64 `json:"ts"`
	// GatewayTimestamp is the gateway push time in nanoseconds. KuCoin's published
	// example shows milliseconds; the live gateway sends nanoseconds, and
	// GatewayTime detects either by magnitude.
	GatewayTimestamp types.Int64 `json:"-"`
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (m MarkPrice) Time() time.Time { return millis(int64(m.Timestamp)) }

// GatewayTime converts GatewayTimestamp to a time.Time.
func (m MarkPrice) GatewayTime() time.Time { return unixAuto(int64(m.GatewayTimestamp)) }

// FundingRate is an update of the funding-fee channel (futures), pushed every
// minute.
//
// Docs: https://www.kucoin.com/docs-new/3470357w0
type FundingRate struct {
	Symbol string `json:"s"`
	// Rate is the current funding rate.
	Rate types.Decimal `json:"fr"`
	// LastRate is the rate that applied at the last settlement.
	LastRate types.Decimal `json:"lfr"`
	// MaxRate and MinRate bound the funding rate (the cap and the floor).
	MaxRate types.Decimal `json:"fc"`
	MinRate types.Decimal `json:"ff"`
	// LastSettlementTimestamp and NextSettlementTimestamp are the previous and the
	// next settlement time points in milliseconds.
	LastSettlementTimestamp types.Int64 `json:"ft"`
	NextSettlementTimestamp types.Int64 `json:"nt"`
	// IntervalMillis is the settlement interval in milliseconds (28800000 for eight
	// hours).
	IntervalMillis types.Int64 `json:"gl"`
	// GatewayTimestamp is the gateway push time in nanoseconds.
	GatewayTimestamp types.Int64 `json:"-"`
}

// LastSettlement converts LastSettlementTimestamp (milliseconds) to a time.Time.
func (f FundingRate) LastSettlement() time.Time { return millis(int64(f.LastSettlementTimestamp)) }

// NextSettlement converts NextSettlementTimestamp (milliseconds) to a time.Time.
func (f FundingRate) NextSettlement() time.Time { return millis(int64(f.NextSettlementTimestamp)) }

// Interval returns the settlement interval as a time.Duration.
func (f FundingRate) Interval() time.Duration {
	return time.Duration(f.IntervalMillis) * time.Millisecond
}

// Time converts GatewayTimestamp (nanoseconds) to a time.Time, the moment the
// rate was pushed.
func (f FundingRate) Time() time.Time { return nanos(int64(f.GatewayTimestamp)) }

// AllFundingRates is one push of the funding-fee-all-symbols channel (futures):
// the funding data of every contract, pushed every minute. KuCoin notes that its
// latency may be higher than that of the per-symbol funding-fee channel.
//
// Docs: https://www.kucoin.com/docs-new/3470412w0
type AllFundingRates struct {
	Rates []FundingRate
	// GatewayTimestamp is the gateway push time in nanoseconds.
	GatewayTimestamp types.Int64
}

// Time converts GatewayTimestamp (nanoseconds) to a time.Time.
func (a AllFundingRates) Time() time.Time { return nanos(int64(a.GatewayTimestamp)) }

// CallAuctionInfo is an update of the callAuctionInfo channel (spot): during the
// call-auction phase of a symbol it carries the estimated price and size and the
// order price ranges, every 100ms.
//
// The documentation names the estimated price "ep" in its schema and "eq" in its
// example; both are accepted and EstimatedPrice holds whichever was sent. The
// documented strings are JSON numbers on the live gateway; Decimal takes both.
//
// Docs: https://www.kucoin.com/docs-new/3470353w0
type CallAuctionInfo struct {
	Symbol string `json:"s"`
	// Kind is the push type, "snapshot".
	Kind UpdateKind `json:"-"`
	// EstimatedPrice and EstimatedSize are the estimated transaction price and size.
	EstimatedPrice types.Decimal `json:"ep"`
	EstimatedSize  types.Decimal `json:"es"`
	// SellLowPrice and SellHighPrice are the lowest and highest sell order prices;
	// BuyLowPrice and BuyHighPrice the lowest and highest buy order prices.
	SellLowPrice  types.Decimal `json:"slp"`
	SellHighPrice types.Decimal `json:"shp"`
	BuyLowPrice   types.Decimal `json:"blp"`
	BuyHighPrice  types.Decimal `json:"bhp"`
	// Timestamp is in milliseconds.
	Timestamp types.Int64 `json:"ts"`
	// GatewayTimestamp is the gateway push time in nanoseconds.
	GatewayTimestamp types.Int64 `json:"-"`
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (c CallAuctionInfo) Time() time.Time { return millis(int64(c.Timestamp)) }

// GatewayTime converts GatewayTimestamp (nanoseconds) to a time.Time.
func (c CallAuctionInfo) GatewayTime() time.Time { return nanos(int64(c.GatewayTimestamp)) }

// callAuctionWire is the payload of the call-auction channel as sent: the
// estimated price arrives as "ep" (schema) or "eq" (example).
type callAuctionWire struct {
	Symbol        string        `json:"s"`
	EstimatedPx   types.Decimal `json:"ep"`
	EstimatedEq   types.Decimal `json:"eq"`
	EstimatedSize types.Decimal `json:"es"`
	SellLow       types.Decimal `json:"slp"`
	SellHigh      types.Decimal `json:"shp"`
	BuyLow        types.Decimal `json:"blp"`
	BuyHigh       types.Decimal `json:"bhp"`
	Timestamp     types.Int64   `json:"ts"`
}
