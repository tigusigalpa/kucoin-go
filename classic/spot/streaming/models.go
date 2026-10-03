package streaming

import (
	"strings"
	"time"

	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/types"
)

// Numeric conventions: prices, rates, sizes of funds and other values that can
// carry decimals are types.Decimal (exact text, never float64); sequences,
// counts and timestamps are types.Int64. Both accept the JSON string and number
// spellings KuCoin uses interchangeably: the symbol snapshot sends bare numbers
// with long decimals such as 315.20000000000000000000, the order channels send
// strings, the trade time is a numeric string. Timestamp units are stated on
// every field: KuCoin mixes milliseconds and nanoseconds, sometimes inside one
// channel.

func millis(ms int64) time.Time { return time.UnixMilli(ms) }
func nanos(ns int64) time.Time  { return time.Unix(0, ns) }

// Side values of a trade or an order.
const (
	SideBuy  = "buy"
	SideSell = "sell"
)

// Ticker is a best-bid/offer update from /market/ticker:{symbol},{symbol},
// pushed at most once every 100ms whenever the best bid, the best offer or the
// last trade of the symbol changes.
//
// Docs: https://www.kucoin.com/docs-new/3470063w0
type Ticker struct {
	// Symbol is taken from the topic: the payload itself does not carry it.
	Symbol string `json:"-"`
	// Sequence is the symbol's ticker sequence number.
	Sequence types.Int64 `json:"sequence"`
	// Price and Size are the price and the amount of the last trade.
	Price types.Decimal `json:"price"`
	Size  types.Decimal `json:"size"`
	// BestAsk and BestBid are the best offer and the best bid, BestAskSize and
	// BestBidSize the amounts resting at them (in the base currency).
	BestAsk     types.Decimal `json:"bestAsk"`
	BestAskSize types.Decimal `json:"bestAskSize"`
	BestBid     types.Decimal `json:"bestBid"`
	BestBidSize types.Decimal `json:"bestBidSize"`
	// Timestamp is the matching time of the last trade in milliseconds. It moves
	// only when a trade happens, not when just the best bid or offer changes.
	//
	// The documentation spells the key "time" in the schema and "Time" in the
	// example; the decoder accepts both.
	Timestamp types.Int64 `json:"time"`
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (t Ticker) Time() time.Time { return millis(int64(t.Timestamp)) }

// AllTickerUpdate is a push of /market/ticker:all, which carries the best bid
// and offer of every symbol, pushed at most once every 100ms per symbol. It has
// the fields of Ticker; Symbol is taken from the push's subject, because the
// topic is the same for every symbol.
//
// The channel is busy (hundreds of symbols, and a burst of one update per
// symbol right after subscribing): pass stream.WithBuffer for a larger queue
// when the consumer is not fast.
//
// Docs: https://www.kucoin.com/docs-new/3470064w0
type AllTickerUpdate struct {
	Ticker
}

// Board values of SymbolSnapshot.Board.
const (
	// BoardPrimary is the primary trading-pair partition.
	BoardPrimary = 0
	// BoardKuCoinPlus is the KuCoin Plus partition.
	BoardKuCoinPlus = 1
)

// Mark values of SymbolSnapshot.Mark.
const (
	// MarkDefault is a regular trading pair.
	MarkDefault = 0
	// MarkST is a trading pair with the ST (special treatment) mark.
	MarkST = 1
	// MarkNew is a newly listed trading pair.
	MarkNew = 2
)

// MarketChange holds the statistics of one rolling window (one hour, four hours
// or 24 hours) of a SymbolSnapshot.
type MarketChange struct {
	// ChangePrice and ChangeRate are the price change over the window, absolute
	// and as a fraction (0.0047 is 0.47%).
	ChangePrice types.Decimal `json:"changePrice"`
	ChangeRate  types.Decimal `json:"changeRate"`
	// High, Low and Open are the extreme and the opening price of the window.
	High types.Decimal `json:"high"`
	Low  types.Decimal `json:"low"`
	Open types.Decimal `json:"open"`
	// Vol is the traded amount in the base currency, VolValue the traded value in
	// the quote currency.
	Vol      types.Decimal `json:"vol"`
	VolValue types.Decimal `json:"volValue"`
}

// SymbolSnapshot is the market statistics push of a trading pair from
// /market/snapshot:{symbol} (every 2 seconds) or, for every pair of a market,
// from /market/snapshot:{market}.
//
// On the wire the snapshot is nested one level deeper than the other channels
// (data.sequence next to data.data); this type flattens it. Symbol always comes
// from the payload, which matters for the market channel whose topic names the
// market, not the symbol.
//
// Docs: https://www.kucoin.com/docs-new/3470065w0 and https://www.kucoin.com/docs-new/3470066w0
type SymbolSnapshot struct {
	// Sequence is the sequence number the push carries next to the snapshot. On
	// the symbol channel it is the symbol's counter, on the market channel a
	// millisecond clock value, so only compare it between pushes of one symbol
	// subscription.
	Sequence types.Int64 `json:"-"`
	// SymbolSequence is the symbol's own sequence number from inside the
	// snapshot. KuCoin does not document it but sends it on live pushes; it equals
	// Sequence on the symbol channel and is the real counter on the market
	// channel.
	SymbolSequence types.Int64 `json:"sequence"`

	Symbol        string `json:"symbol"`
	SymbolCode    string `json:"symbolCode"`
	BaseCurrency  string `json:"baseCurrency"`
	QuoteCurrency string `json:"quoteCurrency"`
	// Market is the market the pair is listed in ("USDS", "BTC", "ALTS", ...).
	Market string `json:"market"`
	// Markets lists every market and category the pair belongs to.
	Markets   []string `json:"markets"`
	SiteTypes []string `json:"siteTypes"`
	// Board is the trading-pair partition (BoardPrimary, BoardKuCoinPlus), Mark
	// the trading-pair mark (MarkDefault, MarkST, MarkNew) and Sort a sorting
	// number that has no meaning for consumers.
	Board types.Int64 `json:"board"`
	Mark  types.Int64 `json:"mark"`
	Sort  types.Int64 `json:"sort"`
	// MarginTrade tells whether the pair can be traded on margin and Trading
	// whether it is open for trading.
	MarginTrade bool `json:"marginTrade"`
	Trading     bool `json:"trading"`

	Open  types.Decimal `json:"open"`
	Close types.Decimal `json:"close"`
	High  types.Decimal `json:"high"`
	Low   types.Decimal `json:"low"`
	// LastTradedPrice is the price of the last trade and LastSize its amount
	// (LastSize is not in the schema but present on live pushes).
	LastTradedPrice types.Decimal `json:"lastTradedPrice"`
	LastSize        types.Decimal `json:"lastSize"`
	// Buy and Sell are the best bid and best offer, BidSize and AskSize the
	// amounts resting at them.
	Buy          types.Decimal `json:"buy"`
	Sell         types.Decimal `json:"sell"`
	BidSize      types.Decimal `json:"bidSize"`
	AskSize      types.Decimal `json:"askSize"`
	AveragePrice types.Decimal `json:"averagePrice"`
	// ChangePrice and ChangeRate describe the 24-hour change, Vol and VolValue the
	// 24-hour traded amount and value (the same numbers as MarketChange24h).
	ChangePrice types.Decimal `json:"changePrice"`
	ChangeRate  types.Decimal `json:"changeRate"`
	Vol         types.Decimal `json:"vol"`
	VolValue    types.Decimal `json:"volValue"`

	MakerFeeRate     types.Decimal `json:"makerFeeRate"`
	TakerFeeRate     types.Decimal `json:"takerFeeRate"`
	MakerCoefficient types.Decimal `json:"makerCoefficient"`
	TakerCoefficient types.Decimal `json:"takerCoefficient"`

	MarketChange1h  MarketChange `json:"marketChange1h"`
	MarketChange4h  MarketChange `json:"marketChange4h"`
	MarketChange24h MarketChange `json:"marketChange24h"`

	// Datetime is the snapshot time in milliseconds.
	Datetime types.Int64 `json:"datetime"`
}

// Time converts Datetime (milliseconds) to a time.Time.
func (s SymbolSnapshot) Time() time.Time { return millis(int64(s.Datetime)) }

// Level1 is the best bid and best offer of a symbol from
// /spotMarket/level1:{symbol},{symbol}, pushed at most once every 10ms and only
// when the top of the book changes.
//
// Docs: https://www.kucoin.com/docs-new/3470067w0
type Level1 struct {
	// Symbol is taken from the topic: the payload itself does not carry it.
	Symbol string `json:"-"`
	// Ask is the best offer and Bid the best bid. A side without any order is the
	// zero Level (empty Price).
	Ask orderbook.Level `json:"-"`
	Bid orderbook.Level `json:"-"`
	// Timestamp is the exchange's push time in milliseconds.
	Timestamp types.Int64 `json:"timestamp"`
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (l Level1) Time() time.Time { return millis(int64(l.Timestamp)) }

// OrderBookDepth is a top-of-book snapshot from /spotMarket/level2Depth5,
// /spotMarket/level2Depth50 or /callauction/level2Depth50, pushed at most once
// every 100ms and only when the book changes. Each push replaces the previous
// one; no local book maintenance is needed. Bids are ordered best (highest) first
// and asks best (lowest) first.
//
// Unlike the Futures feeds these pushes carry no sequence number.
//
// Docs: https://www.kucoin.com/docs-new/3470069w0, https://www.kucoin.com/docs-new/3470070w0 and https://www.kucoin.com/docs-new/3470137w0
type OrderBookDepth struct {
	// Symbol is taken from the topic: the payload itself does not carry it.
	Symbol string            `json:"-"`
	Bids   []orderbook.Level `json:"bids"`
	Asks   []orderbook.Level `json:"asks"`
	// Timestamp is the push time in milliseconds.
	Timestamp types.Int64 `json:"timestamp"`
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (d OrderBookDepth) Time() time.Time { return millis(int64(d.Timestamp)) }

// LevelChange is one price level of a level-2 increment: the new total Size
// resting at Price (zero removes the level).
type LevelChange struct {
	Price types.Decimal
	Size  types.Decimal
	// Sequence is the sequence number of the price's last modification. KuCoin
	// states that it does not prove the continuity of the feed; continuity is
	// judged by OrderBookChange.SequenceStart and SequenceEnd alone.
	Sequence types.Int64
}

// OrderBookChange is one incremental update of the level-2 order book from
// /market/level2:{symbol},{symbol}. It covers the sequence numbers SequenceStart
// to SequenceEnd (inclusive; often a single number) and lists the price levels
// that changed on each side.
//
// The updates must be applied in sequence order to a REST snapshot:
// SubscribeOrderBook does that, including the resynchronisation after a gap or a
// reconnect, and is what most applications want. A row whose Price is zero is not
// part of the book: KuCoin tells consumers to skip it but still advance the
// sequence. The managed order book does so; this raw view reports the rows as
// they were received.
//
// Docs: https://www.kucoin.com/docs-new/3470068w0
type OrderBookChange struct {
	Symbol string `json:"-"`
	// SequenceStart and SequenceEnd bound the sequence numbers the update covers.
	// A new update continues a book at sequence N when SequenceStart <= N+1 and
	// SequenceEnd > N; SequenceEnd <= N means the book already contains it and
	// SequenceStart > N+1 means updates were missed.
	SequenceStart types.Int64 `json:"-"`
	SequenceEnd   types.Int64 `json:"-"`
	// Asks and Bids are the changed levels of each side.
	Asks []LevelChange `json:"-"`
	Bids []LevelChange `json:"-"`
	// Timestamp is the push time in milliseconds.
	Timestamp types.Int64 `json:"-"`
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (c OrderBookChange) Time() time.Time { return millis(int64(c.Timestamp)) }

// delta converts the update to the form the order-book synchroniser applies:
// rows with a zero price are dropped (the sequence still advances) and the rest
// become changes of the matching side.
func (c OrderBookChange) delta() orderbook.Delta {
	d := orderbook.Delta{
		Symbol:  c.Symbol,
		Start:   int64(c.SequenceStart),
		End:     int64(c.SequenceEnd),
		Changes: make([]orderbook.Change, 0, len(c.Asks)+len(c.Bids)),
	}
	add := func(side orderbook.Side, rows []LevelChange) {
		for _, r := range rows {
			if r.Price.IsZero() {
				continue
			}
			d.Changes = append(d.Changes, orderbook.Change{Side: side, Price: r.Price, Size: r.Size})
		}
	}
	add(orderbook.Ask, c.Asks)
	add(orderbook.Bid, c.Bids)
	return d
}

// Interval is a candle interval of the kline channel.
type Interval string

// Candle intervals of /market/candles. The documentation's table of the channel
// leaves out Interval5Min, which the REST endpoint and the live feed have; it is
// offered nevertheless.
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
)

// Valid reports whether the interval is one this package offers for the Spot kline
// channel. KuCoin acknowledges a subscription with an interval it does not serve
// (2min, for example) and then never sends a candle, which is why it is checked
// locally. The live feed also serves "1month" candles (the period starts on the
// first day of the month at 00:00 UTC), which the documentation's table omits and
// this package therefore does not offer; Client gives access to such a stream.
func (i Interval) Valid() bool {
	switch i {
	case Interval1Min, Interval3Min, Interval5Min, Interval15Min, Interval30Min, Interval1Hour,
		Interval2Hour, Interval4Hour, Interval6Hour, Interval8Hour, Interval12Hour, Interval1Day, Interval1Week:
		return true
	}
	return false
}

// Kline is a candle update from /market/candles:{symbol}_{type}, pushed in real
// time while the candle changes.
//
// On the wire the candle is an array in the order
// [start time, open, close, high, low, volume, turnover] — close comes before
// high and low, unlike the REST klines.
//
// Docs: https://www.kucoin.com/docs-new/3470071w0
type Kline struct {
	Symbol   string   `json:"symbol"`
	Interval Interval `json:"-"`
	// StartTime is the start of the candle period in Unix seconds.
	StartTime int64         `json:"-"`
	Open      types.Decimal `json:"-"`
	Close     types.Decimal `json:"-"`
	High      types.Decimal `json:"-"`
	Low       types.Decimal `json:"-"`
	// Volume is the traded amount in the base currency and Turnover the traded
	// value in the quote currency; only one side of each trade is counted.
	Volume   types.Decimal `json:"-"`
	Turnover types.Decimal `json:"-"`
	// Timestamp is the push time in nanoseconds. The documentation's example
	// comment says microseconds, its schema and the live feed use nanoseconds.
	Timestamp types.Int64 `json:"time"`
	// Candles is the raw array.
	Candles []types.Decimal `json:"candles"`
}

// Start converts StartTime to a time.Time.
func (k Kline) Start() time.Time { return time.Unix(k.StartTime, 0) }

// Time converts Timestamp (nanoseconds) to a time.Time.
func (k Kline) Time() time.Time { return nanos(int64(k.Timestamp)) }

// Trade is a match (a Level 3 match event) from /market/match:{symbol},{symbol}.
//
// Docs: https://www.kucoin.com/docs-new/3470072w0
type Trade struct {
	Symbol   string      `json:"symbol"`
	Sequence types.Int64 `json:"sequence"`
	// Type is "match".
	Type string `json:"type"`
	// Side is the taker's side: "buy" or "sell".
	Side         string        `json:"side"`
	Price        types.Decimal `json:"price"`
	Size         types.Decimal `json:"size"`
	TradeID      types.ID      `json:"tradeId"`
	TakerOrderID types.ID      `json:"takerOrderId"`
	MakerOrderID types.ID      `json:"makerOrderId"`
	// Timestamp is the matching-engine execution time in nanoseconds (a numeric
	// string on the wire).
	Timestamp types.Int64 `json:"time"`
}

// Time converts Timestamp (nanoseconds) to a time.Time.
func (t Trade) Time() time.Time { return nanos(int64(t.Timestamp)) }

// CallAuctionData is the state of a call auction from
// /callauction/callauctionData:{symbol}, pushed at most once every 100ms while
// the symbol is in its call auction phase: the estimated match and the price
// ranges of the orders. Nothing is pushed outside an auction.
//
// The documentation is inconsistent about this channel: its example (which this
// type follows) uses the full names below, its schema lists abbreviated names
// (s, ep, es, slp, shp, blp, bhp, ts) copied from the UTA channel. The decoder
// accepts both spellings.
//
// Docs: https://www.kucoin.com/docs-new/3470138w0
type CallAuctionData struct {
	Symbol string `json:"-"`
	// EstimatedPrice and EstimatedSize are the price and the amount the auction
	// would match at now.
	EstimatedPrice types.Decimal `json:"-"`
	EstimatedSize  types.Decimal `json:"-"`
	// SellOrderRangeLowPrice and SellOrderRangeHighPrice bound the prices of the
	// sell orders, BuyOrderRangeLowPrice and BuyOrderRangeHighPrice those of the
	// buy orders.
	SellOrderRangeLowPrice  types.Decimal `json:"-"`
	SellOrderRangeHighPrice types.Decimal `json:"-"`
	BuyOrderRangeLowPrice   types.Decimal `json:"-"`
	BuyOrderRangeHighPrice  types.Decimal `json:"-"`
	// Timestamp is in milliseconds.
	Timestamp types.Int64 `json:"-"`
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (c CallAuctionData) Time() time.Time { return millis(int64(c.Timestamp)) }

// PrivateEnvelope holds the fields every private-channel push carries next to
// its payload.
type PrivateEnvelope struct {
	// Subject names the kind of update within the topic.
	Subject string `json:"-"`
	// UserID is the KuCoin user the update belongs to.
	UserID string `json:"-"`
	// ChannelType is "private" for private topics.
	ChannelType string `json:"-"`
}

// Order status, order event type and related values of the order channels.
const (
	// OrderStatusNew: the order entered the matching system (V2 only).
	OrderStatusNew = "new"
	// OrderStatusOpen: the order rests in the order book (a maker order).
	OrderStatusOpen = "open"
	// OrderStatusMatch: a taker order is executing against the book.
	OrderStatusMatch = "match"
	// OrderStatusDone: the order is finished (filled or cancelled).
	OrderStatusDone = "done"

	// OrderEventReceived: the order entered the matching system and has not been
	// matched yet; always with status "new" (V2 only).
	OrderEventReceived = "received"
	// OrderEventOpen: the order was placed in the book.
	OrderEventOpen = "open"
	// OrderEventMatch: a trade. Status "open" is a maker match, "match" a taker
	// match.
	OrderEventMatch = "match"
	// OrderEventUpdate: the order was modified by a partial cancellation or by
	// self-trade prevention.
	OrderEventUpdate = "update"
	// OrderEventFilled: the order became "done" by being traded.
	OrderEventFilled = "filled"
	// OrderEventCanceled: the order became "done" by being cancelled.
	OrderEventCanceled = "canceled"

	OrderTypeLimit  = "limit"
	OrderTypeMarket = "market"

	FeeTypeTaker = "takerFee"
	FeeTypeMaker = "makerFee"

	LiquidityTaker = "taker"
	LiquidityMaker = "maker"
)

// OrderUpdate is an order event from /spotMarket/tradeOrdersV2 or
// /spotMarket/tradeOrders (all symbols, Spot and Margin orders alike). Type says
// what happened and Status the resulting order state:
//
//   - Type "received" (V2 only, Status "new"): the order entered the matching
//     system;
//   - Type "open" (Status "open"): the order was placed in the book;
//   - Type "match": a trade. Status "open" is a maker match, Status "match" a
//     taker match; MatchPrice, MatchSize, TradeID, FeeType and Liquidity are set;
//   - Type "update": a partial cancellation or self-trade prevention changed the
//     order (OldSize is the size before); Status is "open", "match" or "done";
//   - Type "filled" (Status "done"): the order was traded completely;
//   - Type "canceled" (Status "done"): the order was cancelled.
//
// V2 delivers every V1 event plus the "received"/"new" event; there is no
// difference in speed, so new code should prefer V2. Fields that an event kind
// does not use stay empty.
//
// Docs: https://www.kucoin.com/docs-new/3470073w0 (V2), https://www.kucoin.com/docs-new/3470074w0 (V1);
// Margin orders use the same specification: https://www.kucoin.com/docs-new/3470256w0 and https://www.kucoin.com/docs-new/3470257w0
type OrderUpdate struct {
	PrivateEnvelope
	Symbol string `json:"symbol"`
	Status string `json:"status"`
	Type   string `json:"type"`
	Side   string `json:"side"`
	// OrderType is "limit" or "market".
	OrderType string `json:"orderType"`
	// FeeType is "takerFee" or "makerFee" (the fee that was charged); Liquidity is
	// "taker" or "maker" (the role in the trade). They are set on matches.
	FeeType   string        `json:"feeType"`
	Liquidity string        `json:"liquidity"`
	Price     types.Decimal `json:"price"`
	OrderID   types.ID      `json:"orderId"`
	ClientOid string        `json:"clientOid"`
	TradeID   types.ID      `json:"tradeId"`
	// OriginSize is the size the order was placed with; Size is the current size,
	// which shrinks below OriginSize when part of the order is cancelled.
	OriginSize types.Decimal `json:"originSize"`
	Size       types.Decimal `json:"size"`
	// FilledSize and CanceledSize are cumulative.
	FilledSize   types.Decimal `json:"filledSize"`
	CanceledSize types.Decimal `json:"canceledSize"`
	// MatchPrice and MatchSize describe the trade of a "match" event.
	MatchPrice types.Decimal `json:"matchPrice"`
	MatchSize  types.Decimal `json:"matchSize"`
	// OldSize is the size before an "update".
	OldSize    types.Decimal `json:"oldSize"`
	RemainSize types.Decimal `json:"remainSize"`
	// RemainFunds is what is left of the funds of a market order placed by funds.
	RemainFunds types.Decimal `json:"remainFunds"`
	// OrderTime is when the gateway received the order, in milliseconds.
	OrderTime types.Int64 `json:"orderTime"`
	// Timestamp is when the matching engine finished executing, in nanoseconds; it
	// applies to every event kind.
	Timestamp types.Int64 `json:"ts"`
}

// Time converts Timestamp (nanoseconds) to a time.Time.
func (o OrderUpdate) Time() time.Time { return nanos(int64(o.Timestamp)) }

// GatewayTime converts OrderTime (milliseconds) to a time.Time.
func (o OrderUpdate) GatewayTime() time.Time { return millis(int64(o.OrderTime)) }

// RelationContext is the trade a balance change relates to.
type RelationContext struct {
	Symbol  string   `json:"symbol"`
	OrderID types.ID `json:"orderId"`
	TradeID types.ID `json:"tradeId"`
}

// BalanceUpdate is an account balance change from /account/balance, pushed in
// real time for every account of the user (main, trade, trade_hf, margin,
// isolated margin and their V2 variants).
//
// RelationEvent says what caused the change, as "<account>.<event>": the account
// is main, trade, trade_hf, margin, marginV2, isolated_{symbol} or
// isolatedV2_{symbol}, the event is deposit, withdraw_hold, withdraw_done (main
// only), hold (funds reserved by an order), setted (a settlement), transfer or
// other; a bare "other" is also possible. RelationContext names the symbol,
// order and trade behind a hold or settlement.
//
// Docs: https://www.kucoin.com/docs-new/3470075w0 (Margin uses the same specification: https://www.kucoin.com/docs-new/3470258w0)
type BalanceUpdate struct {
	PrivateEnvelope
	// ID is the ID of the push.
	ID string `json:"-"`

	AccountID types.ID `json:"accountId"`
	Currency  string   `json:"currency"`
	// Total is Available + Hold.
	Total types.Decimal `json:"total"`
	// Available can be withdrawn or traded, Hold is reserved.
	Available types.Decimal `json:"available"`
	Hold      types.Decimal `json:"hold"`
	// AvailableChange and HoldChange are the changes that caused this update.
	AvailableChange types.Decimal   `json:"availableChange"`
	HoldChange      types.Decimal   `json:"holdChange"`
	RelationContext RelationContext `json:"relationContext"`
	RelationEvent   string          `json:"relationEvent"`
	// RelationEventID identifies the event that caused the change.
	RelationEventID types.ID `json:"relationEventId"`
	// Timestamp is in milliseconds (a numeric string on the wire).
	Timestamp types.Int64 `json:"time"`
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (b BalanceUpdate) Time() time.Time { return millis(int64(b.Timestamp)) }

// Event types, stop kinds and trade types of the stop-order channel.
const (
	// StopEventOpen: the stop order was placed.
	StopEventOpen = "open"
	// StopEventCancel: the stop order was cancelled.
	StopEventCancel = "cancel"
	// StopEventTriggered: the stop price was reached and the order was released to
	// the book; it continues on the order channels. The Spot documentation spells
	// it in capitals (the Futures one in lower case); StopOrderUpdate.IsTriggered
	// ignores the case.
	StopEventTriggered = "TRIGGERED"

	// TradeTypeSpot, TradeTypeMargin and TradeTypeIsolatedMargin are the account
	// kinds a stop order belongs to: Spot, cross margin and isolated margin.
	TradeTypeSpot           = "TRADE"
	TradeTypeMargin         = "MARGIN_TRADE"
	TradeTypeIsolatedMargin = "MARGIN_ISOLATED_TRADE"
)

// StopOrderUpdate is a stop-order event from /spotMarket/advancedOrders, which
// carries every change of the user's stop orders, Spot and Margin alike (see
// TradeType). Type is "open", "cancel" or "TRIGGERED"; after a trigger the order
// continues on the order channels.
//
// Docs: https://www.kucoin.com/docs-new/3470139w0 (Margin uses the same specification: https://www.kucoin.com/docs-new/3470259w0)
type StopOrderUpdate struct {
	PrivateEnvelope
	OrderID    types.ID      `json:"orderId"`
	OrderPrice types.Decimal `json:"orderPrice"`
	// OrderType is "stop".
	OrderType string        `json:"orderType"`
	Side      string        `json:"side"`
	Size      types.Decimal `json:"size"`
	// Stop is the kind of stop order. KuCoin documents the values "loss",
	// "entry", "l_l_o", "l_s_o", "e_l_o", "e_s_o" and "tso" without explaining
	// them.
	Stop      string        `json:"stop"`
	StopPrice types.Decimal `json:"stopPrice"`
	Symbol    string        `json:"symbol"`
	// TradeType is TradeTypeSpot, TradeTypeMargin or TradeTypeIsolatedMargin.
	TradeType string `json:"tradeType"`
	// Type is StopEventOpen, StopEventCancel or StopEventTriggered.
	Type string `json:"type"`
	// CreatedAt is in milliseconds; Timestamp is the push time in nanoseconds.
	CreatedAt types.Int64 `json:"createdAt"`
	Timestamp types.Int64 `json:"ts"`
}

// IsTriggered reports whether the event is the trigger of the stop order. It
// ignores the case of Type.
func (s StopOrderUpdate) IsTriggered() bool { return strings.EqualFold(s.Type, StopEventTriggered) }

// Time converts Timestamp (nanoseconds) to a time.Time.
func (s StopOrderUpdate) Time() time.Time { return nanos(int64(s.Timestamp)) }

// CreatedTime converts CreatedAt (milliseconds) to a time.Time.
func (s StopOrderUpdate) CreatedTime() time.Time { return millis(int64(s.CreatedAt)) }
