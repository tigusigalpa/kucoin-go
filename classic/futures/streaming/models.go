package streaming

import (
	"time"

	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/types"
)

// Numeric conventions: prices, rates, sizes of funds and other values that can
// carry decimals are types.Decimal (exact text, never float64); integer counts
// of contracts, sequences and timestamps are types.Int64. Both accept the JSON
// string and number spellings KuCoin uses interchangeably. Timestamp units are
// stated on every field: KuCoin mixes milliseconds and nanoseconds.

func millis(ms int64) time.Time { return time.UnixMilli(ms) }
func nanos(ns int64) time.Time  { return time.Unix(0, ns) }

// Side values of a trade or an order.
const (
	SideBuy  = "buy"
	SideSell = "sell"
)

// TickerV2 is a best-bid/offer update from /contractMarket/tickerV2:{symbol},
// pushed whenever the order book changes (not necessarily at the top).
//
// Sequence is shared with the level-2 stream: it increases but is not
// continuous here, because this channel only fires when the top of the book
// changes.
//
// Docs: https://www.kucoin.com/docs-new/3470080w0
type TickerV2 struct {
	Symbol       string        `json:"symbol"`
	Sequence     types.Int64   `json:"sequence"`
	BestBidPrice types.Decimal `json:"bestBidPrice"`
	// BestBidSize and BestAskSize are in contracts (lots).
	BestBidSize  types.Int64   `json:"bestBidSize"`
	BestAskPrice types.Decimal `json:"bestAskPrice"`
	BestAskSize  types.Int64   `json:"bestAskSize"`
	// Timestamp is the exchange's push time in nanoseconds.
	Timestamp types.Int64 `json:"ts"`
}

// Time converts Timestamp (nanoseconds) to a time.Time.
func (t TickerV2) Time() time.Time { return nanos(int64(t.Timestamp)) }

// TickerV1 is an update from the deprecated /contractMarket/ticker:{symbol}
// channel, which fires on every match and carries the last trade together with
// the best bid and offer. KuCoin recommends TickerV2 instead.
//
// Docs: https://www.kucoin.com/docs-new/3470081w0
type TickerV1 struct {
	Symbol       string        `json:"symbol"`
	Sequence     types.Int64   `json:"sequence"`
	Side         string        `json:"side"`
	Size         types.Int64   `json:"size"`
	Price        types.Decimal `json:"price"`
	TradeID      types.ID      `json:"tradeId"`
	BestBidPrice types.Decimal `json:"bestBidPrice"`
	BestBidSize  types.Int64   `json:"bestBidSize"`
	BestAskPrice types.Decimal `json:"bestAskPrice"`
	BestAskSize  types.Int64   `json:"bestAskSize"`
	// Timestamp is the push time in nanoseconds.
	Timestamp types.Int64 `json:"ts"`
}

// Time converts Timestamp (nanoseconds) to a time.Time.
func (t TickerV1) Time() time.Time { return nanos(int64(t.Timestamp)) }

// OrderBookDepth is a top-of-book snapshot from /contractMarket/level2Depth5 or
// /contractMarket/level2Depth50. Each push replaces the previous one; no local
// book maintenance is needed. Bids are ordered best first, asks best first. A
// push holds at most five (fifty) levels per side, fewer when the book is thinner.
//
// Docs: https://www.kucoin.com/docs-new/3470083w0 and https://www.kucoin.com/docs-new/3470097w0
type OrderBookDepth struct {
	// Symbol is taken from the topic: the payload itself does not carry it.
	Symbol   string            `json:"-"`
	Sequence types.Int64       `json:"sequence"`
	Bids     []orderbook.Level `json:"bids"`
	Asks     []orderbook.Level `json:"asks"`
	// Timestamp is the push time in milliseconds.
	Timestamp types.Int64 `json:"timestamp"`
	// Ts is documented as a second push timestamp; it holds the same
	// millisecond value as Timestamp.
	Ts types.Int64 `json:"ts"`
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (d OrderBookDepth) Time() time.Time { return millis(int64(d.Timestamp)) }

// OrderBookChange is one price-level update from the level-2 incremental channel
// /contractMarket/level2:{symbol}. Updates must be applied in sequence order to
// a snapshot; SubscribeOrderBook does that, including resynchronisation after a
// gap or reconnect, and is what most applications want.
//
// Docs: https://www.kucoin.com/docs-new/3470082w0
type OrderBookChange struct {
	Symbol string `json:"-"`
	// Sequence increases by exactly one per update (verified against the live
	// feed); a jump means updates were missed.
	Sequence types.Int64 `json:"sequence"`
	// Side, Price and Size come from the wire string "price,side,size". A Size
	// of zero removes the level.
	Side  orderbook.Side `json:"-"`
	Price types.Decimal  `json:"-"`
	Size  types.Decimal  `json:"-"`
	// Timestamp is the push time in milliseconds.
	Timestamp types.Int64 `json:"timestamp"`
	// Change is the raw "price,side,size" string.
	Change string `json:"change"`
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (c OrderBookChange) Time() time.Time { return millis(int64(c.Timestamp)) }

// Interval is a candle interval of the kline channel.
type Interval string

// Candle intervals supported by /contractMarket/limitCandle.
const (
	Interval1Min   Interval = "1min"
	Interval3Min   Interval = "3min"
	Interval5Min   Interval = "5min"
	Interval15Min  Interval = "15min"
	Interval30Min  Interval = "30min"
	Interval1Hour  Interval = "1hour"
	Interval2Hour  Interval = "2hour"
	Interval4Hour  Interval = "4hour"
	Interval8Hour  Interval = "8hour"
	Interval12Hour Interval = "12hour"
	Interval1Day   Interval = "1day"
	Interval1Week  Interval = "1week"
	Interval1Month Interval = "1month"
)

// Valid reports whether the interval is one KuCoin supports.
func (i Interval) Valid() bool {
	switch i {
	case Interval1Min, Interval3Min, Interval5Min, Interval15Min, Interval30Min, Interval1Hour,
		Interval2Hour, Interval4Hour, Interval8Hour, Interval12Hour, Interval1Day, Interval1Week, Interval1Month:
		return true
	}
	return false
}

// Kline is a candle update from /contractMarket/limitCandle:{symbol}_{type}.
//
// On the wire the candle is an array in the order
// [start time, open, close, high, low, turnover, volume]. Close comes before
// high and low, unlike the REST klines, and the last two elements are in the
// opposite order to the one the documentation gives ("volume, turnover"). Both
// were checked against the REST kline of the same minute: element 5 is the
// turnover in the quote currency, element 6 the traded size in contracts (lots).
//
// The last push of a candle period is its final state; during the period every
// push replaces the previous one.
//
// Docs: https://www.kucoin.com/docs-new/3470086w0
type Kline struct {
	Symbol   string   `json:"symbol"`
	Interval Interval `json:"-"`
	// StartTime is the start of the candle period in Unix seconds.
	StartTime int64         `json:"-"`
	Open      types.Decimal `json:"-"`
	Close     types.Decimal `json:"-"`
	High      types.Decimal `json:"-"`
	Low       types.Decimal `json:"-"`
	// Volume is the traded size in contracts (lots).
	Volume types.Decimal `json:"-"`
	// Turnover is the traded amount in the quote currency.
	Turnover types.Decimal `json:"-"`
	// Timestamp is the push time in milliseconds.
	Timestamp types.Int64 `json:"time"`
	// Candles is the raw array in wire order (see above).
	Candles []types.Decimal `json:"candles"`
}

// Start converts StartTime to a time.Time.
func (k Kline) Start() time.Time { return time.Unix(k.StartTime, 0) }

// Time converts Timestamp (milliseconds) to a time.Time.
func (k Kline) Time() time.Time { return millis(int64(k.Timestamp)) }

// Trade is a match from /contractMarket/execution:{symbol}.
//
// Docs: https://www.kucoin.com/docs-new/3470084w0
type Trade struct {
	Symbol   string      `json:"symbol"`
	Sequence types.Int64 `json:"sequence"`
	// Side is the taker's side: "buy" or "sell".
	Side string `json:"side"`
	// Size is in contracts (lots).
	Size         types.Int64   `json:"size"`
	Price        types.Decimal `json:"price"`
	TakerOrderID types.ID      `json:"takerOrderId"`
	MakerOrderID types.ID      `json:"makerOrderId"`
	TradeID      types.ID      `json:"tradeId"`
	// Timestamp is the matching-engine execution time in nanoseconds.
	Timestamp types.Int64 `json:"ts"`
}

// Time converts Timestamp (nanoseconds) to a time.Time.
func (t Trade) Time() time.Time { return nanos(int64(t.Timestamp)) }

// Subjects of the instrument channel.
const (
	// SubjectMarkIndexPrice marks an InstrumentEvent carrying mark and index
	// price (pushed every second).
	SubjectMarkIndexPrice = "mark.index.price"
	// SubjectFundingRate marks an InstrumentEvent carrying the funding rate
	// (pushed every minute).
	SubjectFundingRate = "funding.rate"
)

// InstrumentEvent is an update from /contract/instrument:{symbol}. The channel
// carries two kinds of update told apart by Subject: SubjectMarkIndexPrice
// fills MarkPrice and IndexPrice, SubjectFundingRate fills FundingRate; the
// other fields stay empty.
//
// Docs: https://www.kucoin.com/docs-new/3470087w0
type InstrumentEvent struct {
	// Symbol is taken from the topic.
	Symbol  string `json:"-"`
	Subject string `json:"-"`

	MarkPrice   types.Decimal `json:"markPrice"`
	IndexPrice  types.Decimal `json:"indexPrice"`
	FundingRate types.Decimal `json:"fundingRate"`
	// Granularity is the update interval in milliseconds.
	Granularity types.Int64 `json:"granularity"`
	// Period is sent with the funding-rate push (observed value 1) although the
	// documentation does not mention it. What it counts is not documented, so it
	// is passed through unchanged; zero when absent.
	Period types.Int64 `json:"period"`
	// Timestamp is in milliseconds.
	Timestamp types.Int64 `json:"timestamp"`
}

// IsMarkIndexPrice reports whether the event carries mark and index price.
func (e InstrumentEvent) IsMarkIndexPrice() bool { return e.Subject == SubjectMarkIndexPrice }

// IsFundingRate reports whether the event carries the funding rate.
func (e InstrumentEvent) IsFundingRate() bool { return e.Subject == SubjectFundingRate }

// Time converts Timestamp (milliseconds) to a time.Time.
func (e InstrumentEvent) Time() time.Time { return millis(int64(e.Timestamp)) }

// Subjects of the funding-settlement channel.
const (
	SubjectFundingBegin = "funding.begin"
	SubjectFundingEnd   = "funding.end"
)

// FundingSettlement is a funding-fee settlement notice from the global
// /contract/announcement topic, sent when settlement starts
// (SubjectFundingBegin) and ends (SubjectFundingEnd), every eight hours.
//
// The documentation names the topic "/contract/announcement:{symbol}" in its
// title but subscribes to "/contract/announcement" in the example and carries
// the symbol in each message; this SDK subscribes to the global topic.
//
// Docs: https://www.kucoin.com/docs-new/3470088w0
type FundingSettlement struct {
	Subject string `json:"-"`
	Symbol  string `json:"symbol"`
	// FundingTime is the settlement time in milliseconds.
	FundingTime types.Int64   `json:"fundingTime"`
	FundingRate types.Decimal `json:"fundingRate"`
	// Timestamp is in milliseconds.
	Timestamp types.Int64 `json:"timestamp"`
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (f FundingSettlement) Time() time.Time { return millis(int64(f.Timestamp)) }

// SymbolSnapshot is the 24-hour statistics push from
// /contractMarket/snapshot:{symbol}, sent every 5 seconds.
//
// Docs: https://www.kucoin.com/docs-new/3470089w0
type SymbolSnapshot struct {
	Symbol             string        `json:"symbol"`
	HighPrice          types.Decimal `json:"highPrice"`
	LastPrice          types.Decimal `json:"lastPrice"`
	LowPrice           types.Decimal `json:"lowPrice"`
	Price24HoursBefore types.Decimal `json:"price24HoursBefore"`
	PriceChg           types.Decimal `json:"priceChg"`
	PriceChgPct        types.Decimal `json:"priceChgPct"`
	// Turnover is the 24-hour turnover in the quote currency.
	Turnover types.Decimal `json:"turnover"`
	// Volume is the 24-hour traded size in the base currency, not in contracts:
	// 3938.189 for XBTUSDTM, whose contract is 0.001 BTC, is 3,938,189 contracts.
	Volume types.Decimal `json:"volume"`
	// FundingRate is the current funding rate. The live feed sends it with every
	// snapshot although the documentation does not list it; empty when absent.
	FundingRate types.Decimal `json:"fundingRate"`
	// Timestamp is in nanoseconds.
	Timestamp types.Int64 `json:"ts"`
}

// Time converts Timestamp (nanoseconds) to a time.Time.
func (s SymbolSnapshot) Time() time.Time { return nanos(int64(s.Timestamp)) }

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

// Order status, order type and related values of the orders channel.
const (
	OrderStatusOpen  = "open"
	OrderStatusMatch = "match"
	OrderStatusDone  = "done"

	OrderEventOpen     = "open"
	OrderEventMatch    = "match"
	OrderEventUpdate   = "update"
	OrderEventFilled   = "filled"
	OrderEventCanceled = "canceled"

	MarginModeIsolated = "ISOLATED"
	MarginModeCross    = "CROSS"
)

// OrderChange is an order event from /contractMarket/tradeOrders (all symbols)
// or /contractMarket/tradeOrders:{symbol}. Type says what happened (open, match,
// update, filled, canceled) and Status the resulting order state (open, match,
// done); a maker match has Status "open" and Type "match", a taker match Status
// "match" and Type "match". TradeType is "trade", "liquid" or "adl".
//
// Docs: https://www.kucoin.com/docs-new/3470090w0
type OrderChange struct {
	PrivateEnvelope
	Symbol     string `json:"symbol"`
	MarginMode string `json:"marginMode"`
	Status     string `json:"status"`
	Type       string `json:"type"`
	Side       string `json:"side"`
	TradeType  string `json:"tradeType"`
	// OrderType is "limit" or "market"; empty on some events.
	OrderType string `json:"orderType"`
	// FeeType is "takerFee" or "makerFee"; Liquidity is "taker" or "maker".
	FeeType   string   `json:"feeType"`
	Liquidity string   `json:"liquidity"`
	OrderID   types.ID `json:"orderId"`
	ClientOid string   `json:"clientOid"`
	TradeID   types.ID `json:"tradeId"`
	// PositionSide is "BOTH", "LONG" or "SHORT".
	PositionSide string        `json:"positionSide"`
	Price        types.Decimal `json:"price"`
	Size         types.Decimal `json:"size"`
	FilledSize   types.Decimal `json:"filledSize"`
	CanceledSize types.Decimal `json:"canceledSize"`
	// AllCanceledSize is the total cancelled size.
	AllCanceledSize types.Decimal `json:"allCanceledSize"`
	RemainSize      types.Decimal `json:"remainSize"`
	// MatchSize and MatchPrice are set when Type is "match".
	MatchSize  types.Decimal `json:"matchSize"`
	MatchPrice types.Decimal `json:"matchPrice"`
	// OldSize is the size before an order update.
	OldSize types.Decimal `json:"oldSize"`
	// OrderTime is when the futures service generated the order ID, in
	// nanoseconds.
	OrderTime types.Int64 `json:"orderTime"`
	// Timestamp is when the matching engine received the message, in
	// nanoseconds.
	Timestamp types.Int64 `json:"ts"`
}

// Time converts Timestamp (nanoseconds) to a time.Time.
func (o OrderChange) Time() time.Time { return nanos(int64(o.Timestamp)) }

// StopOrderEvent is a stop-order event from /contractMarket/advancedOrders. Type
// is "open", "triggered" or "cancel"; after a trigger the order continues on the
// orders channel.
//
// Docs: https://www.kucoin.com/docs-new/3470091w0
type StopOrderEvent struct {
	PrivateEnvelope
	// ID is the event ID.
	ID         string        `json:"-"`
	MarginMode string        `json:"marginMode"`
	OrderID    types.ID      `json:"orderId"`
	OrderPrice types.Decimal `json:"orderPrice"`
	OrderType  string        `json:"orderType"`
	Side       string        `json:"side"`
	Size       types.Decimal `json:"size"`
	// Stop is "up" or "down".
	Stop          string        `json:"stop"`
	StopPrice     types.Decimal `json:"stopPrice"`
	StopPriceType string        `json:"stopPriceType"`
	Symbol        string        `json:"symbol"`
	Type          string        `json:"type"`
	PositionSide  string        `json:"positionSide"`
	// CreatedAt is in milliseconds; Timestamp is in nanoseconds.
	CreatedAt types.Int64 `json:"createdAt"`
	Timestamp types.Int64 `json:"ts"`
}

// Subjects of the wallet-balance channel.
const (
	// SubjectWalletBalanceChange carries the full per-currency balance. It
	// replaces the three deprecated subjects below once the account has switched
	// to cross margin.
	SubjectWalletBalanceChange = "walletBalance.change"
	// SubjectOrderMarginChange, SubjectAvailableBalanceChange and
	// SubjectWithdrawHoldChange are deprecated by KuCoin and stop being pushed
	// after the first switch from isolated to cross margin mode.
	SubjectOrderMarginChange      = "orderMargin.change"
	SubjectAvailableBalanceChange = "availableBalance.change"
	SubjectWithdrawHoldChange     = "withdrawHold.change"
)

// BalanceEvent is an account-balance update from /contractAccount/wallet. For
// SubjectWalletBalanceChange the margin fields are filled; the deprecated
// subjects fill only OrderMargin, AvailableBalance/HoldBalance or WithdrawHold.
//
// Docs: https://www.kucoin.com/docs-new/3470092w0
type BalanceEvent struct {
	PrivateEnvelope
	ID string `json:"-"`

	Currency                 string        `json:"currency"`
	Equity                   types.Decimal `json:"equity"`
	WalletBalance            types.Decimal `json:"walletBalance"`
	AvailableBalance         types.Decimal `json:"availableBalance"`
	HoldBalance              types.Decimal `json:"holdBalance"`
	TotalCrossMargin         types.Decimal `json:"totalCrossMargin"`
	CrossPosMargin           types.Decimal `json:"crossPosMargin"`
	CrossOrderMargin         types.Decimal `json:"crossOrderMargin"`
	CrossUnPnl               types.Decimal `json:"crossUnPnl"`
	IsolatedPosMargin        types.Decimal `json:"isolatedPosMargin"`
	IsolatedOrderMargin      types.Decimal `json:"isolatedOrderMargin"`
	IsolatedFundingFeeMargin types.Decimal `json:"isolatedFundingFeeMargin"`
	IsolatedUnPnl            types.Decimal `json:"isolatedUnPnl"`
	// MaxWithdrawAmount is present on live pushes although not in the schema.
	MaxWithdrawAmount types.Decimal `json:"maxWithdrawAmount"`
	// Version increases with every change of the account.
	Version types.ID `json:"version"`
	// Timestamp is the last modification time in milliseconds.
	Timestamp types.Int64 `json:"timestamp"`

	// Fields of the deprecated subjects.
	OrderMargin  types.Decimal `json:"orderMargin"`
	WithdrawHold types.Decimal `json:"withdrawHold"`
}

// IsDeprecatedSubject reports whether the event is one of the three balance
// subjects KuCoin has deprecated.
func (b BalanceEvent) IsDeprecatedSubject() bool {
	switch b.Subject {
	case SubjectOrderMarginChange, SubjectAvailableBalanceChange, SubjectWithdrawHoldChange:
		return true
	}
	return false
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (b BalanceEvent) Time() time.Time { return millis(int64(b.Timestamp)) }

// Subjects of the position channels.
const (
	// SubjectPositionChange is a change of an open position.
	SubjectPositionChange = "position.change"
	// SubjectPositionSettlement is a funding-fee settlement of a position.
	SubjectPositionSettlement = "position.settlement"
	// SubjectPositionAdjustRiskLimit reports the outcome of an isolated-margin
	// risk-limit adjustment.
	SubjectPositionAdjustRiskLimit = "position.adjustRiskLimit"
)

// PositionEvent is a position update from /contract/positionAll (all symbols) or
// /contract/position:{symbol}. Subject says which of three kinds it is:
// SubjectPositionChange fills the position fields (ChangeReason says why),
// SubjectPositionSettlement fills Qty, FundingFee, FundingRate, FundingTime,
// MarkPrice and Timestamp, SubjectPositionAdjustRiskLimit fills Success, Msg and
// RiskLimitLevel. Fields not used by a kind stay empty.
//
// Docs: https://www.kucoin.com/docs-new/3470093w0
type PositionEvent struct {
	PrivateEnvelope

	Symbol     string `json:"symbol"`
	MarginMode string `json:"marginMode"`
	CrossMode  bool   `json:"crossMode"`
	// PositionSide is "BOTH" in one-way mode.
	PositionSide string `json:"positionSide"`
	// ChangeReason is "marginChange", "positionChange", "liquidation",
	// "autoAppendMarginStatusChange", "adl" or "changeRiskLimit" (observed live).
	ChangeReason     string        `json:"changeReason"`
	DelevPercentage  types.Decimal `json:"delevPercentage"`
	OpeningTimestamp types.Int64   `json:"openingTimestamp"` // ms
	CurrentTimestamp types.Int64   `json:"currentTimestamp"` // ms
	// CurrentQty is the signed position size in contracts.
	CurrentQty        types.Int64   `json:"currentQty"`
	CurrentCost       types.Decimal `json:"currentCost"`
	CurrentComm       types.Decimal `json:"currentComm"`
	UnrealisedCost    types.Decimal `json:"unrealisedCost"`
	UnrealisedPnl     types.Decimal `json:"unrealisedPnl"`
	UnrealisedPnlPcnt types.Decimal `json:"unrealisedPnlPcnt"`
	UnrealisedRoePcnt types.Decimal `json:"unrealisedRoePcnt"`
	RealisedCost      types.Decimal `json:"realisedCost"`
	RealisedGrossCost types.Decimal `json:"realisedGrossCost"`
	RealisedGrossPnl  types.Decimal `json:"realisedGrossPnl"`
	RealisedPnl       types.Decimal `json:"realisedPnl"`
	IsOpen            bool          `json:"isOpen"`
	MarkPrice         types.Decimal `json:"markPrice"`
	MarkValue         types.Decimal `json:"markValue"`
	PosCost           types.Decimal `json:"posCost"`
	PosInit           types.Decimal `json:"posInit"`
	PosMargin         types.Decimal `json:"posMargin"`
	PosMaint          types.Decimal `json:"posMaint"`
	AvgEntryPrice     types.Decimal `json:"avgEntryPrice"`
	LiquidationPrice  types.Decimal `json:"liquidationPrice"`
	BankruptPrice     types.Decimal `json:"bankruptPrice"`
	SettleCurrency    string        `json:"settleCurrency"`
	Leverage          types.Decimal `json:"leverage"`
	MaintMarginReq    types.Decimal `json:"maintMarginReq"`
	RiskLimitLevel    types.Int64   `json:"riskLimitLevel"`

	// Isolated-margin only fields.
	AutoDeposit  bool          `json:"autoDeposit"`
	RiskLimit    types.Decimal `json:"riskLimit"`
	RealLeverage types.Decimal `json:"realLeverage"`
	PosCross     types.Decimal `json:"posCross"`
	PosComm      types.Decimal `json:"posComm"`
	PosLoss      types.Decimal `json:"posLoss"`
	PosFunding   types.Decimal `json:"posFunding"`
	MaintMargin  types.Decimal `json:"maintMargin"`
	Tax          types.Decimal `json:"tax"`
	DealComm     types.Decimal `json:"dealComm"`
	AggRate      types.Decimal `json:"aggRate"`

	// Fields of the funding-settlement kind.
	FundingTime types.Int64   `json:"fundingTime"` // ms
	Qty         types.Int64   `json:"qty"`
	FundingRate types.Decimal `json:"fundingRate"`
	FundingFee  types.Decimal `json:"fundingFee"`
	// Timestamp is the settlement time in nanoseconds.
	Timestamp types.Int64 `json:"ts"`

	// Fields of the risk-limit-adjustment kind.
	Success bool   `json:"success"`
	Msg     string `json:"msg"`
}

// IsChange, IsSettlement and IsRiskLimitAdjustment tell the three kinds apart.
func (p PositionEvent) IsChange() bool     { return p.Subject == SubjectPositionChange }
func (p PositionEvent) IsSettlement() bool { return p.Subject == SubjectPositionSettlement }
func (p PositionEvent) IsRiskLimitAdjustment() bool {
	return p.Subject == SubjectPositionAdjustRiskLimit
}

// MarginModeEvent is a margin-mode change from /contract/marginMode. Modes maps
// each affected symbol to "CROSS" or "ISOLATED".
//
// Docs: https://www.kucoin.com/docs-new/3470095w0
type MarginModeEvent struct {
	PrivateEnvelope
	Modes map[string]string
}

// CrossLeverageEvent is a leverage change of cross-margin futures from
// /contract/crossLeverage. Leverages maps each affected symbol to its new
// leverage.
//
// Docs: https://www.kucoin.com/docs-new/3470096w0
type CrossLeverageEvent struct {
	PrivateEnvelope
	Leverages map[string]types.Decimal
}
