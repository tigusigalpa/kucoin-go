package market

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/types"
)

// Granularity is a kline interval in minutes, the unit the Futures kline
// endpoint expects on the wire (1, 3, 5, 15, 30, 60, 120, 240, 480, 720, 1440,
// 10080, 43200). Classic Spot, by contrast, names its intervals as strings such
// as "1hour".
//
// KuCoin's documentation lists every value above except 3 and 43200. The live
// endpoint accepts both (and rejects anything else with code 300000), and they
// are the 3min and 1month candles of the WebSocket kline channel, so they are
// offered here too.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-klines
type Granularity int

// Granularity constants are the kline intervals accepted by GetKlines.
const (
	Granularity1Min Granularity = 1
	// Granularity3Min and Granularity1Month are accepted by the live endpoint
	// although the documentation does not list them.
	Granularity3Min   Granularity = 3
	Granularity5Min   Granularity = 5
	Granularity15Min  Granularity = 15
	Granularity30Min  Granularity = 30
	Granularity1Hour  Granularity = 60
	Granularity2Hour  Granularity = 120
	Granularity4Hour  Granularity = 240
	Granularity8Hour  Granularity = 480
	Granularity12Hour Granularity = 720
	Granularity1Day   Granularity = 1440
	Granularity1Week  Granularity = 10080
	// Granularity1Month is a calendar month.
	Granularity1Month Granularity = 43200
)

// Valid reports whether g is one of the granularities KuCoin accepts.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-klines
func (g Granularity) Valid() bool {
	switch g {
	case Granularity1Min, Granularity3Min, Granularity5Min, Granularity15Min, Granularity30Min,
		Granularity1Hour, Granularity2Hour, Granularity4Hour, Granularity8Hour,
		Granularity12Hour, Granularity1Day, Granularity1Week, Granularity1Month:
		return true
	}
	return false
}

// String returns a short label such as "15min", "4hour" or "1month", or
// "Granularity(n)" for a value KuCoin does not accept.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-klines
func (g Granularity) String() string {
	switch g {
	case Granularity1Min:
		return "1min"
	case Granularity3Min:
		return "3min"
	case Granularity5Min:
		return "5min"
	case Granularity15Min:
		return "15min"
	case Granularity30Min:
		return "30min"
	case Granularity1Hour:
		return "1hour"
	case Granularity2Hour:
		return "2hour"
	case Granularity4Hour:
		return "4hour"
	case Granularity8Hour:
		return "8hour"
	case Granularity12Hour:
		return "12hour"
	case Granularity1Day:
		return "1day"
	case Granularity1Week:
		return "1week"
	case Granularity1Month:
		return "1month"
	}
	return "Granularity(" + strconv.Itoa(int(g)) + ")"
}

// ContractType is the kind of a Futures contract (Symbol.Type). KuCoin
// documents two values but the field stays an open string so a value added
// later still decodes.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol
type ContractType string

// Documented contract types. KuCoin does not explain the codes. Observed on the
// live API: perpetual swaps carry FFWCSX and the dated (expiring) future
// XBTMZ26 carries FFICSX.
const (
	ContractTypeFFWCSX ContractType = "FFWCSX"
	ContractTypeFFICSX ContractType = "FFICSX"
)

// ContractStatus is the lifecycle status of a contract (Symbol.Status). The
// field stays an open string so a value added later still decodes.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol
type ContractStatus string

// Documented contract statuses.
const (
	ContractStatusInit         ContractStatus = "Init"
	ContractStatusOpen         ContractStatus = "Open"
	ContractStatusBeingSettled ContractStatus = "BeingSettled"
	ContractStatusSettled      ContractStatus = "Settled"
	ContractStatusPaused       ContractStatus = "Paused"
	ContractStatusClosed       ContractStatus = "Closed"
	ContractStatusCancelOnly   ContractStatus = "CancelOnly"
)

// MarketStage is the trading stage of a contract (Symbol.MarketStage). The
// field stays an open string so a value added later still decodes.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol
type MarketStage string

// Documented market stages.
const (
	// MarketStageNormal is standard trading.
	MarketStageNormal MarketStage = "NORMAL"
	// MarketStagePreMarket is the pre-market phase, before the contract turns
	// into a standard perpetual (see Symbol.PreMarketToPerpDate).
	MarketStagePreMarket MarketStage = "PRE_MARKET"
)

// MarketType identifies the market category of the underlying
// (Symbol.MarketType). KuCoin says new values may be added when more
// equity-related markets are supported, so the field stays an open string.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol
type MarketType string

// Documented market types.
const (
	// MarketTypeCrypto is the cryptocurrency market.
	MarketTypeCrypto MarketType = "CRYPTO"
	// MarketTypeNasdaq is the Nasdaq-related equity and stock-index perpetual
	// market.
	MarketTypeNasdaq MarketType = "NASDAQ"
)

// AssetClass is the asset class of a contract's base currency
// (Symbol.AssetClass). The field stays an open string so a value added later
// still decodes.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol
type AssetClass string

// Documented asset classes.
const (
	AssetClassCrypto    AssetClass = "CRYPTO"
	AssetClassMetal     AssetClass = "METAL"
	AssetClassCommodity AssetClass = "COMMODITY"
	AssetClassStock     AssetClass = "STOCK"
)

// SubMarketType is the sub-market of an asset class (Symbol.SubMarketType); it
// is empty (JSON null) when not applicable, which is the case for every
// crypto, metal and commodity contract. The field stays an open string so a
// value added later still decodes.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol
type SubMarketType string

// Documented sub-market types.
const (
	SubMarketTypeUSStock SubMarketType = "US.STOCK"
	SubMarketTypeKRStock SubMarketType = "KR.STOCK"
	SubMarketTypeHKStock SubMarketType = "HK.STOCK"
	SubMarketTypeJPStock SubMarketType = "JP.STOCK"
)

// Side is the taker side of a trade, that is, the side of the order that was
// matched against resting orders on the book.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-ticker
type Side string

// Trade sides.
const (
	SideBuy  Side = "buy"
	SideSell Side = "sell"
)

// ServiceState is the state of the Futures trading service
// (ServiceStatus.Status). The field stays an open string so a value added later
// still decodes.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-service-status
type ServiceState string

// Documented service states.
const (
	// ServiceStateOpen is normal trading.
	ServiceStateOpen ServiceState = "open"
	// ServiceStateClose is stopped trading or maintenance.
	ServiceStateClose ServiceState = "close"
	// ServiceStateCancelOnly allows cancelling orders but not placing them.
	ServiceStateCancelOnly ServiceState = "cancelonly"
)

// Symbol is one Classic Futures contract: its static specification (tick and
// lot size, multiplier, fee rates, risk parameters, funding configuration) and
// a live statistics snapshot (mark, index and last price, 24-hour range, open
// interest, next funding time). GetSymbol returns one and GetAllSymbols one per
// contract; both endpoints share this schema.
//
// Every price, size, rate and ratio is a types.Decimal holding the exact text
// KuCoin sent, so the Java exponent form (2.0E-4), bare integers and literals
// such as 490.0 survive unchanged; compare with Decimal.Cmp, never by string
// equality. Counts, timestamps (milliseconds) and granularities (milliseconds)
// are types.Int64.
//
// KuCoin sends JSON null for fields that do not apply to a contract (for
// example the funding fields of the dated future XBTMZ26, or ExpireDate on a
// perpetual). A null decodes to the zero value: 0 for a types.Int64 field and
// the empty Decimal or string otherwise; use Decimal.IsEmpty to tell "no value"
// from the number zero.
//
// KuCoin's field table lists 84 fields and this struct maps all of them with
// the exact documented JSON names. Observed on the live API and not described
// by the docs: DailyInterestRate and LastTimeFundingRate are returned (they
// appear in the docs' example response but not in the table), the funding
// fields are null for dated futures, and NextFundingRateTime is a countdown
// rather than a timestamp.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol
type Symbol struct {
	Symbol              string       `json:"symbol"`              // contract symbol, e.g. XBTUSDTM
	DisplaySymbol       string       `json:"displaySymbol"`       // symbol shown in the web/app UI
	RootSymbol          string       `json:"rootSymbol"`          // contract group, e.g. USDT
	Type                ContractType `json:"type"`                // FFWCSX or FFICSX
	FirstOpenDate       types.Int64  `json:"firstOpenDate"`       // milliseconds
	ExpireDate          types.Int64  `json:"expireDate"`          // milliseconds; 0 (JSON null) means it never expires
	SettleDate          types.Int64  `json:"settleDate"`          // milliseconds; 0 (JSON null) means automatic settlement is not supported
	BaseCurrency        string       `json:"baseCurrency"`        // e.g. XBT
	DisplayBaseCurrency string       `json:"displayBaseCurrency"` // base currency shown in the web/app UI
	QuoteCurrency       string       `json:"quoteCurrency"`       // e.g. USDT
	SettleCurrency      string       `json:"settleCurrency"`      // currency used to clear and settle trades

	// MaxOrderQty is the maximum order quantity in contracts (lots).
	MaxOrderQty types.Int64 `json:"maxOrderQty"`
	// MarketMaxOrderQty is the maximum quantity of a market order in contracts.
	// KuCoin documents it as a number on GetSymbol but as an integer on
	// GetAllSymbols and the live value is always an integer, so it is kept as
	// a Decimal.
	MarketMaxOrderQty  types.Decimal `json:"marketMaxOrderQty"`
	MaxPrice           types.Decimal `json:"maxPrice"`           // maximum order price
	LotSize            types.Int64   `json:"lotSize"`            // minimum lot size in contracts
	TickSize           types.Decimal `json:"tickSize"`           // minimum price change
	IndexPriceTickSize types.Decimal `json:"indexPriceTickSize"` // tick size of the index price
	// Multiplier is the base-currency amount per contract (lot), for example
	// 0.001 for XBTUSDTM. Inverse (coin-margined) contracts report -1 on the
	// live API: each of their contracts is worth 1 USD.
	Multiplier     types.Decimal `json:"multiplier"`
	InitialMargin  types.Decimal `json:"initialMargin"`  // initial margin requirement as a ratio (0.008 is 0.8%)
	MaintainMargin types.Decimal `json:"maintainMargin"` // maintenance margin requirement as a ratio
	// MaxRiskLimit, MinRiskLimit and RiskStep are the risk-limit bounds and
	// increment. KuCoin documents the unit as XBT, but the live values (250000
	// for USDT-margined XBTUSDTM, 5 for XBT-margined XBTUSDM) indicate the
	// contract's settlement currency.
	MaxRiskLimit  types.Decimal `json:"maxRiskLimit"`
	MinRiskLimit  types.Decimal `json:"minRiskLimit"`
	RiskStep      types.Decimal `json:"riskStep"`
	MakerFeeRate  types.Decimal `json:"makerFeeRate"`  // ratio (2.0E-4 is 0.02%)
	TakerFeeRate  types.Decimal `json:"takerFeeRate"`  // ratio (6.0E-4 is 0.06%)
	TakerFixFee   types.Decimal `json:"takerFixFee"`   // deprecated by KuCoin
	MakerFixFee   types.Decimal `json:"makerFixFee"`   // deprecated by KuCoin
	SettlementFee types.Decimal `json:"settlementFee"` // null on every live contract
	IsDeleverage  bool          `json:"isDeleverage"`  // auto-deleveraging (ADL) enabled
	IsQuanto      bool          `json:"isQuanto"`      // deprecated by KuCoin
	IsInverse     bool          `json:"isInverse"`     // reverse (coin-margined) contract

	MarkMethod         string         `json:"markMethod"`         // marking method, documented value FairPrice
	FairMethod         string         `json:"fairMethod"`         // fair-price method, documented value FundingRate; empty (null) for dated futures
	FundingBaseSymbol  string         `json:"fundingBaseSymbol"`  // base-currency interest symbol, e.g. .XBTINT8H; empty (null) for dated futures
	FundingQuoteSymbol string         `json:"fundingQuoteSymbol"` // quote-currency interest symbol, e.g. .USDTINT8H; empty (null) for dated futures
	FundingRateSymbol  string         `json:"fundingRateSymbol"`  // funding-rate symbol, e.g. .XBTUSDTMFPI8H; empty (null) for dated futures
	IndexSymbol        string         `json:"indexSymbol"`        // spot index symbol, e.g. .KXBTUSDT
	SettlementSymbol   string         `json:"settlementSymbol"`   // settlement symbol; empty (or null) unless the contract expires
	Status             ContractStatus `json:"status"`

	FundingFeeRate          types.Decimal `json:"fundingFeeRate"`          // current funding rate; empty (null) for dated futures
	PredictedFundingFeeRate types.Decimal `json:"predictedFundingFeeRate"` // null on every live contract
	// FundingRateGranularity is the configured funding interval in
	// milliseconds (28800000 is 8 hours); the docs describe it in hours. A
	// change takes effect from the next cycle, see
	// EffectiveFundingRateCycleStartTime.
	FundingRateGranularity types.Int64 `json:"fundingRateGranularity"`
	// EffectiveFundingRateCycleStartTime is the time in milliseconds from which
	// FundingRateGranularity is effective; null on a few live contracts.
	EffectiveFundingRateCycleStartTime types.Int64 `json:"effectiveFundingRateCycleStartTime"`
	// CurrentFundingRateGranularity is the currently effective funding interval
	// in milliseconds; null on a few live contracts.
	CurrentFundingRateGranularity types.Int64   `json:"currentFundingRateGranularity"`
	FundingRateCap                types.Decimal `json:"fundingRateCap"`   // upper limit of the funding rate
	FundingRateFloor              types.Decimal `json:"fundingRateFloor"` // lower limit of the funding rate
	Period                        types.Int64   `json:"period"`           // funding collection mode, deprecated by KuCoin (always current period)

	OpenInterest  types.Decimal `json:"openInterest"`  // open interest in contracts; the only numeric field KuCoin sends as a JSON string
	TurnoverOf24h types.Decimal `json:"turnoverOf24h"` // 24-hour turnover
	VolumeOf24h   types.Decimal `json:"volumeOf24h"`   // 24-hour volume
	MarkPrice     types.Decimal `json:"markPrice"`
	IndexPrice    types.Decimal `json:"indexPrice"`
	// LastTradePrice is empty (null) when the contract has not traded yet.
	LastTradePrice types.Decimal `json:"lastTradePrice"`
	// NextFundingRateTime is the number of milliseconds left until the next
	// funding settlement, a countdown and not a timestamp (the docs only say
	// "milliseconds"). Use NextFundingRateDateTime for the timestamp.
	NextFundingRateTime types.Int64 `json:"nextFundingRateTime"`
	// NextFundingRateDateTime is the next funding settlement as a Unix
	// timestamp in milliseconds.
	NextFundingRateDateTime types.Int64 `json:"nextFundingRateDateTime"`
	MaxLeverage             types.Int64 `json:"maxLeverage"`
	SourceExchanges         []string    `json:"sourceExchanges"` // exchanges feeding the index price

	PremiumsSymbol1M     string `json:"premiumsSymbol1M"`     // 1-minute premium-index symbol, e.g. .XBTUSDTMPI
	PremiumsSymbol8H     string `json:"premiumsSymbol8H"`     // 8-hour premium-index symbol, e.g. .XBTUSDTMPI8H
	FundingBaseSymbol1M  string `json:"fundingBaseSymbol1M"`  // 1-minute base-currency interest symbol, e.g. .XBTINT; empty (null) for dated futures
	FundingQuoteSymbol1M string `json:"fundingQuoteSymbol1M"` // 1-minute quote-currency interest symbol, e.g. .USDTINT; empty (null) for dated futures

	LowPrice    types.Decimal `json:"lowPrice"`    // 24-hour low
	HighPrice   types.Decimal `json:"highPrice"`   // 24-hour high
	PriceChgPct types.Decimal `json:"priceChgPct"` // 24-hour price change as a ratio (-0.0022 is -0.22%)
	PriceChg    types.Decimal `json:"priceChg"`    // 24-hour price change

	// K, M, F, MmrLimit and MmrLevConstant are parameters of KuCoin's
	// margin-requirement model; the docs describe none of them.
	K              types.Decimal `json:"k"`
	M              types.Decimal `json:"m"`
	F              types.Decimal `json:"f"`
	MmrLimit       types.Decimal `json:"mmrLimit"`
	MmrLevConstant types.Decimal `json:"mmrLevConstant"`
	SupportCross   bool          `json:"supportCross"` // cross margin supported

	BuyLimit  types.Decimal `json:"buyLimit"`  // highest buy price currently allowed
	SellLimit types.Decimal `json:"sellLimit"` // lowest sell price currently allowed

	// AdjustK, AdjustM, AdjustMmrLevConstant and AdjustActiveTime describe a
	// pending change of K, M and MmrLevConstant and when it takes effect
	// (milliseconds). All are null (empty / 0) when nothing is pending.
	AdjustK              types.Decimal `json:"adjustK"`
	AdjustM              types.Decimal `json:"adjustM"`
	AdjustMmrLevConstant types.Decimal `json:"adjustMmrLevConstant"`
	AdjustActiveTime     types.Int64   `json:"adjustActiveTime"`
	// CrossRiskLimit is the hard cap of the position limit in cross-margin mode:
	// the smaller of the system cap and the maximum openable amount.
	CrossRiskLimit types.Decimal `json:"crossRiskLimit"`
	MarketStage    MarketStage   `json:"marketStage"`
	// PreMarketToPerpDate is when a pre-market contract becomes a standard
	// perpetual, in milliseconds; 0 (JSON null) when not scheduled.
	PreMarketToPerpDate types.Int64 `json:"preMarketToPerpDate"`
	// OrderPriceRange is the allowed deviation of an order price from the mark
	// price as a ratio: BuyLimit is about MarkPrice * (1 + OrderPriceRange) and
	// SellLimit about MarkPrice * (1 - OrderPriceRange).
	OrderPriceRange types.Decimal `json:"orderPriceRange"`
	MarketType      MarketType    `json:"marketType"`
	AssetClass      AssetClass    `json:"assetClass"`
	SubMarketType   SubMarketType `json:"subMarketType"` // empty (null) when not applicable

	// DailyInterestRate is returned live and appears in the docs' example
	// response, but is missing from the docs' field table; null for dated
	// futures.
	DailyInterestRate types.Decimal `json:"dailyInterestRate"`
	// LastTimeFundingRate is returned live and appears in the docs' example
	// response, but is missing from the docs' field table; it matches the rate
	// of the latest settlement in GetPublicFundingHistory. Null for dated
	// futures.
	LastTimeFundingRate types.Decimal `json:"lastTimeFundingRate"`
}

// Ticker is the last trade plus the best bid and ask of one contract, a
// Level-1 snapshot without 24-hour statistics (those are on Symbol).
// GetTicker returns one and GetAllTickers one per contract. Sizes are in
// contracts (lots); Symbol.Multiplier says what one contract is worth.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-ticker
type Ticker struct {
	// Sequence lets a consumer judge whether WebSocket messages are continuous.
	Sequence types.Int64 `json:"sequence"`
	Symbol   string      `json:"symbol"`
	// Side is the taker side of the last trade.
	Side         Side          `json:"side"`
	Size         types.Int64   `json:"size"` // size of the last trade in contracts
	TradeID      types.ID      `json:"tradeId"`
	Price        types.Decimal `json:"price"` // price of the last trade
	BestBidPrice types.Decimal `json:"bestBidPrice"`
	BestBidSize  types.Int64   `json:"bestBidSize"` // contracts
	BestAskPrice types.Decimal `json:"bestAskPrice"`
	BestAskSize  types.Int64   `json:"bestAskSize"` // contracts
	// Timestamp is the time of the last trade in nanoseconds.
	Timestamp types.Int64 `json:"ts"`
}

// Time returns the time of the last trade (Timestamp is in nanoseconds) in UTC,
// or the zero time when the field is absent.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-ticker
func (t Ticker) Time() time.Time { return nanosToTime(t.Timestamp.Value()) }

// Trade is one public trade, as returned by GetTradeHistory. Size is in
// contracts (lots); Symbol.Multiplier says what one contract is worth.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-trade-history
type Trade struct {
	Sequence     types.Int64   `json:"sequence"`
	ContractID   types.Int64   `json:"contractId"` // deprecated by KuCoin
	TradeID      types.ID      `json:"tradeId"`
	MakerOrderID types.ID      `json:"makerOrderId"`
	TakerOrderID types.ID      `json:"takerOrderId"`
	Size         types.Int64   `json:"size"` // contracts
	Price        types.Decimal `json:"price"`
	// Side is the taker side.
	Side Side `json:"side"`
	// Timestamp is the trade time in nanoseconds.
	Timestamp types.Int64 `json:"ts"`
}

// Time returns the trade time (Timestamp is in nanoseconds) in UTC, or the zero
// time when the field is absent.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-trade-history
func (t Trade) Time() time.Time { return nanosToTime(t.Timestamp.Value()) }

// OrderBook is a price-aggregated order-book snapshot, returned by both
// GetFullOrderBook and GetPartOrderBook. Bids run from the highest price to the
// lowest and Asks from the lowest to the highest; sizes are in contracts. Each
// level keeps the exact decimal text, so the same price can read 84474.0 here
// and 84474 on another feed: compare prices with Decimal.Cmp or Canonical.
//
// KuCoin documents Symbol as optional on these responses and the live API
// includes it, but the decode does not rely on it.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-full-orderbook
type OrderBook struct {
	Sequence types.Int64       `json:"sequence"`
	Symbol   string            `json:"symbol"`
	Bids     []orderbook.Level `json:"bids"`
	Asks     []orderbook.Level `json:"asks"`
	// Timestamp is the snapshot time in nanoseconds.
	Timestamp types.Int64 `json:"ts"`
}

// Time returns the snapshot time (Timestamp is in nanoseconds) in UTC, or the
// zero time when the field is absent.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-full-orderbook
func (b OrderBook) Time() time.Time { return nanosToTime(b.Timestamp.Value()) }

// KlineOptions selects the candles GetKlines returns. Symbol is a contract
// symbol such as XBTUSDTM, or one of its index and premium symbols
// (Symbol.IndexSymbol, PremiumsSymbol1M, PremiumsSymbol8H, for example
// .KXBTUSDT, .XBTUSDTMPI). From and To are Unix milliseconds and are omitted
// from the request when zero: with only From, KuCoin returns the first page of
// candles from there towards now; with only To, the last page ending at To; with
// neither, the most recent page. A page holds up to 500 candles according to
// the docs but at most 200 on the live API.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-klines
type KlineOptions struct {
	Symbol      string
	Granularity Granularity
	From        int64
	To          int64
}

// Kline is one candle. On the wire it is a JSON array of numbers in the order
// [time, open, high, low, close, volume, turnover]; this is the order shown by
// KuCoin's example response and confirmed against live data (each candle's open
// equals the previous close, and high and low bracket open and close), and it
// differs from Classic Spot, which puts close before high and low. Time is the
// candle's start in Unix milliseconds. Volume is in contracts and Turnover is
// in the quote currency; for example, 322 contracts of XBTUSDTM (0.001 BTC
// each) at about 84.5k gave 27205.9061.
//
// UnmarshalJSON accepts 7 elements, or 6 without the turnover (the docs' schema
// only says an array of numbers, so a shorter row is tolerated), each element as
// a JSON number or a numeric string; Turnover is then empty. MarshalJSON writes
// the same array, with the decimals as strings like every types.Decimal and
// without the turnover element when Turnover is empty, so a decoded Kline
// round-trips.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-klines
type Kline struct {
	Time     int64
	Open     types.Decimal
	High     types.Decimal
	Low      types.Decimal
	Close    types.Decimal
	Volume   types.Decimal
	Turnover types.Decimal
}

// Timestamp returns the candle's start time in UTC, or the zero time when Time
// is 0.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-klines
func (k Kline) Timestamp() time.Time { return millisToTime(k.Time) }

// UnmarshalJSON decodes a 6- or 7-element candle array whose elements are JSON
// numbers or numeric strings. A JSON null leaves the Kline unchanged, as is
// conventional for json.Unmarshaler.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-klines
func (k *Kline) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil
	}
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("kucoin: decode futures kline array: %w", err)
	}
	if len(raw) != 6 && len(raw) != 7 {
		return fmt.Errorf("kucoin: decode futures kline array: want 6 or 7 elements, got %d", len(raw))
	}
	var out Kline
	if blank := bytes.TrimSpace(raw[0]); bytes.Equal(blank, []byte("null")) || bytes.Equal(blank, []byte(`""`)) {
		return errors.New("kucoin: decode futures kline time: missing value")
	}
	var start types.Int64
	if err := json.Unmarshal(raw[0], &start); err != nil {
		return fmt.Errorf("kucoin: decode futures kline time: %w", err)
	}
	out.Time = start.Value()
	fields := [...]struct {
		name string
		dst  *types.Decimal
	}{
		{"open", &out.Open},
		{"high", &out.High},
		{"low", &out.Low},
		{"close", &out.Close},
		{"volume", &out.Volume},
		{"turnover", &out.Turnover},
	}
	for i, f := range fields[:len(raw)-1] {
		if err := json.Unmarshal(raw[i+1], f.dst); err != nil {
			return fmt.Errorf("kucoin: decode futures kline %s: %w", f.name, err)
		}
	}
	*k = out
	return nil
}

// MarshalJSON encodes the candle as [time, "open", "high", "low", "close",
// "volume", "turnover"]: the time as a JSON number and the decimals as strings,
// omitting the turnover element when Turnover is empty.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-klines
func (k Kline) MarshalJSON() ([]byte, error) {
	row := []any{k.Time, k.Open, k.High, k.Low, k.Close, k.Volume}
	if !k.Turnover.IsEmpty() {
		row = append(row, k.Turnover)
	}
	return json.Marshal(row)
}

// MarkPrice is the current mark price and index price of one contract, as
// returned by GetMarkPrice.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-mark-price
type MarkPrice struct {
	Symbol      string        `json:"symbol"`
	Granularity types.Int64   `json:"granularity"` // milliseconds, 1000 on the live API
	TimePoint   types.Int64   `json:"timePoint"`   // milliseconds
	Value       types.Decimal `json:"value"`       // the mark price
	IndexPrice  types.Decimal `json:"indexPrice"`
}

// Time returns TimePoint (milliseconds) in UTC, or the zero time when absent.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-mark-price
func (m MarkPrice) Time() time.Time { return millisToTime(m.TimePoint.Value()) }

// IndexOptions selects and pages the series returned by GetSpotIndexPrice,
// GetInterestRateIndex and GetPremiumIndex, which share their parameters.
//
// Symbol is the index symbol, not the contract symbol: see each method. StartAt
// and EndAt are Unix milliseconds and are omitted when zero. Reverse and Forward
// are tri-state: nil leaves KuCoin's default (true for both), otherwise the
// value is sent as "true" or "false". MaxCount is the page size: 0 uses KuCoin's
// default of 10 and the documented maximum is 100; a larger or negative value
// returns ErrInvalidMaxCount without a network call. The SDK enforces the
// documented maximum although the live API was observed to accept larger values.
//
// Paging, as observed on the live API: with the defaults a page holds the newest
// records first, and Offset set to the TimePoint of the last record of the
// previous page continues towards older records; Reverse=false lists the oldest
// records first; Forward=false pages back towards newer records. Offset is
// omitted when zero, which requests the first page. The pages report whether
// more follow in HasMore.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-spot-index-price
type IndexOptions struct {
	Symbol   string
	StartAt  int64
	EndAt    int64
	Reverse  *bool
	Forward  *bool
	Offset   int64
	MaxCount int
}

// IndexPricePage is one page of the spot index price series, returned by
// GetSpotIndexPrice.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-spot-index-price
type IndexPricePage struct {
	DataList []IndexPrice `json:"dataList"`
	HasMore  bool         `json:"hasMore"` // whether more pages follow
}

// IndexPrice is one spot index price point with the exchange prices it was
// computed from.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-spot-index-price
type IndexPrice struct {
	Symbol      string        `json:"symbol"`
	Granularity types.Int64   `json:"granularity"` // milliseconds, 1000 on the live API
	TimePoint   types.Int64   `json:"timePoint"`   // milliseconds
	Value       types.Decimal `json:"value"`       // the index price
	// DecompositionList is the per-exchange breakdown of the index. KuCoin
	// spells the JSON key "decomposionList" (without the second "i"); the key
	// is kept as sent and only the Go field name is spelled correctly. It is
	// documented as optional.
	DecompositionList []IndexComponent `json:"decomposionList"`
}

// Time returns TimePoint (milliseconds) in UTC, or the zero time when absent.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-spot-index-price
func (p IndexPrice) Time() time.Time { return millisToTime(p.TimePoint.Value()) }

// IndexComponent is one exchange's contribution to an IndexPrice.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-spot-index-price
type IndexComponent struct {
	Exchange string `json:"exchange"` // exchange code, e.g. binance
	// ExchangeName is a display name such as Binance. Observed live, not in the
	// current docs.
	ExchangeName string        `json:"exchangeName"`
	Price        types.Decimal `json:"price"`  // the exchange's price
	Weight       types.Decimal `json:"weight"` // the exchange's weight in the index
}

// InterestRatePage is one page of an interest-rate index series, returned by
// GetInterestRateIndex.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-interest-rate-index
type InterestRatePage struct {
	DataList []InterestRate `json:"dataList"`
	HasMore  bool           `json:"hasMore"` // whether more pages follow
}

// InterestRate is one interest-rate index point.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-interest-rate-index
type InterestRate struct {
	Symbol      string        `json:"symbol"`
	Granularity types.Int64   `json:"granularity"` // milliseconds
	TimePoint   types.Int64   `json:"timePoint"`   // milliseconds
	Value       types.Decimal `json:"value"`       // the interest rate
}

// Time returns TimePoint (milliseconds) in UTC, or the zero time when absent.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-interest-rate-index
func (r InterestRate) Time() time.Time { return millisToTime(r.TimePoint.Value()) }

// PremiumIndexPage is one page of a premium-index series, returned by
// GetPremiumIndex.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-premium-index
type PremiumIndexPage struct {
	DataList []PremiumIndex `json:"dataList"`
	HasMore  bool           `json:"hasMore"` // whether more pages follow
}

// PremiumIndex is one premium-index point.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-premium-index
type PremiumIndex struct {
	Symbol      string        `json:"symbol"`
	Granularity types.Int64   `json:"granularity"` // milliseconds
	TimePoint   types.Int64   `json:"timePoint"`   // milliseconds
	Value       types.Decimal `json:"value"`       // the premium index, a ratio; may be negative
}

// Time returns TimePoint (milliseconds) in UTC, or the zero time when absent.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-premium-index
func (p PremiumIndex) Time() time.Time { return millisToTime(p.TimePoint.Value()) }

// Stats24h is the platform-wide 24-hour futures trading volume, returned by
// Get24hStats.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-24hr-stats
type Stats24h struct {
	// TurnoverOf24h is the 24-hour platform futures trading volume in USD; only
	// one side of each trade is counted. KuCoin sends it in Java exponent form
	// such as 1.1155733413273683E9.
	TurnoverOf24h types.Decimal `json:"turnoverOf24h"`
}

// ServiceStatus is the state of the Futures trading service, returned by
// GetServiceStatus.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-service-status
type ServiceStatus struct {
	Status  ServiceState `json:"status"`
	Message string       `json:"msg"` // KuCoin's note; empty in normal operation
}

// FundingRate is the current funding state of one contract, returned by
// GetCurrentFundingRate. Symbol is the funding-rate symbol (for example
// .XBTUSDTMFPI8H), not the contract symbol.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/funding-fees/get-current-funding-rate
type FundingRate struct {
	Symbol      string      `json:"symbol"`      // funding-rate symbol
	Granularity types.Int64 `json:"granularity"` // milliseconds (28800000 is 8 hours)
	// TimePoint is the settlement time of the previous cycle in milliseconds. For
	// a newly listed contract it is when the first record was pre-generated, not
	// when it takes effect.
	TimePoint types.Int64   `json:"timePoint"`
	Value     types.Decimal `json:"value"` // funding rate of the current cycle
	// PredictedValue is the predicted funding rate. It is documented (field
	// table and example) but not observed live, so it is empty there.
	PredictedValue types.Decimal `json:"predictedValue"`
	// DailyInterestRate is observed live, not in the current docs.
	DailyInterestRate types.Decimal `json:"dailyInterestRate"`
	FundingRateCap    types.Decimal `json:"fundingRateCap"`   // maximum funding rate
	FundingRateFloor  types.Decimal `json:"fundingRateFloor"` // minimum funding rate
	Period            types.Int64   `json:"period"`           // whether the current funding fee is charged within this cycle (1 or 0)
	// FundingTime is the next settlement time in milliseconds.
	FundingTime types.Int64 `json:"fundingTime"`
	// LastTimeFundingRate is observed live, not in the current docs; it matches
	// the rate of the settlement at TimePoint in GetPublicFundingHistory.
	LastTimeFundingRate types.Decimal `json:"lastTimeFundingRate"`
}

// Time returns TimePoint (milliseconds) in UTC, or the zero time when absent.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/funding-fees/get-current-funding-rate
func (f FundingRate) Time() time.Time { return millisToTime(f.TimePoint.Value()) }

// NextFundingTime returns FundingTime (milliseconds) in UTC, or the zero time
// when absent.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/funding-fees/get-current-funding-rate
func (f FundingRate) NextFundingTime() time.Time { return millisToTime(f.FundingTime.Value()) }

// FundingHistoryOptions selects the settlements GetPublicFundingHistory returns.
// All three fields are required by KuCoin; From and To are Unix milliseconds.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/funding-fees/get-public-funding-history
type FundingHistoryOptions struct {
	Symbol string
	From   int64
	To     int64
}

// FundingRatePoint is the funding rate of one settlement, as returned by
// GetPublicFundingHistory.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/funding-fees/get-public-funding-history
type FundingRatePoint struct {
	Symbol      string        `json:"symbol"`      // contract symbol
	FundingRate types.Decimal `json:"fundingRate"` // funding rate of the settlement
	// TimePoint is the settlement time in milliseconds. KuCoin spells this key
	// "timepoint" in lowercase here, unlike "timePoint" everywhere else.
	TimePoint types.Int64 `json:"timepoint"`
}

// Time returns TimePoint (milliseconds) in UTC, or the zero time when absent.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/funding-fees/get-public-funding-history
func (p FundingRatePoint) Time() time.Time { return millisToTime(p.TimePoint.Value()) }

// millisToTime converts Unix milliseconds to UTC time; 0 maps to the zero time
// so that an absent field reads as IsZero rather than as 1970.
func millisToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// nanosToTime converts Unix nanoseconds to UTC time; 0 maps to the zero time.
func nanosToTime(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns).UTC()
}
