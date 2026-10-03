package market

import (
	"encoding/json"
	"fmt"
)

// TradeType selects the UTA v2 market product family.
type TradeType string

// UTA v2 market product families supported by the public endpoints.
const (
	TradeTypeSpot    TradeType = "SPOT"
	TradeTypeFutures TradeType = "FUTURES"
)

// AnnouncementOptions filters paginated platform announcements. KuCoin uses
// the previous month when no time range is supplied.
type AnnouncementOptions struct {
	Language   string
	Type       string
	PageNumber int
	PageSize   int
	StartTime  int64
	EndTime    int64
}

// AnnouncementPage is a page of platform announcements.
type AnnouncementPage struct {
	TotalNumber int            `json:"totalNumber"`
	TotalPage   int            `json:"totalPage"`
	PageNumber  int            `json:"pageNumber"`
	PageSize    int            `json:"pageSize"`
	List        []Announcement `json:"list"`
}

// Announcement is one platform announcement.
type Announcement struct {
	ID          int64    `json:"id"`
	Title       string   `json:"title"`
	Type        []string `json:"type"`
	Description string   `json:"description"`
	ReleaseTime int64    `json:"releaseTime"`
	Language    string   `json:"language"`
	URL         string   `json:"url"`
}

// Currency contains a currency and its supported chain configurations.
type Currency struct {
	Currency  string          `json:"currency"`
	Name      string          `json:"name"`
	FullName  string          `json:"fullName"`
	Precision int             `json:"precision"`
	Chains    []CurrencyChain `json:"list"`
}

// CurrencyChain is one deposit and withdrawal blockchain configuration.
type CurrencyChain struct {
	ChainName         string `json:"chainName"`
	Chain             string `json:"chain"`
	WithdrawFeeRate   string `json:"withdrawFeeRate"`
	IsWithdrawEnabled bool   `json:"isWithdrawEnabled"`
	IsDepositEnabled  bool   `json:"isDepositEnabled"`
	Confirms          int    `json:"confirms"`
	PreConfirms       int    `json:"preConfirms"`
	ContractAddress   string `json:"contractAddress"`
	WithdrawPrecision int    `json:"withdrawPrecision"`
	MaxWithdrawFee    string `json:"maxWithdrawFee"`
	IsMemoRequired    bool   `json:"isMemoRequired"`
	MinWithdrawSize   string `json:"minWithdrawSize"`
	MinDepositSize    string `json:"minDepositSize"`
	MaxWithdrawSize   string `json:"maxWithdrawSize"`
	MaxDepositSize    string `json:"maxDepositSize"`
	FixedDepositFee   string `json:"fixedDepositFee"`
	MaxDepositFee     string `json:"maxDepositFee"`
	MinWithdrawFee    string `json:"minWithdrawFee"`
	AddressRegex      string `json:"addressRegex"`
	MemoRegex         string `json:"memoRegex"`
}

// InstrumentList is the response returned by GetInstruments.
type InstrumentList struct {
	TradeType string       `json:"tradeType"`
	List      []Instrument `json:"list"`
}

// Instrument is one spot or futures trading instrument. Product-specific
// fields are empty when KuCoin does not return them for the selected type.
type Instrument struct {
	Symbol              string `json:"symbol"`
	Name                string `json:"name"`
	BaseCurrency        string `json:"baseCurrency"`
	QuoteCurrency       string `json:"quoteCurrency"`
	Market              string `json:"market"`
	MinBaseOrderSize    string `json:"minBaseOrderSize"`
	MinQuoteOrderSize   string `json:"minQuoteOrderSize"`
	MaxBaseOrderSize    string `json:"maxBaseOrderSize"`
	MaxQuoteOrderSize   string `json:"maxQuoteOrderSize"`
	BaseOrderStep       string `json:"baseOrderStep"`
	QuoteOrderStep      string `json:"quoteOrderStep"`
	TickSize            string `json:"tickSize"`
	FeeCurrency         string `json:"feeCurrency"`
	TradingStatus       string `json:"tradingStatus"`
	MarginMode          string `json:"marginMode"`
	PriceLimitRatio     string `json:"priceLimitRatio"`
	FeeCategory         string `json:"feeCategory"`
	MakerFeeCoefficient string `json:"makerFeeCoefficient"`
	TakerFeeCoefficient string `json:"takerFeeCoefficient"`
	MinFunds            string `json:"minFunds"`
	ContractType        string `json:"contractType"`
	SettlementCurrency  string `json:"settlementCurrency"`
	Multiplier          string `json:"multiplier"`
	MaxLeverage         string `json:"maxLeverage"`
	IndexPrice          string `json:"indexPrice"`
	MarkPrice           string `json:"markPrice"`
	FundingInterval     int64  `json:"fundingInterval"`
}

// TickerList is a UTA v2 market ticker snapshot.
type TickerList struct {
	TradeType string   `json:"tradeType"`
	Timestamp int64    `json:"ts"`
	List      []Ticker `json:"list"`
}

// Ticker is best bid/ask plus 24-hour statistics for one instrument.
type Ticker struct {
	Symbol             string `json:"symbol"`
	Name               string `json:"name"`
	BestBidSize        string `json:"bestBidSize"`
	BestBidPrice       string `json:"bestBidPrice"`
	BestAskSize        string `json:"bestAskSize"`
	BestAskPrice       string `json:"bestAskPrice"`
	LastPrice          string `json:"lastPrice"`
	Size               string `json:"size"`
	Open               string `json:"open"`
	High               string `json:"high"`
	Low                string `json:"low"`
	BaseVolume         string `json:"baseVolume"`
	QuoteVolume        string `json:"quoteVolume"`
	PriceChange        string `json:"priceChange"`
	PriceChangePercent string `json:"priceChangePercent"`
	IndexPrice         string `json:"indexPrice"`
	MarkPrice          string `json:"markPrice"`
}

// KlineOptions controls a UTA v2 candle request. KlineType accepts TRADE,
// INDEX_PRICE, MARK_PRICE, or PREMIUM_INDEX where supported by KuCoin.
type KlineOptions struct {
	TradeType TradeType
	Symbol    string
	KlineType string
	Interval  string
	StartAt   int64
	EndAt     int64
}

// KlineList is the response returned by GetKlines.
type KlineList struct {
	TradeType string  `json:"tradeType"`
	Symbol    string  `json:"symbol"`
	List      []Kline `json:"list"`
}

// Kline is one UTA v2 OHLCV candle. Its wire order is [timestamp, open,
// high, low, close, volume, turnover].
type Kline struct {
	Timestamp int64
	Open      string
	High      string
	Low       string
	Close     string
	Volume    string
	Turnover  string
}

// UnmarshalJSON decodes KuCoin's fixed 7-element candle array.
func (k *Kline) UnmarshalJSON(data []byte) error {
	var raw [7]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("kucoin: decode UTA v2 kline array: %w", err)
	}
	if err := json.Unmarshal(raw[0], &k.Timestamp); err != nil {
		return fmt.Errorf("kucoin: decode UTA v2 kline timestamp: %w", err)
	}
	fields := [6]*string{&k.Open, &k.High, &k.Low, &k.Close, &k.Volume, &k.Turnover}
	for i, field := range fields {
		if err := json.Unmarshal(raw[i+1], field); err != nil {
			return fmt.Errorf("kucoin: decode UTA v2 kline field %d: %w", i+1, err)
		}
	}
	return nil
}

// OrderBookOptions controls the authenticated UTA v2 depth snapshot request.
type OrderBookOptions struct {
	TradeType TradeType
	Symbol    string
	// Limit is the number of levels per side. KuCoin requires a limit of 20 or
	// 100; for the complete book set Full instead.
	Limit     int
	RPIFilter int
	// Full requests the complete order book (limit=FULL) and takes precedence
	// over Limit. A book kept in step with the incremental feed needs it: a
	// truncated snapshot would leave every level beyond the cut-off missing.
	Full bool
}

// OrderBook is a UTA v2 depth snapshot. Bids and Asks retain every value in
// each exchange level, including optional RPI metadata.
type OrderBook struct {
	TradeType string     `json:"tradeType"`
	Symbol    string     `json:"symbol"`
	Sequence  int64      `json:"sequence"`
	Bids      [][]string `json:"bids"`
	Asks      [][]string `json:"asks"`
}

// TradeList is the response returned by GetTrades.
type TradeList struct {
	TradeType string  `json:"tradeType"`
	List      []Trade `json:"list"`
}

// Trade is one public trade.
type Trade struct {
	Sequence  int64  `json:"sequence"`
	TradeID   string `json:"tradeId"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Side      string `json:"side"`
	Timestamp int64  `json:"ts"`
}

// CustodyCurrency is a currency supported by a third-party custodian.
type CustodyCurrency struct {
	Custodian string `json:"custodian"`
	Currency  string `json:"currency"`
	Precision int    `json:"precision"`
}

// ServiceStatus reports whether a product family's service is open.
type ServiceStatus struct {
	TradeType    string `json:"tradeType"`
	ServerStatus string `json:"serverStatus"`
	Message      string `json:"msg"`
}

// KYCRegion is one region returned by GetKYCRegions.
type KYCRegion struct {
	Code   string `json:"code"`
	ENName string `json:"enName"`
}

// IndexPriceList is the envelope returned by GetIndexPrices.
type IndexPriceList struct {
	Items []IndexPrice `json:"items"`
}

// IndexPrice is a one-second futures index-price snapshot.
type IndexPrice struct {
	Symbol            string                `json:"symbol"`
	Granularity       int64                 `json:"granularity"`
	Timestamp         int64                 `json:"ts"`
	IndexPrice        string                `json:"indexPrice"`
	DecompositionList []IndexPriceComponent `json:"decompositionList"`
}

// IndexPriceComponent identifies one weighted exchange price in an index.
type IndexPriceComponent struct {
	Exchange string `json:"exchange"`
	Price    string `json:"price"`
	Weight   string `json:"weight"`
}

// PositionTiersOptions filters the public UTA futures position-tier table.
type PositionTiersOptions struct {
	TradeType   string // FUTURES
	Currency    string // comma-separated settlement currencies
	MarginMode  string // CROSS or ISOLATED
	Data        string // RISK_LIMIT
	AccountType string // UNIFIED
	Symbol      string
}

// PositionTier is one UTA futures risk tier.
type PositionTier struct {
	Currency           string `json:"currency"`
	Tier               int    `json:"tier"`
	MinSize            string `json:"minSize"`
	MaxSize            string `json:"maxSize"`
	MaxLeverage        string `json:"maxLeverage"`
	MaintainMarginRate string `json:"maintainMarginRate"`
}

// CollateralRatio describes a currency's tiered collateral discount ratio.
type CollateralRatio struct {
	Currency   string                `json:"currency"`
	CDRConfigs []CollateralRatioTier `json:"cdrConfigs"`
}

// CollateralRatioTier is one balance range within a collateral ratio.
type CollateralRatioTier struct {
	Tier      int    `json:"tier"`
	Min       string `json:"min"`
	Max       string `json:"max"`
	CDR       string `json:"cdr"`
	CDREquity string `json:"cdrEquity"`
}

// BorrowableCurrency is a UTA margin currency eligible for borrowing.
type BorrowableCurrency struct {
	Currency string `json:"currency"`
}

// FundingRatesOptions filters current UTA futures funding rates.
type FundingRatesOptions struct {
	Symbol      string
	ProductType string
}

// FundingRate is the current funding-rate state for one futures symbol.
type FundingRate struct {
	Symbol                  string `json:"symbol"`
	NextFundingRate         string `json:"nextFundingRate"`
	FundingTime             int64  `json:"fundingTime"`
	FundingRateCap          string `json:"fundingRateCap"`
	FundingRateFloor        string `json:"fundingRateFloor"`
	CurrentGranularity      int64  `json:"currentGranularity"`
	NewGranularity          int64  `json:"newGranularity"`
	NewGranularityStartTime int64  `json:"newGranularityStartTime"`
}

// FundingRateHistory is the funding-rate settlement series for one symbol.
type FundingRateHistory struct {
	Symbol string                   `json:"symbol"`
	List   []FundingRateHistoryItem `json:"list"`
}

// FundingRateHistoryItem is one funding settlement.
type FundingRateHistoryItem struct {
	FundingRate string `json:"fundingRate"`
	Timestamp   int64  `json:"ts"`
}

// OpenInterestOptions controls real-time or historical open-interest lookup.
// Set Interval for history; it is required by KuCoin for historical queries.
type OpenInterestOptions struct {
	Symbol   string
	Interval string
	StartAt  int64
	EndAt    int64
	PageSize int
}

// OpenInterest is one current or historical futures open-interest point.
type OpenInterest struct {
	Symbol       string `json:"symbol"`
	OpenInterest string `json:"openInterest"`
	Timestamp    int64  `json:"ts"`
}

// InterestRateIndexOptions filters the cursor-paginated interest-rate index.
type InterestRateIndexOptions struct {
	Symbol   string
	StartAt  int64
	EndAt    int64
	LastID   int64
	PageSize int
}

// InterestRateIndexPage is the paginated UTA futures interest-rate index.
type InterestRateIndexPage struct {
	Items  []InterestRateIndex `json:"items"`
	LastID int64               `json:"lastId"`
}

// InterestRateIndex is one futures interest-rate record.
type InterestRateIndex struct {
	Symbol       string `json:"symbol"`
	InterestRate string `json:"interestRate"`
	Timestamp    int64  `json:"ts"`
}

// TradeStatistics is 24-hour platform turnover for spot and futures.
type TradeStatistics struct {
	Spot    TurnoverStatistic `json:"spot"`
	Futures TurnoverStatistic `json:"futures"`
}

// TurnoverStatistic is a product family's 24-hour turnover.
type TurnoverStatistic struct {
	TurnoverOf24h string `json:"turnoverOf24h"`
}

// CallAuctionInfo is the current auction estimate and order-price ranges.
type CallAuctionInfo struct {
	Symbol                  string `json:"symbol"`
	EstimatedPrice          string `json:"estimatedPrice"`
	EstimatedSize           string `json:"estimatedSize"`
	SellOrderRangeLowPrice  string `json:"sellOrderRangeLowPrice"`
	SellOrderRangeHighPrice string `json:"sellOrderRangeHighPrice"`
	BuyOrderRangeLowPrice   string `json:"buyOrderRangeLowPrice"`
	BuyOrderRangeHighPrice  string `json:"buyOrderRangeHighPrice"`
	Time                    int64  `json:"time"`
}
