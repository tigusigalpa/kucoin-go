// Package market implements KuCoin UTA's public market-data endpoints.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/introduction
package market

import (
	"encoding/json"
	"fmt"
)

// TradeType selects the UTA product family for a market-data request.
// Kept as a plain string alias (not a distinct type) so callers can pass
// literals or these constants interchangeably.
type TradeType = string

const (
	TradeTypeSpot    TradeType = "SPOT"
	TradeTypeFutures TradeType = "FUTURES"
)

// Instrument is a single trading pair's specification.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-symbol
type Instrument struct {
	Symbol                          string `json:"symbol"`
	Name                            string `json:"name"`
	BaseCurrency                    string `json:"baseCurrency"`
	QuoteCurrency                   string `json:"quoteCurrency"`
	Market                          string `json:"market"`
	MinBaseOrderSize                string `json:"minBaseOrderSize"`
	MinQuoteOrderSize               string `json:"minQuoteOrderSize"`
	MaxBaseOrderSize                string `json:"maxBaseOrderSize"`
	MaxQuoteOrderSize               string `json:"maxQuoteOrderSize"`
	BaseOrderStep                   string `json:"baseOrderStep"`
	QuoteOrderStep                  string `json:"quoteOrderStep"`
	TickSize                        string `json:"tickSize"`
	FeeCurrency                     string `json:"feeCurrency"`
	TradingStatus                   string `json:"tradingStatus"`
	MarginMode                      string `json:"marginMode"`
	PriceLimitRatio                 string `json:"priceLimitRatio"`
	FeeCategory                     string `json:"feeCategory"`
	MakerFeeCoefficient             string `json:"makerFeeCoefficient"`
	TakerFeeCoefficient             string `json:"takerFeeCoefficient"`
	St                              bool   `json:"st"`
	MinFunds                        string `json:"minFunds"`
	CallauctionIsEnabled            bool   `json:"callauctionIsEnabled"`
	CallauctionPriceFloor           string `json:"callauctionPriceFloor"`
	CallauctionPriceCeiling         string `json:"callauctionPriceCeiling"`
	CallauctionFirstStageStartTime  int64  `json:"callauctionFirstStageStartTime"`
	CallauctionSecondStageStartTime int64  `json:"callauctionSecondStageStartTime"`
	CallauctionThirdStageStartTime  int64  `json:"callauctionThirdStageStartTime"`
	TradingStartTime                int64  `json:"tradingStartTime"`
}

// InstrumentList is the envelope returned by GetInstruments.
type InstrumentList struct {
	TradeType string       `json:"tradeType"`
	List      []Instrument `json:"list"`
}

// Ticker is best-bid/ask plus 24h statistics for a single symbol.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-ticker
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
}

// TickerList is the envelope returned by GetTickers.
type TickerList struct {
	TradeType string   `json:"tradeType"`
	Ts        int64    `json:"ts"`
	List      []Ticker `json:"list"`
}

// OrderBookLevel is a single [price, size] entry.
type OrderBookLevel [2]string

// OrderBook is a snapshot of the bid/ask depth for a symbol.
//
// Note: unlike every sibling UTA market-data endpoint, this one requires
// credentials — confirmed by a live call (see GetOrderBook's docblock).
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-orderbook
type OrderBook struct {
	TradeType string           `json:"tradeType"`
	Symbol    string           `json:"symbol"`
	Sequence  int64            `json:"sequence"`
	Bids      []OrderBookLevel `json:"bids"`
	Asks      []OrderBookLevel `json:"asks"`
}

// Kline is one OHLCV candle. Field order on the wire is
// [timestamp, open, high, low, close, volume, turnover] — UTA's order
// differs from Classic Spot's; do not share this struct with a Classic
// kline parser.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-klines
type Kline struct {
	Timestamp int64
	Open      string
	High      string
	Low       string
	Close     string
	Volume    string
	Turnover  string
}

// UnmarshalJSON decodes a kline from its wire representation, a 7-element
// JSON array: [timestamp, open, high, low, close, volume, turnover].
func (k *Kline) UnmarshalJSON(data []byte) error {
	var raw [7]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("kucoin: decode kline array: %w", err)
	}
	var timestamp int64
	if err := json.Unmarshal(raw[0], &timestamp); err != nil {
		return fmt.Errorf("kucoin: decode kline timestamp: %w", err)
	}
	fields := [6]*string{&k.Open, &k.High, &k.Low, &k.Close, &k.Volume, &k.Turnover}
	for i, field := range fields {
		if err := json.Unmarshal(raw[i+1], field); err != nil {
			return fmt.Errorf("kucoin: decode kline field %d: %w", i+1, err)
		}
	}
	k.Timestamp = timestamp
	return nil
}

// Trade is a single recent public trade.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-trades
type Trade struct {
	Sequence string `json:"sequence"`
	TradeID  string `json:"tradeId"`
	Price    string `json:"price"`
	Size     string `json:"size"`
	Side     string `json:"side"`
	Ts       int64  `json:"ts"`
}

// TradeList is the envelope returned by GetTrades.
type TradeList struct {
	TradeType string  `json:"tradeType"`
	List      []Trade `json:"list"`
}

// CurrencyChain is one deposit/withdrawal network for a currency.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-currencies
type CurrencyChain struct {
	ChainName         string `json:"chainName"`
	MinWithdrawSize   string `json:"minWithdrawSize"`
	MinDepositSize    string `json:"minDepositSize"`
	WithdrawFeeRate   string `json:"withdrawFeeRate"`
	MinWithdrawFee    string `json:"minWithdrawFee"`
	IsWithdrawEnabled bool   `json:"isWithdrawEnabled"`
	IsDepositEnabled  bool   `json:"isDepositEnabled"`
	Confirms          int    `json:"confirms"`
	PreConfirms       int    `json:"preConfirms"`
	ContractAddress   string `json:"contractAddress"`
	WithdrawPrecision int    `json:"withdrawPrecision"`
	MaxWithdrawSize   string `json:"maxWithdrawSize"`
	MaxDepositSize    string `json:"maxDepositSize"`
	IsMemoRequired    bool   `json:"isMemoRequired"`
	ChainID           string `json:"chainId"`
}

// Currency describes a single asset and its supported chains.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-currencies
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-currency
type Currency struct {
	Currency        string          `json:"currency"`
	Name            string          `json:"name"`
	FullName        string          `json:"fullName"`
	Precision       int             `json:"precision"`
	IsMarginEnabled bool            `json:"isMarginEnabled"`
	IsDebitEnabled  bool            `json:"isDebitEnabled"`
	Items           []CurrencyChain `json:"items"`
}

// ServiceStatus reports whether trading is currently open for a product
// family.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-service-status
type ServiceStatus struct {
	TradeType    string `json:"tradeType"`
	ServerStatus string `json:"serverStatus"`
	Msg          string `json:"msg"`
}

// Announcement is a single platform announcement.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-announcements
type Announcement struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Type        []string `json:"type"`
	Description string   `json:"description"`
	ReleaseTime int64    `json:"releaseTime"`
	Language    string   `json:"language"`
	URL         string   `json:"url"`
}

// AnnouncementPage is the paginated envelope returned by GetAnnouncements.
type AnnouncementPage struct {
	TotalNumber int            `json:"totalNumber"`
	TotalPage   int            `json:"totalPage"`
	PageNumber  int            `json:"pageNumber"`
	PageSize    int            `json:"pageSize"`
	List        []Announcement `json:"list"`
}

// TradeStatistics is 24h platform-wide turnover for spot and futures.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-trade-statistics
type TradeStatistics struct {
	Spot struct {
		TurnoverOf24h string `json:"turnoverOf24h"`
	} `json:"spot"`
	Futures struct {
		TurnoverOf24h string `json:"turnoverOf24h"`
	} `json:"futures"`
}
