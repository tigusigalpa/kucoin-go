// Package market implements KuCoin Classic Spot's market-data endpoints.
// Despite sharing a name with uta/market, Classic Spot's wire formats
// differ (e.g. kline field order) — do not share structs between the two.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-all-symbols
package market

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// ChainInfo is a single deposit/withdrawal network for a Currency.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-currency
type ChainInfo struct {
	ChainName         string `json:"chainName"`
	WithdrawalMinSize string `json:"withdrawalMinSize"`
	WithdrawMinSize   string `json:"withdrawMinSize"`
	DepositMinSize    string `json:"depositMinSize,omitempty"`
	WithdrawFeeRate   string `json:"withdrawFeeRate"`
	WithdrawalMinFee  string `json:"withdrawalMinFee"`
	WithdrawMinFee    string `json:"withdrawMinFee"`
	IsWithdrawEnabled bool   `json:"isWithdrawEnabled"`
	IsDepositEnabled  bool   `json:"isDepositEnabled"`
	Confirms          int    `json:"confirms"`
	PreConfirms       int    `json:"preConfirms"`
	ContractAddress   string `json:"contractAddress,omitempty"`
	WithdrawPrecision int    `json:"withdrawPrecision"`
	MaxWithdraw       string `json:"maxWithdraw,omitempty"`
	MaxDeposit        string `json:"maxDeposit,omitempty"`
	NeedTag           bool   `json:"needTag"`
	ChainID           string `json:"chainId"`
}

// Currency describes a single asset and its supported chains. KuCoin's
// schema table for this endpoint documents "data" as an array, but the
// worked example in the same doc shows a single object — this SDK
// follows the example (single object), which is what the live API
// actually returns.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-currency
type Currency struct {
	Currency        string      `json:"currency"`
	Name            string      `json:"name"`
	FullName        string      `json:"fullName"`
	Precision       int         `json:"precision"`
	Confirms        int         `json:"confirms,omitempty"`
	ContractAddress string      `json:"contractAddress,omitempty"`
	IsMarginEnabled bool        `json:"isMarginEnabled"`
	IsDebitEnabled  bool        `json:"isDebitEnabled"`
	Chains          []ChainInfo `json:"chains"`
}

// Symbol is a single trading pair's specification.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-all-symbols
type Symbol struct {
	Symbol                          string `json:"symbol"`
	Name                            string `json:"name"`
	BaseCurrency                    string `json:"baseCurrency"`
	QuoteCurrency                   string `json:"quoteCurrency"`
	FeeCurrency                     string `json:"feeCurrency"`
	Market                          string `json:"market"`
	BaseMinSize                     string `json:"baseMinSize"`
	QuoteMinSize                    string `json:"quoteMinSize"`
	BaseMaxSize                     string `json:"baseMaxSize"`
	QuoteMaxSize                    string `json:"quoteMaxSize"`
	BaseIncrement                   string `json:"baseIncrement"`
	QuoteIncrement                  string `json:"quoteIncrement"`
	PriceIncrement                  string `json:"priceIncrement"`
	PriceLimitRate                  string `json:"priceLimitRate"`
	MinFunds                        string `json:"minFunds"`
	IsMarginEnabled                 bool   `json:"isMarginEnabled"`
	EnableTrading                   bool   `json:"enableTrading"`
	FeeCategory                     int    `json:"feeCategory"`
	MakerFeeCoefficient             string `json:"makerFeeCoefficient"`
	TakerFeeCoefficient             string `json:"takerFeeCoefficient"`
	St                              bool   `json:"st"`
	CallauctionIsEnabled            bool   `json:"callauctionIsEnabled"`
	CallauctionPriceFloor           string `json:"callauctionPriceFloor,omitempty"`
	CallauctionPriceCeiling         string `json:"callauctionPriceCeiling,omitempty"`
	CallauctionFirstStageStartTime  int64  `json:"callauctionFirstStageStartTime,omitempty"`
	CallauctionSecondStageStartTime int64  `json:"callauctionSecondStageStartTime,omitempty"`
	CallauctionThirdStageStartTime  int64  `json:"callauctionThirdStageStartTime,omitempty"`
	TradingStartTime                int64  `json:"tradingStartTime,omitempty"`
}

// Ticker is the "Level 1" snapshot: best bid/ask plus the last trade.
// This is not a full 24h-stats ticker — see AllTickers for that.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-ticker
type Ticker struct {
	Time        int64  `json:"time"`
	Sequence    string `json:"sequence"`
	Price       string `json:"price"`
	Size        string `json:"size"`
	BestBid     string `json:"bestBid"`
	BestBidSize string `json:"bestBidSize"`
	BestAsk     string `json:"bestAsk"`
	BestAskSize string `json:"bestAskSize"`
}

// TickerStat is one symbol's 24h statistics within AllTickers.
type TickerStat struct {
	Symbol           string `json:"symbol"`
	SymbolName       string `json:"symbolName"`
	Buy              string `json:"buy"`
	BestBidSize      string `json:"bestBidSize"`
	Sell             string `json:"sell"`
	BestAskSize      string `json:"bestAskSize"`
	ChangeRate       string `json:"changeRate"`
	ChangePrice      string `json:"changePrice"`
	High             string `json:"high"`
	Low              string `json:"low"`
	Vol              string `json:"vol"`
	VolValue         string `json:"volValue"`
	Last             string `json:"last"`
	AveragePrice     string `json:"averagePrice"`
	TakerFeeRate     string `json:"takerFeeRate"`
	MakerFeeRate     string `json:"makerFeeRate"`
	TakerCoefficient string `json:"takerCoefficient"`
	MakerCoefficient string `json:"makerCoefficient"`
}

// AllTickers is a 2-second snapshot of every symbol's 24h statistics.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-all-tickers
type AllTickers struct {
	Time   int64        `json:"time"`
	Ticker []TickerStat `json:"ticker"`
}

// Kline is one OHLCV candle. Field order on the wire is
// [startTime, open, close, high, low, volume, turnover] — note "close"
// comes before "high"/"low", and this order differs from UTA's
// [timestamp, open, high, low, close, volume, turnover]. Do not share
// this type with uta/market.Kline.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-klines
type Kline struct {
	StartTime int64
	Open      string
	Close     string
	High      string
	Low       string
	Volume    string
	Turnover  string
}

// UnmarshalJSON decodes a kline from its wire representation, a 7-element
// JSON array of strings: [startTime, open, close, high, low, volume, turnover].
func (k *Kline) UnmarshalJSON(data []byte) error {
	var raw [7]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("kucoin: decode classic spot kline array: %w", err)
	}
	startTime, err := strconv.ParseInt(raw[0], 10, 64)
	if err != nil {
		return fmt.Errorf("kucoin: decode classic spot kline start time: %w", err)
	}
	k.StartTime = startTime
	k.Open = raw[1]
	k.Close = raw[2]
	k.High = raw[3]
	k.Low = raw[4]
	k.Volume = raw[5]
	k.Turnover = raw[6]
	return nil
}

// OrderBookLevel is a single [price, size] entry.
type OrderBookLevel [2]string

// OrderBook is a bid/ask depth snapshot, shared by GetPartOrderBook and
// GetFullOrderBook.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-part-orderbook
type OrderBook struct {
	Time     int64            `json:"time"`
	Sequence string           `json:"sequence"`
	Bids     []OrderBookLevel `json:"bids"`
	Asks     []OrderBookLevel `json:"asks"`
}
