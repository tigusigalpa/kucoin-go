// Package market implements the current UTA REST v2 public market-data API.
//
// This package is deliberately separate from uta/market, which preserves the
// SDK's existing UTA v1 public API. KuCoin lists those v1 endpoints under its
// abandoned API documentation; callers that need the current contract should
// use Client.UTA.V2.Market.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/introduction
package market

import (
	"context"
	"net/http"
	"net/url"
	"strconv"

	"github.com/tigusigalpa/kucoin-go/transport"
)

// Client provides current KuCoin UTA REST v2 market-data endpoints. All are
// public except GetOrderBook, which KuCoin marks as a General-permission
// private endpoint.
type Client struct {
	executor *transport.Executor
}

// NewClient wires a v2 market Client to the shared UTA executor. It is not
// normally called directly; use kucoin.NewClient.
func NewClient(executor *transport.Executor) *Client {
	return &Client{executor: executor}
}

// GetAnnouncements returns a page of KuCoin platform announcements.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-announcements
func (c *Client) GetAnnouncements(ctx context.Context, opts AnnouncementOptions) (*AnnouncementPage, error) {
	query := map[string]string{}
	putIfSet(query, "language", opts.Language)
	putIfSet(query, "type", opts.Type)
	putIntIfPositive(query, "pageNumber", opts.PageNumber)
	putIntIfPositive(query, "pageSize", opts.PageSize)
	putInt64IfSet(query, "startTime", opts.StartTime)
	putInt64IfSet(query, "endTime", opts.EndTime)
	var result AnnouncementPage
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/announcement", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetCurrency returns the configuration for one currency and optionally one
// chain. Use the returned chain value in related asset requests.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/currency
func (c *Client) GetCurrency(ctx context.Context, currency, chain string) (*Currency, error) {
	query := map[string]string{}
	putIfSet(query, "currency", currency)
	putIfSet(query, "chain", chain)
	var result Currency
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/currency", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetCurrencies returns the configuration for zero or more currencies. KuCoin
// requires one currencyList query key per requested currency; this method
// preserves that repeated-key form rather than silently joining values.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/currencies
func (c *Client) GetCurrencies(ctx context.Context, currencies []string, chain string) ([]Currency, error) {
	query := url.Values{}
	for _, currency := range currencies {
		if currency != "" {
			query.Add("currencyList", currency)
		}
	}
	if chain != "" {
		query.Set("chain", chain)
	}
	var result []Currency
	if _, err := c.executor.DoPublicValues(ctx, http.MethodGet, "/api/ua/v2/asset/currencies", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetTickers returns two-second ticker snapshots for a UTA v2 product family,
// optionally filtered to one symbol.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-ticker
func (c *Client) GetTickers(ctx context.Context, tradeType TradeType, symbol string) (*TickerList, error) {
	query := map[string]string{"tradeType": string(tradeType)}
	putIfSet(query, "symbol", symbol)
	var result TickerList
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/ticker", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetInstruments returns spot or futures trading instrument specifications,
// optionally filtered to one symbol.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/instrument
func (c *Client) GetInstruments(ctx context.Context, tradeType TradeType, symbol string) (*InstrumentList, error) {
	query := map[string]string{"tradeType": string(tradeType)}
	putIfSet(query, "symbol", symbol)
	var result InstrumentList
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/instrument", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetKlines returns UTA v2 candles for the requested product, symbol and
// interval. A zero time bound leaves that bound to KuCoin's default.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-klines
func (c *Client) GetKlines(ctx context.Context, opts KlineOptions) (*KlineList, error) {
	query := map[string]string{
		"tradeType": string(opts.TradeType),
		"symbol":    opts.Symbol,
		"interval":  opts.Interval,
	}
	putIfSet(query, "klineType", opts.KlineType)
	putInt64IfSet(query, "startAt", opts.StartAt)
	putInt64IfSet(query, "endAt", opts.EndAt)
	var result KlineList
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/kline", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetOrderBook returns an authenticated UTA v2 order-book snapshot. KuCoin
// assigns this endpoint the General permission even though its data is market
// data, so calls without complete credentials return
// transport.ErrCredentialsRequired before making a request.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-orderbook
func (c *Client) GetOrderBook(ctx context.Context, opts OrderBookOptions) (*OrderBook, error) {
	query := map[string]string{
		"tradeType": string(opts.TradeType),
		"symbol":    opts.Symbol,
	}
	putIntIfPositive(query, "limit", opts.Limit)
	putIntIfPositive(query, "rpiFilter", opts.RPIFilter)
	var result OrderBook
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/ua/v2/market/orderbook", query, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetTrades returns the latest public trades for a UTA v2 spot or futures
// instrument.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-trades
func (c *Client) GetTrades(ctx context.Context, tradeType TradeType, symbol string) (*TradeList, error) {
	query := map[string]string{"tradeType": string(tradeType), "symbol": symbol}
	var result TradeList
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/trade", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetIndexPrices returns index-price snapshots, optionally limited to one
// futures symbol.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-index-price
func (c *Client) GetIndexPrices(ctx context.Context, symbol string) (*IndexPriceList, error) {
	query := optionalQuery("symbol", symbol)
	var result IndexPriceList
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/index-price", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetPositionTiers returns public UTA futures risk tiers for the supplied
// filters. TradeType is normally FUTURES and Data is normally RISK_LIMIT.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-position-tiers
func (c *Client) GetPositionTiers(ctx context.Context, opts PositionTiersOptions) ([]PositionTier, error) {
	query := map[string]string{}
	putIfSet(query, "tradeType", opts.TradeType)
	putIfSet(query, "currency", opts.Currency)
	putIfSet(query, "marginMode", opts.MarginMode)
	putIfSet(query, "data", opts.Data)
	putIfSet(query, "accountType", opts.AccountType)
	putIfSet(query, "symbol", opts.Symbol)
	var result []PositionTier
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/position-tiers", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetCollateralRatios returns the UTA collateral discount-ratio tiers for
// every supported currency.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-collateral-ratio
func (c *Client) GetCollateralRatios(ctx context.Context) ([]CollateralRatio, error) {
	var result []CollateralRatio
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/collateral-discount-ratio", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetBorrowableCurrencies returns currencies that can be borrowed in UTA
// margin trading.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-borrowable-currencies
func (c *Client) GetBorrowableCurrencies(ctx context.Context) ([]BorrowableCurrency, error) {
	var result []BorrowableCurrency
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/borrowable-currency", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetFundingRates returns current perpetual-futures funding rates. Symbol and
// ProductType are optional server filters.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-current-funding
func (c *Client) GetFundingRates(ctx context.Context, opts FundingRatesOptions) ([]FundingRate, error) {
	query := map[string]string{}
	putIfSet(query, "symbol", opts.Symbol)
	putIfSet(query, "productType", opts.ProductType)
	var result []FundingRate
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/funding-rate", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetFundingRateHistory returns funding-rate settlements for one futures
// symbol in the requested Unix-millisecond time range.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-history-funding-rate
func (c *Client) GetFundingRateHistory(ctx context.Context, symbol string, startAt, endAt int64) (*FundingRateHistory, error) {
	query := map[string]string{"symbol": symbol}
	putInt64IfSet(query, "startAt", startAt)
	putInt64IfSet(query, "endAt", endAt)
	var result FundingRateHistory
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/funding-rate-history", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetOpenInterest returns real-time open interest when Interval is empty, or
// historical open interest for one symbol when Interval is set.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-futures-open-interest
func (c *Client) GetOpenInterest(ctx context.Context, opts OpenInterestOptions) ([]OpenInterest, error) {
	query := map[string]string{}
	putIfSet(query, "symbol", opts.Symbol)
	putIfSet(query, "interval", opts.Interval)
	putInt64IfSet(query, "startAt", opts.StartAt)
	putInt64IfSet(query, "endAt", opts.EndAt)
	if opts.PageSize > 0 {
		query["pageSize"] = strconv.Itoa(opts.PageSize)
	}
	var result []OpenInterest
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/open-interest", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetInterestRateIndex returns the cursor-paginated interest-rate index for
// a futures symbol.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-interest-rate-index
func (c *Client) GetInterestRateIndex(ctx context.Context, opts InterestRateIndexOptions) (*InterestRateIndexPage, error) {
	query := map[string]string{"symbol": opts.Symbol}
	putInt64IfSet(query, "startAt", opts.StartAt)
	putInt64IfSet(query, "endAt", opts.EndAt)
	putInt64IfSet(query, "lastId", opts.LastID)
	if opts.PageSize > 0 {
		query["pageSize"] = strconv.Itoa(opts.PageSize)
	}
	var result InterestRateIndexPage
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/interest-rate-index", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetTradeStatistics returns 24-hour platform turnover for spot and futures.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-trade-statistics
func (c *Client) GetTradeStatistics(ctx context.Context) (*TradeStatistics, error) {
	var result TradeStatistics
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/trade-statistics", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetCallAuctionInfo returns the estimated auction price, size and order-price
// ranges for a symbol during the call-auction phase.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-call-auction-info
func (c *Client) GetCallAuctionInfo(ctx context.Context, symbol string) (*CallAuctionInfo, error) {
	var result CallAuctionInfo
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/market/call-auction-info", map[string]string{"symbol": symbol}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetFiatPrices returns quoted prices for the supplied cryptocurrency codes in
// a fiat base currency. KuCoin requires one currencies query key per asset.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/fiat-price
func (c *Client) GetFiatPrices(ctx context.Context, base string, currencies []string) (map[string]string, error) {
	query := url.Values{}
	if base != "" {
		query.Set("base", base)
	}
	for _, currency := range currencies {
		if currency != "" {
			query.Add("currencies", currency)
		}
	}
	var result map[string]string
	if _, err := c.executor.DoPublicValues(ctx, http.MethodGet, "/api/ua/v2/market/fiat-price", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetCustodyCurrencies returns settlement currencies supported by a
// third-party custodian. Both filters are optional server filters.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-oes-settlement-currency
func (c *Client) GetCustodyCurrencies(ctx context.Context, custodian, currency string) ([]CustodyCurrency, error) {
	query := map[string]string{}
	putIfSet(query, "custodian", custodian)
	putIfSet(query, "currency", currency)
	var result []CustodyCurrency
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/oes/currency", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetServiceStatus returns the current service status for a UTA product
// family.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-service-status
func (c *Client) GetServiceStatus(ctx context.Context, tradeType TradeType) (*ServiceStatus, error) {
	var result ServiceStatus
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/server/status", map[string]string{"tradeType": string(tradeType)}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetKYCRegions returns the KYC regions currently exposed by KuCoin.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-kyc-region
func (c *Client) GetKYCRegions(ctx context.Context) ([]KYCRegion, error) {
	var result []KYCRegion
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/user/kyc-region", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetClientIPAddress returns the client IP address observed by KuCoin.
//
// Docs: https://www.kucoin.com/docs-new/v2/rest/ua/get-client-ip-address
func (c *Client) GetClientIPAddress(ctx context.Context) (string, error) {
	var result string
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v2/user/my-ip", nil, &result); err != nil {
		return "", err
	}
	return result, nil
}

func optionalQuery(key, value string) map[string]string {
	if value == "" {
		return nil
	}
	return map[string]string{key: value}
}

func putIfSet(query map[string]string, key, value string) {
	if value != "" {
		query[key] = value
	}
}

func putInt64IfSet(query map[string]string, key string, value int64) {
	if value != 0 {
		query[key] = strconv.FormatInt(value, 10)
	}
}

func putIntIfPositive(query map[string]string, key string, value int) {
	if value > 0 {
		query[key] = strconv.Itoa(value)
	}
}
