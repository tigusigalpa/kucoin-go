package market

import (
	"context"
	"net/http"
	"strconv"

	"github.com/tigusigalpa/kucoin-go/transport"
)

// Client provides KuCoin UTA's public market-data endpoints. All methods
// work without credentials.
type Client struct {
	executor *transport.Executor
}

// NewClient wires a market.Client to the shared UTA Executor. Not
// normally called directly; use kucoin.NewClient.
func NewClient(executor *transport.Executor) *Client {
	return &Client{executor: executor}
}

// GetInstruments returns trading-pair specifications for a product
// family, optionally filtered to a single symbol.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-symbol
func (c *Client) GetInstruments(ctx context.Context, tradeType TradeType, symbol string) (*InstrumentList, error) {
	query := map[string]string{"tradeType": tradeType}
	if symbol != "" {
		query["symbol"] = symbol
	}
	var result InstrumentList
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v1/market/instrument", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetTickers returns best-bid/ask plus 24h statistics for a product
// family, optionally filtered to a single symbol.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-ticker
func (c *Client) GetTickers(ctx context.Context, tradeType TradeType, symbol string) (*TickerList, error) {
	query := map[string]string{"tradeType": tradeType}
	if symbol != "" {
		query["symbol"] = symbol
	}
	var result TickerList
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v1/market/ticker", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetOrderBookOptions are the optional filters for GetOrderBook.
type GetOrderBookOptions struct {
	// Limit is the depth level, e.g. 20 or 100. Zero uses the server
	// default.
	Limit int
	// RPIFilter is a futures-only retail-price-improvement filter.
	RPIFilter int
}

// GetOrderBook returns the bid/ask depth snapshot for a symbol. Unlike
// every sibling method on this service, this endpoint requires
// credentials — confirmed by a live call returning HTTP 400 "Please check
// the header of your request for KC-API-KEY, KC-API-SIGN,
// KC-API-TIMESTAMP, KC-API-PASSPHRASE" when unauthenticated, despite being
// documented alongside public market-data endpoints.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-orderbook
func (c *Client) GetOrderBook(ctx context.Context, tradeType TradeType, symbol string, opts GetOrderBookOptions) (*OrderBook, error) {
	query := map[string]string{"tradeType": tradeType, "symbol": symbol}
	if opts.Limit > 0 {
		query["limit"] = strconv.Itoa(opts.Limit)
	}
	if opts.RPIFilter > 0 {
		query["rpiFilter"] = strconv.Itoa(opts.RPIFilter)
	}
	var result OrderBook
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/ua/v1/market/orderbook", query, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetKlines returns OHLCV candles for a symbol and interval within
// [startAt, endAt] (Unix seconds). Spot returns up to 1500 records per
// call with unlimited history; Futures returns up to 200 records per
// call, and sub-8h intervals are limited to 2025-01-01 onward.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-klines
func (c *Client) GetKlines(ctx context.Context, tradeType TradeType, symbol, interval string, startAt, endAt int64) ([]Kline, error) {
	query := map[string]string{
		"tradeType": tradeType,
		"symbol":    symbol,
		"interval":  interval,
		"startAt":   strconv.FormatInt(startAt, 10),
		"endAt":     strconv.FormatInt(endAt, 10),
	}
	var result []Kline
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v1/market/kline", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetTrades returns the ~100 most recent public trades for a symbol.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-trades
func (c *Client) GetTrades(ctx context.Context, tradeType TradeType, symbol string) (*TradeList, error) {
	query := map[string]string{"tradeType": tradeType, "symbol": symbol}
	var result TradeList
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v1/market/trade", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetCurrencies returns currency/chain metadata, optionally filtered to a
// comma-separated currency list and/or chain.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-currencies
func (c *Client) GetCurrencies(ctx context.Context, currencyList, chain string) ([]Currency, error) {
	query := map[string]string{}
	if currencyList != "" {
		query["currencyList"] = currencyList
	}
	if chain != "" {
		query["chain"] = chain
	}
	var result []Currency
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v1/asset/currencies", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetCurrency returns currency/chain metadata for a single currency.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-currency
func (c *Client) GetCurrency(ctx context.Context, currency, chain string) ([]Currency, error) {
	query := map[string]string{}
	if currency != "" {
		query["currency"] = currency
	}
	if chain != "" {
		query["chain"] = chain
	}
	var result []Currency
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v1/market/currency", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetServiceStatus reports whether trading is currently open for a
// product family.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-service-status
func (c *Client) GetServiceStatus(ctx context.Context, tradeType TradeType) (*ServiceStatus, error) {
	query := map[string]string{"tradeType": tradeType}
	var result ServiceStatus
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v1/server/status", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetAnnouncementsOptions are the optional filters for GetAnnouncements.
// The server defaults to the past month when StartTime/EndTime are zero.
type GetAnnouncementsOptions struct {
	Language   string
	Type       string
	PageNumber int
	PageSize   int
	StartTime  int64
	EndTime    int64
}

// GetAnnouncements returns paginated platform announcements.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-announcements
func (c *Client) GetAnnouncements(ctx context.Context, opts GetAnnouncementsOptions) (*AnnouncementPage, error) {
	query := map[string]string{}
	if opts.Language != "" {
		query["language"] = opts.Language
	}
	if opts.Type != "" {
		query["type"] = opts.Type
	}
	if opts.PageNumber > 0 {
		query["pageNumber"] = strconv.Itoa(opts.PageNumber)
	}
	if opts.PageSize > 0 {
		query["pageSize"] = strconv.Itoa(opts.PageSize)
	}
	if opts.StartTime > 0 {
		query["startTime"] = strconv.FormatInt(opts.StartTime, 10)
	}
	if opts.EndTime > 0 {
		query["endTime"] = strconv.FormatInt(opts.EndTime, 10)
	}
	var result AnnouncementPage
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v1/market/announcement", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetTradeStatistics returns 24h platform-wide turnover for spot and
// futures.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-trade-statistics
func (c *Client) GetTradeStatistics(ctx context.Context) (*TradeStatistics, error) {
	var result TradeStatistics
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/ua/v1/trade-statistics", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
