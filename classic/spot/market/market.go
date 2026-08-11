package market

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/tigusigalpa/kucoin-go/transport"
)

// Client provides KuCoin Classic Spot's market-data endpoints. All
// methods except GetFullOrderBook work without credentials.
type Client struct {
	executor *transport.Executor
}

// NewClient wires a market.Client to the shared Classic Executor. Not
// normally called directly; use kucoin.NewClient.
func NewClient(executor *transport.Executor) *Client {
	return &Client{executor: executor}
}

// GetCurrency returns metadata and supported chains for a single
// currency, optionally filtered to one chain.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-currency
func (c *Client) GetCurrency(ctx context.Context, currency, chain string) (*Currency, error) {
	query := map[string]string{}
	if chain != "" {
		query["chain"] = chain
	}
	var result Currency
	path := "/api/v3/currencies/" + currency
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, path, query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetAllSymbols returns trading-pair specifications, optionally filtered
// to a market (e.g. "ALTS", "USDS", "ETF").
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-all-symbols
func (c *Client) GetAllSymbols(ctx context.Context, market string) ([]Symbol, error) {
	query := map[string]string{}
	if market != "" {
		query["market"] = market
	}
	var result []Symbol
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v2/symbols", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetTicker returns the Level-1 snapshot (best bid/ask + last trade) for
// a symbol.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-ticker
func (c *Client) GetTicker(ctx context.Context, symbol string) (*Ticker, error) {
	var result Ticker
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/market/orderbook/level1", map[string]string{"symbol": symbol}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetAllTickers returns 24h statistics for every symbol (refreshed every
// 2 seconds server-side).
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-all-tickers
func (c *Client) GetAllTickers(ctx context.Context) (*AllTickers, error) {
	var result AllTickers
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/market/allTickers", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Interval is a kline granularity accepted by GetKlines.
type Interval = string

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

// GetKlines returns OHLCV candles for a symbol/interval. startAt/endAt
// are Unix seconds; pass 0 for the server default. Returns up to 1500
// records per call.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-klines
func (c *Client) GetKlines(ctx context.Context, symbol string, interval Interval, startAt, endAt int64) ([]Kline, error) {
	query := map[string]string{"symbol": symbol, "type": interval}
	if startAt != 0 {
		query["startAt"] = strconv.FormatInt(startAt, 10)
	}
	if endAt != 0 {
		query["endAt"] = strconv.FormatInt(endAt, 10)
	}
	var result []Kline
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/market/candles", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetPartOrderBook returns a bid/ask depth snapshot capped at size levels
// per side. size must be 20 or 100.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-part-orderbook
func (c *Client) GetPartOrderBook(ctx context.Context, symbol string, size int) (*OrderBook, error) {
	if size != 20 && size != 100 {
		return nil, fmt.Errorf("kucoin: GetPartOrderBook: size must be 20 or 100, got %d", size)
	}
	var result OrderBook
	path := fmt.Sprintf("/api/v1/market/orderbook/level2_%d", size)
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, path, map[string]string{"symbol": symbol}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetFullOrderBook returns the complete bid/ask depth snapshot. Unlike
// every other method on this service, this endpoint requires credentials
// (confirmed: General permission, Spot rate-limit pool) despite being
// market data. For live updates, prefer the WebSocket incremental feed
// over polling this endpoint.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-full-orderbook
func (c *Client) GetFullOrderBook(ctx context.Context, symbol string) (*OrderBook, error) {
	var result OrderBook
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/v3/market/orderbook/level2", map[string]string{"symbol": symbol}, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetServerTime returns KuCoin's current server time in Unix milliseconds.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-server-time
func (c *Client) GetServerTime(ctx context.Context) (int64, error) {
	var result int64
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/timestamp", nil, &result); err != nil {
		return 0, err
	}
	return result, nil
}
