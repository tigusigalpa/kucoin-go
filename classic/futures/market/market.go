// Package market implements KuCoin Classic Futures' public market-data REST
// endpoints (contracts, tickers, order books, trades, klines, mark/index
// prices, interest-rate and premium indices, server time and service status)
// and the two public funding-rate endpoints.
//
// Prices, sizes, rates, ratios and turnovers are types.Decimal values that keep
// the exact text KuCoin sent. The live Futures API is not consistent about
// JSON numbers versus strings (prices arrive as bare numbers such as 84476.9,
// rates in Java exponent form such as 1.0E-4, order-book levels as numbers, a
// few fields as null), so every numeric field tolerates all of those forms;
// counts, sequences, timestamps and granularities are types.Int64. Order-book
// sides are []orderbook.Level.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol
package market

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/tigusigalpa/kucoin-go/transport"
	"github.com/tigusigalpa/kucoin-go/types"
)

// maxIndexCount is the largest MaxCount KuCoin documents for the paged index
// endpoints.
const maxIndexCount = 100

// ErrSymbolRequired is returned locally (no network call) when a method that
// needs a symbol is called with an empty one.
var ErrSymbolRequired = errors.New("kucoin: symbol is required")

// ErrInvalidDepth is returned locally (no network call) when GetPartOrderBook
// is called with a depth other than 20 or 100.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-part-orderbook
var ErrInvalidDepth = errors.New("kucoin: order-book depth must be 20 or 100")

// ErrInvalidGranularity is returned locally (no network call) when GetKlines is
// called with a Granularity KuCoin does not accept.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-klines
var ErrInvalidGranularity = errors.New("kucoin: unsupported kline granularity")

// ErrInvalidRange is returned locally (no network call) when
// GetPublicFundingHistory is called without both time bounds, or with a start
// after the end.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/funding-fees/get-public-funding-history
var ErrInvalidRange = errors.New("kucoin: invalid time range: from and to are required and from must not be after to")

// ErrInvalidMaxCount is returned locally (no network call) when IndexOptions
// carries a MaxCount outside 0..100.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-spot-index-price
var ErrInvalidMaxCount = errors.New("kucoin: maxCount must be between 0 and 100")

// Client provides KuCoin Classic Futures' market-data endpoints. Every method
// works without credentials except Get24hStats, which KuCoin documents as
// public but serves only to signed callers; see its comment.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/introduction
type Client struct {
	executor *transport.Executor
}

// NewClient wires a market.Client to the shared Classic Futures Executor. Not
// normally called directly; use kucoin.NewClient.
func NewClient(executor *transport.Executor) *Client {
	return &Client{executor: executor}
}

// GetSymbol returns the specification and live statistics of one contract,
// for example XBTUSDTM. The symbol is escaped into the URL path.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol
func (c *Client) GetSymbol(ctx context.Context, symbol string) (*Symbol, error) {
	if symbol == "" {
		return nil, ErrSymbolRequired
	}
	var result Symbol
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/contracts/"+url.PathEscape(symbol), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetAllSymbols returns the specification and live statistics of every
// tradable contract (about 700 at the time of writing, roughly 1.4 MB).
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-all-symbols
func (c *Client) GetAllSymbols(ctx context.Context) ([]Symbol, error) {
	var result []Symbol
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/contracts/active", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetTicker returns the last trade and the best bid/ask of one contract.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-ticker
func (c *Client) GetTicker(ctx context.Context, symbol string) (*Ticker, error) {
	if symbol == "" {
		return nil, ErrSymbolRequired
	}
	var result Ticker
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/ticker", map[string]string{"symbol": symbol}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetAllTickers returns the last trade and the best bid/ask of every contract.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-all-tickers
func (c *Client) GetAllTickers(ctx context.Context) ([]Ticker, error) {
	var result []Ticker
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/allTickers", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetFullOrderBook returns the full price-aggregated order book of one
// contract (1000 levels per side on the live API). It consumes more server
// resources than GetPartOrderBook and is rate-limited more strictly; to keep a
// book current, take this snapshot once and apply the WebSocket level2
// incremental feed on top of it.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-full-orderbook
func (c *Client) GetFullOrderBook(ctx context.Context, symbol string) (*OrderBook, error) {
	if symbol == "" {
		return nil, ErrSymbolRequired
	}
	var result OrderBook
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/level2/snapshot", map[string]string{"symbol": symbol}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetPartOrderBook returns the best depth levels per side of one contract's
// price-aggregated order book. depth must be 20 or 100, otherwise
// ErrInvalidDepth is returned without a network call. KuCoin recommends it over
// GetFullOrderBook because it is faster and lighter.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-part-orderbook
func (c *Client) GetPartOrderBook(ctx context.Context, symbol string, depth int) (*OrderBook, error) {
	if symbol == "" {
		return nil, ErrSymbolRequired
	}
	if depth != 20 && depth != 100 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidDepth, depth)
	}
	var result OrderBook
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/level2/depth"+strconv.Itoa(depth), map[string]string{"symbol": symbol}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetTradeHistory returns the last 100 public trades of one contract, newest
// first.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-trade-history
func (c *Client) GetTradeHistory(ctx context.Context, symbol string) ([]Trade, error) {
	if symbol == "" {
		return nil, ErrSymbolRequired
	}
	var result []Trade
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/trade/history", map[string]string{"symbol": symbol}, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetKlines returns candles for a contract, its index symbol or its premium
// symbols (see KlineOptions). Symbol must be set and Granularity must be one of
// the Granularity constants, otherwise ErrSymbolRequired or
// ErrInvalidGranularity is returned without a network call. A zero From or To
// leaves that bound to KuCoin. One call returns a single page of candles:
// KuCoin documents at most 500 but the live API returns at most 200, so page
// through longer ranges by time (continue from the last candle's Time) and do
// not rely on either number.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-klines
func (c *Client) GetKlines(ctx context.Context, opts KlineOptions) ([]Kline, error) {
	if opts.Symbol == "" {
		return nil, ErrSymbolRequired
	}
	if !opts.Granularity.Valid() {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidGranularity, int(opts.Granularity))
	}
	query := map[string]string{
		"symbol":      opts.Symbol,
		"granularity": strconv.Itoa(int(opts.Granularity)),
	}
	putInt64IfSet(query, "from", opts.From)
	putInt64IfSet(query, "to", opts.To)
	var result []Kline
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/kline/query", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetMarkPrice returns the current mark price and index price of one contract
// (a snapshot refreshed once per second server-side).
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-mark-price
func (c *Client) GetMarkPrice(ctx context.Context, symbol string) (*MarkPrice, error) {
	if symbol == "" {
		return nil, ErrSymbolRequired
	}
	var result MarkPrice
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/mark-price/"+url.PathEscape(symbol)+"/current", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetSpotIndexPrice returns a page of the spot index price of an index symbol
// (Symbol.IndexSymbol, for example .KXBTUSDT) with the per-exchange
// components it was built from. KuCoin keeps up to 3 months of one-second
// history. See IndexOptions for the paging parameters.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-spot-index-price
func (c *Client) GetSpotIndexPrice(ctx context.Context, opts IndexOptions) (*IndexPricePage, error) {
	query, err := indexQuery(opts)
	if err != nil {
		return nil, err
	}
	var result IndexPricePage
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/index/query", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetInterestRateIndex returns a page of an interest-rate index. The symbol is
// one of Symbol.FundingBaseSymbol, FundingQuoteSymbol, FundingBaseSymbol1M or
// FundingQuoteSymbol1M, for example .XBTINT8H or .USDTINT. See IndexOptions for
// the paging parameters.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-interest-rate-index
func (c *Client) GetInterestRateIndex(ctx context.Context, opts IndexOptions) (*InterestRatePage, error) {
	query, err := indexQuery(opts)
	if err != nil {
		return nil, err
	}
	var result InterestRatePage
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/interest/query", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetPremiumIndex returns a page of a premium index, the input of the funding
// rate. The symbol is Symbol.PremiumsSymbol1M or PremiumsSymbol8H, for example
// .XBTUSDTMPI or .XBTUSDTMPI8H. See IndexOptions for the paging parameters.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-premium-index
func (c *Client) GetPremiumIndex(ctx context.Context, opts IndexOptions) (*PremiumIndexPage, error) {
	query, err := indexQuery(opts)
	if err != nil {
		return nil, err
	}
	var result PremiumIndexPage
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/premium/query", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Get24hStats returns the platform-wide 24-hour futures trading volume.
//
// IMPORTANT: KuCoin documents this endpoint as public, but the live API
// answers an unsigned request with HTTP 400 and code 400001 ("Please check the
// header of your request for KC-API-KEY, KC-API-SIGN, ..."). This is the only
// method of the service that is not truly public. It therefore uses the
// executor's optional-authentication path: when complete credentials are
// configured the request is signed and succeeds; when they are not, the
// request is still sent unsigned and KuCoin's own error comes back as a
// *transport.KucoinError that also matches transport.ErrBadRequest, never a
// local transport.ErrCredentialsRequired. If KuCoin makes the endpoint public
// as documented, the unsigned call will start to succeed without any change.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-24hr-stats
func (c *Client) Get24hStats(ctx context.Context) (*Stats24h, error) {
	var result Stats24h
	if _, err := c.executor.DoOptional(ctx, http.MethodGet, "/api/v1/trade-statistics", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetServerTime returns KuCoin's current server time in Unix milliseconds.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-server-time
func (c *Client) GetServerTime(ctx context.Context) (int64, error) {
	var result types.Int64
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/timestamp", nil, &result); err != nil {
		return 0, err
	}
	return result.Value(), nil
}

// GetServiceStatus returns whether Futures trading is open, closed for
// maintenance, or cancel-only, with KuCoin's message.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-service-status
func (c *Client) GetServiceStatus(ctx context.Context) (*ServiceStatus, error) {
	var result ServiceStatus
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/status", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetCurrentFundingRate returns the current funding rate of one contract. The
// symbol is the contract symbol (XBTUSDTM); the live API also accepts the
// funding-rate symbol (.XBTUSDTMFPI8H), which is what FundingRate.Symbol
// carries in the response. The symbol is escaped into the URL path.
//
// This endpoint is public although KuCoin lists the Futures permission for it.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/funding-fees/get-current-funding-rate
func (c *Client) GetCurrentFundingRate(ctx context.Context, symbol string) (*FundingRate, error) {
	if symbol == "" {
		return nil, ErrSymbolRequired
	}
	var result FundingRate
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/funding-rate/"+url.PathEscape(symbol)+"/current", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetPublicFundingHistory returns the funding rate of every settlement of one
// contract between From and To. Symbol, From and To are all required by
// KuCoin, so ErrSymbolRequired or ErrInvalidRange is returned without a
// network call when one is missing or From is after To.
//
// This endpoint is public although KuCoin lists the Futures permission for it.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/funding-fees/get-public-funding-history
func (c *Client) GetPublicFundingHistory(ctx context.Context, opts FundingHistoryOptions) ([]FundingRatePoint, error) {
	if opts.Symbol == "" {
		return nil, ErrSymbolRequired
	}
	if opts.From <= 0 || opts.To <= 0 || opts.From > opts.To {
		return nil, fmt.Errorf("%w: from=%d to=%d", ErrInvalidRange, opts.From, opts.To)
	}
	query := map[string]string{
		"symbol": opts.Symbol,
		"from":   strconv.FormatInt(opts.From, 10),
		"to":     strconv.FormatInt(opts.To, 10),
	}
	var result []FundingRatePoint
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/contract/funding-rates", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// indexQuery validates IndexOptions and renders the query shared by the three
// paged index endpoints. Zero values and nil flags are left to KuCoin's
// defaults and omitted from the request.
func indexQuery(opts IndexOptions) (map[string]string, error) {
	if opts.Symbol == "" {
		return nil, ErrSymbolRequired
	}
	if opts.MaxCount < 0 || opts.MaxCount > maxIndexCount {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidMaxCount, opts.MaxCount)
	}
	query := map[string]string{"symbol": opts.Symbol}
	putInt64IfSet(query, "startAt", opts.StartAt)
	putInt64IfSet(query, "endAt", opts.EndAt)
	putBoolIfSet(query, "reverse", opts.Reverse)
	putBoolIfSet(query, "forward", opts.Forward)
	putInt64IfSet(query, "offset", opts.Offset)
	putIntIfPositive(query, "maxCount", opts.MaxCount)
	return query, nil
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

func putBoolIfSet(query map[string]string, key string, value *bool) {
	if value != nil {
		query[key] = strconv.FormatBool(*value)
	}
}
