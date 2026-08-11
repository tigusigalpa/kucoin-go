package market

import (
	"context"
	"net/http"

	"github.com/tigusigalpa/kucoin-go/transport"
)

// Client provides a seed set of KuCoin Classic Margin's public
// market-data endpoints. No method requires credentials.
type Client struct {
	executor *transport.Executor
}

// NewClient wires a market.Client to the shared Classic Executor. Not
// normally called directly; use kucoin.NewClient.
func NewClient(executor *transport.Executor) *Client {
	return &Client{executor: executor}
}

// GetSymbols returns cross-margin trading-pair specifications.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/market-data/get-symbols-cross-margin
func (c *Client) GetSymbols(ctx context.Context) (*SymbolsPage, error) {
	var result SymbolsPage
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v3/margin/symbols", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetIsolatedSymbols returns isolated-margin trading-pair specifications.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/market-data/get-symbols-isolated-margin
func (c *Client) GetIsolatedSymbols(ctx context.Context) ([]IsolatedSymbol, error) {
	var result []IsolatedSymbol
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/isolated/symbols", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetMarkPriceList returns the current mark price for every margin
// symbol.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/market-data/get-mark-price-list
func (c *Client) GetMarkPriceList(ctx context.Context) ([]MarkPrice, error) {
	var result []MarkPrice
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v3/mark-price/all-symbols", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetMarkPriceDetail returns the current mark price for a single symbol.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/market-data/get-mark-price-detail
func (c *Client) GetMarkPriceDetail(ctx context.Context, symbol string) (*MarkPrice, error) {
	var result MarkPrice
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/mark-price/"+symbol+"/current", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetConfig returns account-wide margin trading limits (supported
// currencies, max leverage, warning/liquidation debt ratios).
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/market-data/get-margin-config
func (c *Client) GetConfig(ctx context.Context) (*Config, error) {
	var result Config
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v1/margin/config", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetRiskLimitCross returns cross-margin risk limits, optionally filtered
// to one currency.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/risk-limit/margin-trading-pair-configuration
func (c *Client) GetRiskLimitCross(ctx context.Context, currency string) ([]RiskLimitCrossItem, error) {
	query := map[string]string{"isIsolated": "false"}
	if currency != "" {
		query["currency"] = currency
	}
	var result []RiskLimitCrossItem
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v3/margin/currencies", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetRiskLimitIsolated returns isolated-margin risk limits, optionally
// filtered to one symbol.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/risk-limit/margin-trading-pair-configuration
func (c *Client) GetRiskLimitIsolated(ctx context.Context, symbol string) ([]RiskLimitIsolatedItem, error) {
	query := map[string]string{"isIsolated": "true"}
	if symbol != "" {
		query["symbol"] = symbol
	}
	var result []RiskLimitIsolatedItem
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v3/margin/currencies", query, &result); err != nil {
		return nil, err
	}
	return result, nil
}
