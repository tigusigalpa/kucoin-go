package positions

import (
	"context"
	"net/http"

	"github.com/tigusigalpa/kucoin-go/transport"
)

// Client provides a seed set of KuCoin Classic Futures' private
// position-related endpoints. Every method requires credentials with the
// General permission; calls return transport.ErrCredentialsRequired
// locally (no network call) if none are configured.
type Client struct {
	executor *transport.Executor
}

// NewClient wires a positions.Client to the shared Classic Futures
// Executor. Not normally called directly; use kucoin.NewClient.
func NewClient(executor *transport.Executor) *Client {
	return &Client{executor: executor}
}

// GetPositionDetails returns the position(s) for a symbol. KuCoin wraps
// this in a JSON array despite the endpoint's singular name/description
// — see the docblock on Details.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-position-details
func (c *Client) GetPositionDetails(ctx context.Context, symbol string) ([]Details, error) {
	var result []Details
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/v2/position", map[string]string{"symbol": symbol}, nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetPositionList lists open positions, optionally filtered to a
// settlement currency (e.g. "USDT", "XBT"); omit to list every position.
// Uses the older /api/v1/positions schema — see the docblock on ListItem
// for how it differs from GetPositionDetails.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-position-list
func (c *Client) GetPositionList(ctx context.Context, currency string) ([]ListItem, error) {
	query := map[string]string{}
	if currency != "" {
		query["currency"] = currency
	}
	var result []ListItem
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/v1/positions", query, nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetMarginMode returns a single symbol's current margin mode.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-margin-mode
func (c *Client) GetMarginMode(ctx context.Context, symbol string) (*MarginMode, error) {
	var result MarginMode
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/v2/position/getMarginMode", map[string]string{"symbol": symbol}, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetPositionMode returns the account-wide position mode (applies to
// every futures symbol, not per-symbol like margin mode).
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-position-mode
func (c *Client) GetPositionMode(ctx context.Context) (*PositionMode, error) {
	var result PositionMode
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/v2/position/getPositionMode", nil, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
