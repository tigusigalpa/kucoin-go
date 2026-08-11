package leverage

import (
	"context"
	"net/http"

	"github.com/tigusigalpa/kucoin-go/transport"
)

// Client provides KuCoin UTA's private leverage-management endpoints.
// Every method requires credentials with the "Unified" permission; calls
// return transport.ErrCredentialsRequired locally (no network call) if no
// credentials are configured.
type Client struct {
	executor *transport.Executor
}

// NewClient wires a leverage.Client to the shared UTA Executor. Not
// normally called directly; use kucoin.NewClient.
func NewClient(executor *transport.Executor) *Client {
	return &Client{executor: executor}
}

// ModifyFuturesLeverage sets futures leverage for a symbol.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/modify-leverage-uta
func (c *Client) ModifyFuturesLeverage(ctx context.Context, req ModifyFuturesLeverageRequest) (*FuturesLeverageResult, error) {
	var result FuturesLeverageResult
	if _, err := c.executor.Do(ctx, http.MethodPost, "/api/ua/v1/unified/account/modify-leverage", nil, req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ModifyCrossMarginLeverage sets cross-margin leverage.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/modify-cross-margin-leverage-uta
func (c *Client) ModifyCrossMarginLeverage(ctx context.Context, req ModifyCrossMarginLeverageRequest) (*CrossMarginLeverageResult, error) {
	var result CrossMarginLeverageResult
	if _, err := c.executor.Do(ctx, http.MethodPost, "/api/ua/v1/unified/account/modify-leverage-margin-cross", nil, req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetLeverage returns current leverage entries. tradeType is required
// ("MARGIN" or "FUTURES"); currency is required when tradeType=MARGIN,
// symbol is required when tradeType=FUTURES — omitting the relevant one
// returns every entry for that trade type. marginMode is optional.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-leverage
func (c *Client) GetLeverage(ctx context.Context, tradeType, currency, symbol, marginMode string) ([]LeverageEntry, error) {
	query := map[string]string{"tradeType": tradeType}
	if currency != "" {
		query["currency"] = currency
	}
	if symbol != "" {
		query["symbol"] = symbol
	}
	if marginMode != "" {
		query["marginMode"] = marginMode
	}
	var result []LeverageEntry
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/ua/v1/unified/account/leverage", query, nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}
