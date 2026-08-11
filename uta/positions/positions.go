package positions

import (
	"context"
	"net/http"
	"strconv"

	"github.com/tigusigalpa/kucoin-go/transport"
)

// Client provides KuCoin UTA's private position-management endpoints.
// Every method requires credentials; calls return
// transport.ErrCredentialsRequired locally (no network call) if none are
// configured.
type Client struct {
	executor *transport.Executor
}

// NewClient wires a positions.Client to the shared UTA Executor. Not
// normally called directly; use kucoin.NewClient.
func NewClient(executor *transport.Executor) *Client {
	return &Client{executor: executor}
}

// GetPositionsOptions are the optional filters for GetPositions.
type GetPositionsOptions struct {
	Symbol     string
	PageNumber int
	PageSize   int
}

// GetPositions lists open futures positions, sorted by internal symbol
// ID. Despite accepting pagination parameters, KuCoin returns this as a
// plain array with no page-count metadata.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-position-list-uta
func (c *Client) GetPositions(ctx context.Context, opts GetPositionsOptions) ([]Position, error) {
	query := map[string]string{}
	if opts.Symbol != "" {
		query["symbol"] = opts.Symbol
	}
	if opts.PageNumber > 0 {
		query["pageNumber"] = strconv.Itoa(opts.PageNumber)
	}
	if opts.PageSize > 0 {
		query["pageSize"] = strconv.Itoa(opts.PageSize)
	}
	var result []Position
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/ua/v1/unified/position/open-list", query, nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetPositionHistoryOptions are the optional filters for
// GetPositionHistory.
type GetPositionHistoryOptions struct {
	Symbol   string
	StartAt  int64 // Unix milliseconds
	EndAt    int64 // Unix milliseconds
	LastID   int64
	PageSize int
}

// GetPositionHistory lists closed positions from the last 3 months (max
// 7 days per query).
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-position-history-uta
func (c *Client) GetPositionHistory(ctx context.Context, opts GetPositionHistoryOptions) (*PositionHistoryPage, error) {
	query := map[string]string{}
	if opts.Symbol != "" {
		query["symbol"] = opts.Symbol
	}
	if opts.StartAt != 0 {
		query["startAt"] = strconv.FormatInt(opts.StartAt, 10)
	}
	if opts.EndAt != 0 {
		query["endAt"] = strconv.FormatInt(opts.EndAt, 10)
	}
	if opts.LastID != 0 {
		query["lastId"] = strconv.FormatInt(opts.LastID, 10)
	}
	if opts.PageSize > 0 {
		query["pageSize"] = strconv.Itoa(opts.PageSize)
	}
	var result PositionHistoryPage
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/ua/v1/position/history", query, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetFundingFeeHistoryOptions are the optional filters for
// GetFundingFeeHistory. The startAt/endAt span is capped at 90 days.
type GetFundingFeeHistoryOptions struct {
	Symbol   string
	StartAt  int64 // Unix milliseconds
	EndAt    int64 // Unix milliseconds
	LastID   int64
	PageSize int
}

// GetFundingFeeHistory lists funding-fee settlement records.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-private-funding-fee-history
func (c *Client) GetFundingFeeHistory(ctx context.Context, opts GetFundingFeeHistoryOptions) (*FundingFeeHistoryPage, error) {
	query := map[string]string{}
	if opts.Symbol != "" {
		query["symbol"] = opts.Symbol
	}
	if opts.StartAt != 0 {
		query["startAt"] = strconv.FormatInt(opts.StartAt, 10)
	}
	if opts.EndAt != 0 {
		query["endAt"] = strconv.FormatInt(opts.EndAt, 10)
	}
	if opts.LastID != 0 {
		query["lastId"] = strconv.FormatInt(opts.LastID, 10)
	}
	if opts.PageSize > 0 {
		query["pageSize"] = strconv.Itoa(opts.PageSize)
	}
	var result FundingFeeHistoryPage
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/ua/v1/position/funding-history", query, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// BatchModifyMarginMode changes margin mode for one or more symbols
// (comma-separated in req.Symbol) at once.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/batch-modify-margin-mode
func (c *Client) BatchModifyMarginMode(ctx context.Context, req BatchModifyMarginModeRequest) (*BatchModifyMarginModeResult, error) {
	var result BatchModifyMarginModeResult
	if _, err := c.executor.Do(ctx, http.MethodPost, "/api/ua/v1/unified/position/margin-mode", nil, req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ModifyPositionMargin deposits or withdraws margin from a position.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/modify-isolated-futures-margin
func (c *Client) ModifyPositionMargin(ctx context.Context, req ModifyPositionMarginRequest) (*ModifyPositionMarginResult, error) {
	var result ModifyPositionMarginResult
	if _, err := c.executor.Do(ctx, http.MethodPost, "/api/ua/v1/unified/position/modify-margin", nil, req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetMarginMode returns the current margin mode for one symbol, or every
// symbol if symbol is empty.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-margin-mode
func (c *Client) GetMarginMode(ctx context.Context, symbol string) (*MarginModeList, error) {
	query := map[string]string{}
	if symbol != "" {
		query["symbol"] = symbol
	}
	var result MarginModeList
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/ua/v1/unified/position/margin-mode", query, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
