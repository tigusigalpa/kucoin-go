package debit

import (
	"context"
	"net/http"
	"strconv"

	"github.com/tigusigalpa/kucoin-go/transport"
)

// Client provides KuCoin Classic Margin's borrow/repay/interest and
// leverage-modification endpoints. GetBorrowRate works without
// credentials; every other method requires them and returns
// transport.ErrCredentialsRequired locally (no network call) if none are
// configured.
type Client struct {
	executor *transport.Executor
}

// NewClient wires a debit.Client to the shared Classic Executor. Not
// normally called directly; use kucoin.NewClient.
func NewClient(executor *transport.Executor) *Client {
	return &Client{executor: executor}
}

// GetBorrowRate returns current hourly/annualized borrow rates,
// optionally filtered by VIP level and/or a comma-separated currency
// list (up to 50 currencies).
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/debit/get-borrow-interest-rate
func (c *Client) GetBorrowRate(ctx context.Context, vipLevel int, currency string) (*BorrowRatePage, error) {
	query := map[string]string{}
	if vipLevel > 0 {
		query["vipLevel"] = strconv.Itoa(vipLevel)
	}
	if currency != "" {
		query["currency"] = currency
	}
	var result BorrowRatePage
	if _, err := c.executor.DoPublic(ctx, http.MethodGet, "/api/v3/margin/borrowRate", query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Borrow submits a new borrow order.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/debit/borrow
func (c *Client) Borrow(ctx context.Context, req BorrowRequest) (*BorrowRef, error) {
	var result BorrowRef
	if _, err := c.executor.Do(ctx, http.MethodPost, "/api/v3/margin/borrow", nil, req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func historyQuery(currency string, opts HistoryOptions) map[string]string {
	query := map[string]string{}
	if currency != "" {
		query["currency"] = currency
	}
	if opts.IsIsolated {
		query["isIsolated"] = "true"
	}
	if opts.Symbol != "" {
		query["symbol"] = opts.Symbol
	}
	if opts.OrderNo != "" {
		query["orderNo"] = opts.OrderNo
	}
	if opts.StartTime != 0 {
		query["startTime"] = strconv.FormatInt(opts.StartTime, 10)
	}
	if opts.EndTime != 0 {
		query["endTime"] = strconv.FormatInt(opts.EndTime, 10)
	}
	if opts.CurrentPage > 0 {
		query["currentPage"] = strconv.Itoa(opts.CurrentPage)
	}
	if opts.PageSize > 0 {
		query["pageSize"] = strconv.Itoa(opts.PageSize)
	}
	return query
}

// GetBorrowHistory lists historical borrow orders for a currency.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/debit/get-borrow-history
func (c *Client) GetBorrowHistory(ctx context.Context, currency string, opts HistoryOptions) (*BorrowHistoryPage, error) {
	var result BorrowHistoryPage
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/v3/margin/borrow", historyQuery(currency, opts), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Repay submits a new repayment.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/debit/repay
func (c *Client) Repay(ctx context.Context, req RepayRequest) (*RepayRef, error) {
	var result RepayRef
	if _, err := c.executor.Do(ctx, http.MethodPost, "/api/v3/margin/repay", nil, req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetRepayHistory lists historical repayments for a currency.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/debit/get-repay-history
func (c *Client) GetRepayHistory(ctx context.Context, currency string, opts HistoryOptions) (*RepayHistoryPage, error) {
	var result RepayHistoryPage
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/v3/margin/repay", historyQuery(currency, opts), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetInterestHistory lists accrued daily interest, optionally filtered by
// currency. Unlike GetBorrowHistory/GetRepayHistory, currency is
// optional here and OrderNo is not a valid filter (ignored if set).
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/debit/get-interest-history
func (c *Client) GetInterestHistory(ctx context.Context, currency string, opts HistoryOptions) (*InterestHistoryPage, error) {
	opts.OrderNo = ""
	var result InterestHistoryPage
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/v3/margin/interest", historyQuery(currency, opts), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ModifyLeverage changes cross-account or per-symbol isolated leverage.
// KuCoin returns an empty payload on success.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/debit/modify-leverage-multiplier
func (c *Client) ModifyLeverage(ctx context.Context, req ModifyLeverageRequest) error {
	_, err := c.executor.Do(ctx, http.MethodPost, "/api/v3/position/update-user-leverage", nil, req, nil)
	return err
}
