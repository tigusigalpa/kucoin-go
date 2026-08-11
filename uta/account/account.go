package account

import (
	"context"
	"net/http"
	"strconv"

	"github.com/tigusigalpa/kucoin-go/transport"
)

// Client provides KuCoin UTA's private account endpoints. Every method
// requires credentials; calls return transport.ErrCredentialsRequired
// locally (no network call) if none are configured.
type Client struct {
	executor *transport.Executor
}

// NewClient wires an account.Client to the shared UTA Executor. Not
// normally called directly; use kucoin.NewClient.
func NewClient(executor *transport.Executor) *Client {
	return &Client{executor: executor}
}

// GetOverview returns the unified account's aggregate risk/margin
// snapshot.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/account/get-account-overview-uta
func (c *Client) GetOverview(ctx context.Context) (*Overview, error) {
	var result Overview
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/ua/v1/unified/account/overview", nil, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetAssets returns the unified account's per-coin balance snapshot. Use
// Assets.Currencies() to read the balance list.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-account-currency-assets-uta
func (c *Client) GetAssets(ctx context.Context) (*Assets, error) {
	var result Assets
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/ua/v1/unified/account/balance", nil, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetFeeRate returns the maker/taker fee rate for a product family and
// symbol list. tradeType is one of "SPOT"/"FUTURES"; symbol accepts up to
// 10 comma-separated symbols for SPOT, exactly 1 for FUTURES.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-actual-fee
func (c *Client) GetFeeRate(ctx context.Context, tradeType, symbol string) (*FeeRateList, error) {
	query := map[string]string{"tradeType": tradeType, "symbol": symbol}
	var result FeeRateList
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/ua/v1/user/fee-rate", query, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetLedgerOptions are the optional filters for GetLedger.
type GetLedgerOptions struct {
	Currency     string
	Direction    string // "IN" or "OUT"
	BusinessType string
	LastID       int64
	StartAt      int64
	EndAt        int64
	PageSize     int
}

// GetLedger returns account ledger entries for accountType (one of
// "FUNDING"/"SPOT"/"FUTURES"/"CROSS"/"ISOLATED"/"UNIFIED"). The query
// window is capped at 1 day per call and 7 days total lookback.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-account-ledger
func (c *Client) GetLedger(ctx context.Context, accountType string, opts GetLedgerOptions) (*LedgerPage, error) {
	query := map[string]string{"accountType": accountType}
	if opts.Currency != "" {
		query["currency"] = opts.Currency
	}
	if opts.Direction != "" {
		query["direction"] = opts.Direction
	}
	if opts.BusinessType != "" {
		query["businessType"] = opts.BusinessType
	}
	if opts.LastID != 0 {
		query["lastId"] = strconv.FormatInt(opts.LastID, 10)
	}
	if opts.StartAt != 0 {
		query["startAt"] = strconv.FormatInt(opts.StartAt, 10)
	}
	if opts.EndAt != 0 {
		query["endAt"] = strconv.FormatInt(opts.EndAt, 10)
	}
	if opts.PageSize > 0 {
		query["pageSize"] = strconv.Itoa(opts.PageSize)
	}
	var result LedgerPage
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/ua/v1/account/ledger", query, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetMode returns the account's Classic/Unified mode and any linked
// sub-accounts.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-account-mode
func (c *Client) GetMode(ctx context.Context) (*Mode, error) {
	var result Mode
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/ua/v1/account/mode", nil, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetAPIKeyInfo returns metadata about the calling API key, including its
// KC-API-KEY-VERSION (APIKeyInfo.ApiVersion) and granted permissions.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-apikey-info
func (c *Client) GetAPIKeyInfo(ctx context.Context) (*APIKeyInfo, error) {
	var result APIKeyInfo
	if _, err := c.executor.Do(ctx, http.MethodGet, "/api/ua/v1/user/api-key", nil, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
