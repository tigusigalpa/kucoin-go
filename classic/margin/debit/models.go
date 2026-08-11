// Package debit implements KuCoin Classic Margin's borrow/repay/interest
// endpoints (the "Debit" sub-domain) plus leverage modification. Isolated
// vs cross margin is discriminated by an IsIsolated bool + a Symbol that
// becomes required only when IsIsolated is true — a different convention
// from package orders' TradeType enum, matching KuCoin's own
// inconsistency between the two sub-domains.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/debit/introduction
package debit

// BorrowRateItem is one currency's current hourly/annualized borrow
// rate.
type BorrowRateItem struct {
	Currency             string `json:"currency"`
	HourlyBorrowRate     string `json:"hourlyBorrowRate"`
	AnnualizedBorrowRate string `json:"annualizedBorrowRate"`
}

// BorrowRatePage is the envelope returned by GetBorrowRate.
type BorrowRatePage struct {
	VipLevel int              `json:"vipLevel"`
	Items    []BorrowRateItem `json:"items"`
}

// BorrowRequest submits a new borrow order.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/debit/borrow
type BorrowRequest struct {
	Currency    string `json:"currency"`
	Size        string `json:"size"`
	TimeInForce string `json:"timeInForce"`      // IOC, FOK
	Symbol      string `json:"symbol,omitempty"` // required if IsIsolated
	IsIsolated  bool   `json:"isIsolated,omitempty"`
	IsHf        bool   `json:"isHf,omitempty"`
}

// BorrowRef confirms a submitted borrow order.
type BorrowRef struct {
	OrderNo    string `json:"orderNo"`
	ActualSize string `json:"actualSize"`
}

// HistoryOptions are the shared optional filters for GetBorrowHistory,
// GetRepayHistory, and GetInterestHistory.
type HistoryOptions struct {
	IsIsolated  bool
	Symbol      string // required if IsIsolated
	OrderNo     string // ignored by GetInterestHistory
	StartTime   int64  // Unix milliseconds; borrow/repay/interest history floors at 1680278400000 (2023-04-01) regardless of this value
	EndTime     int64  // Unix milliseconds
	CurrentPage int
	PageSize    int // default 50, range 10-500
}

// BorrowHistoryItem is one historical borrow order. Symbol is empty for
// cross-margin borrows (KuCoin sends null on the wire).
type BorrowHistoryItem struct {
	OrderNo     string `json:"orderNo"`
	Symbol      string `json:"symbol"`
	Currency    string `json:"currency"`
	Size        string `json:"size"`
	ActualSize  string `json:"actualSize"`
	Status      string `json:"status"` // PENDING, SUCCESS, FAILED
	CreatedTime int64  `json:"createdTime"`
}

// BorrowHistoryPage is the page-paginated envelope returned by
// GetBorrowHistory.
type BorrowHistoryPage struct {
	Timestamp   int64               `json:"timestamp"`
	CurrentPage int                 `json:"currentPage"`
	PageSize    int                 `json:"pageSize"`
	TotalNum    int                 `json:"totalNum"`
	TotalPage   int                 `json:"totalPage"`
	Items       []BorrowHistoryItem `json:"items"`
}

// RepayRequest submits a new repayment.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/debit/repay
type RepayRequest struct {
	Currency   string `json:"currency"`
	Size       string `json:"size"`
	Symbol     string `json:"symbol,omitempty"` // required if IsIsolated
	IsIsolated bool   `json:"isIsolated,omitempty"`
	IsHf       bool   `json:"isHf,omitempty"`
}

// RepayRef confirms a submitted repayment.
type RepayRef struct {
	Timestamp  int64  `json:"timestamp"`
	OrderNo    string `json:"orderNo"`
	ActualSize string `json:"actualSize"`
}

// RepayHistoryItem is one historical repayment. Symbol is empty for
// cross-margin repayments (KuCoin sends null on the wire).
type RepayHistoryItem struct {
	OrderNo     string `json:"orderNo"`
	Symbol      string `json:"symbol"`
	Currency    string `json:"currency"`
	Size        string `json:"size"`
	Principal   string `json:"principal"`
	Interest    string `json:"interest"`
	Status      string `json:"status"` // PENDING, SUCCESS, FAILED
	CreatedTime int64  `json:"createdTime"`
}

// RepayHistoryPage is the page-paginated envelope returned by
// GetRepayHistory.
type RepayHistoryPage struct {
	Timestamp   int64              `json:"timestamp"`
	CurrentPage int                `json:"currentPage"`
	PageSize    int                `json:"pageSize"`
	TotalNum    int                `json:"totalNum"`
	TotalPage   int                `json:"totalPage"`
	Items       []RepayHistoryItem `json:"items"`
}

// InterestHistoryItem is one day's accrued interest for a currency.
type InterestHistoryItem struct {
	Currency       string `json:"currency"`
	DayRatio       string `json:"dayRatio"`
	InterestAmount string `json:"interestAmount"`
	CreatedTime    int64  `json:"createdTime"`
}

// InterestHistoryPage is the page-paginated envelope returned by
// GetInterestHistory.
type InterestHistoryPage struct {
	Timestamp   int64                 `json:"timestamp"`
	CurrentPage int                   `json:"currentPage"`
	PageSize    int                   `json:"pageSize"`
	TotalNum    int                   `json:"totalNum"`
	TotalPage   int                   `json:"totalPage"`
	Items       []InterestHistoryItem `json:"items"`
}

// ModifyLeverageRequest changes account (cross) or symbol (isolated)
// leverage. KuCoin returns an empty payload on success — ModifyLeverage
// returns only an error.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/debit/modify-leverage-multiplier
type ModifyLeverageRequest struct {
	Leverage   string `json:"leverage"`         // > 1, up to 2 decimals
	Symbol     string `json:"symbol,omitempty"` // required if IsIsolated
	IsIsolated bool   `json:"isIsolated,omitempty"`
}
