// Package orders implements KuCoin Classic Margin's private
// order-management endpoints, all under the /api/v3/hf/margin/...
// path family — a different version AND path family from Classic
// Spot's /api/v1/hf/... orders, confirmed across every endpoint here.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/orders/add-order
package orders

// TradeType discriminates cross vs isolated margin on the
// listing/cancel-all endpoints (a separate convention from the
// isIsolated boolean used on PlaceOrderRequest and package debit).
const (
	TradeTypeMargin         = "MARGIN_TRADE"
	TradeTypeMarginIsolated = "MARGIN_ISOLATED_TRADE"
)

// PlaceOrderRequest submits a new Classic Margin order. clientOid is
// never generated automatically by this SDK.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/orders/add-order
type PlaceOrderRequest struct {
	ClientOid   string `json:"clientOid"`
	Symbol      string `json:"symbol"`
	Side        string `json:"side"`                  // buy, sell
	Type        string `json:"type,omitempty"`        // limit (default), market
	Price       string `json:"price,omitempty"`       // required for limit
	Size        string `json:"size,omitempty"`        // required for limit; market: size or funds
	Funds       string `json:"funds,omitempty"`       // market orders only, alternative to size
	TimeInForce string `json:"timeInForce,omitempty"` // GTC (default), GTT, IOC, FOK
	CancelAfter int64  `json:"cancelAfter,omitempty"` // seconds; used with GTT
	PostOnly    bool   `json:"postOnly,omitempty"`
	Stp         string `json:"stp,omitempty"` // CN, CO, CB, DC
	IsIsolated  bool   `json:"isIsolated,omitempty"`
	AutoBorrow  bool   `json:"autoBorrow,omitempty"`
	AutoRepay   bool   `json:"autoRepay,omitempty"`
}

// OrderRef identifies a newly placed order. BorrowSize/LoanApplyID are
// only populated when AutoBorrow was set on the request.
type OrderRef struct {
	OrderID     string `json:"orderId"`
	ClientOid   string `json:"clientOid"`
	BorrowSize  string `json:"borrowSize,omitempty"`
	LoanApplyID string `json:"loanApplyId,omitempty"`
}

// CancelByOrderIDResult confirms a cancellation by order ID.
type CancelByOrderIDResult struct {
	OrderID string `json:"orderId"`
}

// CancelByClientOidResult confirms a cancellation by client order ID.
type CancelByClientOidResult struct {
	ClientOid string `json:"clientOid"`
}

// Order is the canonical Classic Margin order record, returned by
// GetOrderByID/GetOrderByClientOid and embedded in GetOpenOrders/
// GetClosedOrders. CancelReason is only present once an order has been
// cancelled (absent, decoding as zero, on open orders).
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/orders/get-order-by-orderid
type Order struct {
	ID             string `json:"id"`
	ClientOid      string `json:"clientOid"`
	Symbol         string `json:"symbol"`
	OpType         string `json:"opType"`
	Type           string `json:"type"` // limit, market
	Side           string `json:"side"` // buy, sell
	Price          string `json:"price"`
	Size           string `json:"size"`
	Funds          string `json:"funds"`
	DealSize       string `json:"dealSize"`
	DealFunds      string `json:"dealFunds"`
	CancelledSize  string `json:"cancelledSize"`
	CancelledFunds string `json:"cancelledFunds"`
	RemainSize     string `json:"remainSize"`
	RemainFunds    string `json:"remainFunds"`
	Fee            string `json:"fee"`
	FeeCurrency    string `json:"feeCurrency"`
	Stp            string `json:"stp,omitempty"`
	Stop           string `json:"stop,omitempty"`
	StopTriggered  bool   `json:"stopTriggered"`
	StopPrice      string `json:"stopPrice,omitempty"`
	TimeInForce    string `json:"timeInForce"`
	PostOnly       bool   `json:"postOnly"`
	Hidden         bool   `json:"hidden"`
	Iceberg        bool   `json:"iceberg"`
	VisibleSize    string `json:"visibleSize"`
	CancelAfter    int64  `json:"cancelAfter"`
	Channel        string `json:"channel"`
	Remark         string `json:"remark,omitempty"`
	Tags           string `json:"tags,omitempty"`
	CancelExist    bool   `json:"cancelExist"`
	TradeType      string `json:"tradeType"`
	InOrderBook    bool   `json:"inOrderBook"`
	Active         bool   `json:"active"`
	Tax            string `json:"tax"`
	CreatedAt      int64  `json:"createdAt"`
	LastUpdatedAt  int64  `json:"lastUpdatedAt"`
	CancelReason   int    `json:"cancelReason,omitempty"`
}

// ClosedOrderPage is the cursor-paginated envelope returned by
// GetClosedOrders.
type ClosedOrderPage struct {
	LastID int64   `json:"lastId"`
	Items  []Order `json:"items"`
}

// Fill is a single trade execution record.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/orders/get-trade-history
type Fill struct {
	ID             int64  `json:"id"`
	Symbol         string `json:"symbol"`
	TradeID        int64  `json:"tradeId"`
	OrderID        string `json:"orderId"`
	CounterOrderID string `json:"counterOrderId"`
	Side           string `json:"side"`
	Liquidity      string `json:"liquidity"` // taker, maker
	ForceTaker     bool   `json:"forceTaker"`
	Price          string `json:"price"`
	Size           string `json:"size"`
	Funds          string `json:"funds"`
	Fee            string `json:"fee"`
	FeeRate        string `json:"feeRate"`
	FeeCurrency    string `json:"feeCurrency"`
	Stop           string `json:"stop,omitempty"`
	TradeType      string `json:"tradeType"`
	Tax            string `json:"tax"`
	TaxRate        string `json:"taxRate"`
	Type           string `json:"type"`
	CreatedAt      int64  `json:"createdAt"`
}

// FillPage is the cursor-paginated envelope returned by GetTradeHistory.
type FillPage struct {
	Items  []Fill `json:"items"`
	LastID int64  `json:"lastId"`
}
