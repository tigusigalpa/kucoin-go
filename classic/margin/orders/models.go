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

// CancelOrderIDsResult confirms one or more cancellations. Returned by
// every stop-order and OCO-order cancel method in this package.
type CancelOrderIDsResult struct {
	CancelledOrderIDs []string `json:"cancelledOrderIds"`
}

// StopOrderRequest submits a new Classic Margin stop order — a
// distinct, older order family from PlaceOrderRequest's core HF orders
// (path /api/v3/hf/margin/stop-order), sharing the same host and
// signing scheme. Unlike PlaceOrderRequest, ClientOid is optional here.
// Unlike Classic Spot's stop orders, margin mode is selected via
// IsIsolated (not a TradeType query param) on this Add endpoint.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/orders/add-stop-order
type StopOrderRequest struct {
	Symbol      string `json:"symbol"`
	Side        string `json:"side"`           // buy, sell
	Type        string `json:"type,omitempty"` // limit (default), market
	StopPrice   string `json:"stopPrice"`
	Price       string `json:"price,omitempty"` // required for limit
	Size        string `json:"size,omitempty"`  // required for limit; market: size or funds
	Funds       string `json:"funds,omitempty"` // market orders only, alternative to size
	ClientOid   string `json:"clientOid,omitempty"`
	Remark      string `json:"remark,omitempty"`      // max 20 chars
	Stop        string `json:"stop,omitempty"`        // loss (<=, default), entry (>=)
	TimeInForce string `json:"timeInForce,omitempty"` // GTC (default), GTT, IOC, FOK
	CancelAfter int64  `json:"cancelAfter,omitempty"` // seconds; used with GTT
	PostOnly    bool   `json:"postOnly,omitempty"`
	Stp         string `json:"stp,omitempty"` // DC, CO, CN, CB
	IsIsolated  bool   `json:"isIsolated"`    // documented required; false = cross (default), true = isolated
	AutoBorrow  bool   `json:"autoBorrow,omitempty"`
	AutoRepay   bool   `json:"autoRepay,omitempty"`
}

// StopOrderRef identifies a newly placed stop order.
type StopOrderRef struct {
	OrderID   string `json:"orderId"`
	ClientOid string `json:"clientOid,omitempty"`
}

// StopOrder is the canonical Classic Margin stop order record, returned
// by GetStopOrderByID/GetStopOrderByClientOid and embedded in
// GetStopOrderList. Unlike the Add request, margin mode is not a
// separate field here — inspect TradeType (TRADE, MARGIN_TRADE,
// MARGIN_ISOLATED_TRADE) instead; KuCoin does not echo
// IsIsolated/AutoBorrow/AutoRepay back on the order record.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/orders/get-stop-order-details-by-orderid
type StopOrder struct {
	ID              string `json:"id"`
	Symbol          string `json:"symbol"`
	UserID          string `json:"userId"`
	Status          string `json:"status"` // NEW, TRIGGERED
	Type            string `json:"type"`   // limit, market
	Side            string `json:"side"`   // buy, sell
	Price           string `json:"price"`
	Size            string `json:"size"`
	Funds           string `json:"funds"`
	Stp             string `json:"stp,omitempty"`
	TimeInForce     string `json:"timeInForce"`
	CancelAfter     int64  `json:"cancelAfter"`
	PostOnly        bool   `json:"postOnly"`
	Hidden          bool   `json:"hidden"`
	Iceberg         bool   `json:"iceberg"`
	VisibleSize     string `json:"visibleSize"`
	Channel         string `json:"channel"`
	ClientOid       string `json:"clientOid,omitempty"`
	Remark          string `json:"remark,omitempty"`
	Tags            string `json:"tags,omitempty"`
	DomainID        string `json:"domainId"`
	TradeSource     string `json:"tradeSource"` // USER, MARGIN_SYSTEM
	TradeType       string `json:"tradeType"`   // TRADE, MARGIN_TRADE, MARGIN_ISOLATED_TRADE
	FeeCurrency     string `json:"feeCurrency"`
	TakerFeeRate    string `json:"takerFeeRate"`
	MakerFeeRate    string `json:"makerFeeRate"`
	CreatedAt       int64  `json:"createdAt"`
	OrderTime       int64  `json:"orderTime"` // nanosecond precision, unlike CreatedAt's milliseconds
	Stop            string `json:"stop"`      // loss, entry
	StopTriggerTime int64  `json:"stopTriggerTime,omitempty"`
	StopPrice       string `json:"stopPrice"`
}

// StopOrderPage is the page-paginated envelope returned by
// GetStopOrderList.
type StopOrderPage struct {
	CurrentPage int         `json:"currentPage"`
	PageSize    int         `json:"pageSize"`
	TotalNum    int         `json:"totalNum"`
	TotalPage   int         `json:"totalPage"`
	Items       []StopOrder `json:"items"`
}

// OCOOrderRequest submits a new Classic Margin OCO
// (One-Cancels-the-Other) order — a limit-order leg paired with a
// stop-limit leg, under /api/v3/hf/margin/oco-order. Unlike
// StopOrderRequest, ClientOid is required here (matching Classic
// Spot's OCO convention), and IsIsolated has no documented default —
// always set it explicitly.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/orders/add-oco-order
type OCOOrderRequest struct {
	Symbol     string `json:"symbol"`
	Side       string `json:"side"` // buy, sell
	ClientOid  string `json:"clientOid"`
	Price      string `json:"price"`      // limit-order leg price
	Size       string `json:"size"`       // shared by both legs
	StopPrice  string `json:"stopPrice"`  // trigger price for the stop-limit leg
	LimitPrice string `json:"limitPrice"` // limit price applied once stopPrice triggers
	IsIsolated bool   `json:"isIsolated"` // false = cross, true = isolated -- no documented default, set explicitly
	AutoBorrow bool   `json:"autoBorrow,omitempty"`
	AutoRepay  bool   `json:"autoRepay,omitempty"`
}

// OCOOrderRef identifies a newly placed OCO order.
type OCOOrderRef struct {
	OrderID string `json:"orderId"`
}

// OCOOrderInfo is the flat OCO-pair summary returned by
// GetOCOOrderByID/GetOCOOrderByClientOid and embedded in
// GetOCOOrderList. It carries no side/price — only OCOOrderDetails
// exposes the two constituent legs.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/orders/get-oco-order-info-by-orderid
type OCOOrderInfo struct {
	OrderID   string `json:"orderId"`
	Symbol    string `json:"symbol"`
	ClientOid string `json:"clientOid"`
	OrderTime int64  `json:"orderTime"`
	Status    string `json:"status"` // NEW, DONE, TRIGGERED, CANCELLED
}

// OCOOrderLeg is one constituent order (the limit leg or the
// stop-limit leg) of an OCO pair, as returned within
// OCOOrderDetails.Orders. Note the leg's own ID is keyed "id", not
// "orderId" -- the same KuCoin naming inconsistency confirmed on
// Classic Spot's OCO legs.
type OCOOrderLeg struct {
	ID        string `json:"id"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"` // buy, sell
	Price     string `json:"price"`
	StopPrice string `json:"stopPrice"`
	Size      string `json:"size"`
	Status    string `json:"status"`
}

// OCOOrderDetails is the full OCO-pair record — the same summary
// fields as OCOOrderInfo plus the two constituent leg orders. There is
// no "by ClientOid" variant of this endpoint, unlike GetOCOOrderByID.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/orders/get-oco-order-details
type OCOOrderDetails struct {
	OrderID   string        `json:"orderId"`
	Symbol    string        `json:"symbol"`
	ClientOid string        `json:"clientOid"`
	OrderTime int64         `json:"orderTime"`
	Status    string        `json:"status"`
	Orders    []OCOOrderLeg `json:"orders"`
}

// OCOOrderPage is the page-paginated envelope returned by
// GetOCOOrderList. Items are flat summaries (OCOOrderInfo) — call
// GetOCOOrderDetails per order ID for leg-level detail.
type OCOOrderPage struct {
	CurrentPage int            `json:"currentPage"`
	PageSize    int            `json:"pageSize"`
	TotalNum    int            `json:"totalNum"`
	TotalPage   int            `json:"totalPage"`
	Items       []OCOOrderInfo `json:"items"`
}
