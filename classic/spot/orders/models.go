// Package orders implements KuCoin Classic Spot's private order-management
// endpoints. Every endpoint here lives under the /api/v1/hf/... path
// family (KuCoin's "High-Frequency" trading engine) — not /api/v3/... or
// plain /api/v1/orders — confirmed across all 10 endpoints.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/orders/add-order
package orders

// PlaceOrderRequest submits a new Classic Spot order. clientOid is never
// generated automatically by this SDK.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/orders/add-order
type PlaceOrderRequest struct {
	Type               string `json:"type"` // limit, market
	Symbol             string `json:"symbol"`
	Side               string `json:"side"` // buy, sell
	ClientOid          string `json:"clientOid,omitempty"`
	Price              string `json:"price,omitempty"` // required for limit
	Stp                string `json:"stp,omitempty"`   // DC, CO, CN, CB
	Tags               string `json:"tags,omitempty"`
	Remark             string `json:"remark,omitempty"`
	TimeInForce        string `json:"timeInForce,omitempty"` // GTC (default), GTT, IOC, FOK
	CancelAfter        int64  `json:"cancelAfter,omitempty"` // seconds; used with GTT
	PostOnly           bool   `json:"postOnly,omitempty"`
	Size               string `json:"size,omitempty"`  // required for limit; market: size or funds
	Funds              string `json:"funds,omitempty"` // market orders only, alternative to size
	AllowMaxTimeWindow int64  `json:"allowMaxTimeWindow,omitempty"`
	ClientTimestamp    int64  `json:"clientTimestamp,omitempty"`
}

// OrderRef identifies a newly placed order.
type OrderRef struct {
	OrderID   string `json:"orderId"`
	ClientOid string `json:"clientOid"`
}

// BatchOrderResult is one order's outcome within BatchAddOrders. Failed
// entries have Success=false and FailMsg set, with OrderID/ClientOid
// empty.
type BatchOrderResult struct {
	Success   bool   `json:"success"`
	FailMsg   string `json:"failMsg,omitempty"`
	OrderID   string `json:"orderId,omitempty"`
	ClientOid string `json:"clientOid,omitempty"`
}

// CancelByOrderIDResult confirms a cancellation by order ID.
type CancelByOrderIDResult struct {
	OrderID string `json:"orderId"`
}

// CancelByClientOidResult confirms a cancellation by client order ID.
type CancelByClientOidResult struct {
	ClientOid string `json:"clientOid"`
}

// CancelAllResult reports which symbols were successfully cleared. The
// exact shape of failedSymbols' entries is not documented with named
// fields (KuCoin's docs only ever show an empty array in the example);
// decoded here as a raw map so no data is silently dropped if KuCoin
// returns something.
type CancelAllResult struct {
	SucceedSymbols []string                 `json:"succeedSymbols"`
	FailedSymbols  []map[string]interface{} `json:"failedSymbols"`
}

// FeeDetail is unused today but reserved — Classic Spot's Order object
// carries fee as a single string field, not a breakdown; kept out of
// Order to avoid a misleading empty type.

// Order is the canonical Classic Spot HF order record, returned by
// GetOrderByID and embedded in GetOpenOrders/GetClosedOrders.
//
// cancelReason is an integer code (0-18, 34-39, 99) with no published
// semantic labels as of this SDK's last documentation pass — treat it as
// an opaque code, not a friendly enum, until KuCoin publishes
// definitions.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/orders/get-order-by-orderld
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
	// CancelReason is only present once an order has been cancelled
	// (absent on GetOpenOrders results).
	CancelReason int `json:"cancelReason,omitempty"`
}

// ClosedOrderPage is the cursor-paginated envelope returned by
// GetClosedOrders.
type ClosedOrderPage struct {
	LastID int64   `json:"lastId"`
	Items  []Order `json:"items"`
}

// Fill is a single trade execution record (called "hf/fills" on the
// wire). Note Id/OrderId/CounterOrderId/TradeId are integers here, unlike
// the string order IDs used elsewhere in this SDK.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/orders/get-trade-history
type Fill struct {
	ID             int64  `json:"id"`
	OrderID        string `json:"orderId"`
	CounterOrderID string `json:"counterOrderId"`
	TradeID        int64  `json:"tradeId"`
	Symbol         string `json:"symbol"`
	Side           string `json:"side"`
	Liquidity      string `json:"liquidity"` // taker, maker
	Type           string `json:"type"`
	ForceTaker     bool   `json:"forceTaker"`
	Price          string `json:"price"`
	Size           string `json:"size"`
	Funds          string `json:"funds"`
	Fee            string `json:"fee"`
	FeeRate        string `json:"feeRate"`
	FeeCurrency    string `json:"feeCurrency"`
	Stop           string `json:"stop,omitempty"`
	TradeType      string `json:"tradeType"`
	TaxRate        string `json:"taxRate"`
	Tax            string `json:"tax"`
	CreatedAt      int64  `json:"createdAt"`
}

// FillPage is the cursor-paginated envelope returned by GetTradeHistory.
type FillPage struct {
	LastID int64  `json:"lastId"`
	Items  []Fill `json:"items"`
}

// StopOrderRequest submits a new Classic Spot stop order — a distinct,
// older order family from PlaceOrderRequest's HF orders (path
// /api/v1/stop-order vs /api/v1/hf/orders), sharing the same host and
// signing scheme. Unlike PlaceOrderRequest, ClientOid is optional here
// (KuCoin does not require it for stop orders).
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/orders/add-stop-order
type StopOrderRequest struct {
	Symbol      string `json:"symbol"`
	Type        string `json:"type"` // limit, market
	Side        string `json:"side"` // buy, sell
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
	Stp         string `json:"stp,omitempty"`       // DC, CO, CN, CB
	TradeType   string `json:"tradeType,omitempty"` // TRADE (default), MARGIN_TRADE, MARGIN_ISOLATED_TRADE
}

// StopOrderRef identifies a newly placed stop order.
type StopOrderRef struct {
	OrderID string `json:"orderId"`
}

// CancelStopOrderByClientOidResult confirms a cancellation by client
// order ID.
type CancelStopOrderByClientOidResult struct {
	CancelledOrderID string `json:"cancelledOrderId"`
	ClientOid        string `json:"clientOid"`
}

// CancelStopOrderResult confirms one or more cancellations. Used by both
// CancelStopOrderByID (a single-element slice) and CancelStopOrders
// (batch) — KuCoin returns an array in both cases.
type CancelStopOrderResult struct {
	CancelledOrderIDs []string `json:"cancelledOrderIds"`
}

// StopOrder is the canonical Classic Spot stop order record, returned by
// GetStopOrderByID/GetStopOrderByClientOid and embedded in
// GetStopOrderList.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/orders/get-stop-order-details-by-orderid
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
	TradeType       string `json:"tradeType"`
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

// OCOOrderRequest submits a new Classic Spot OCO (One-Cancels-the-Other)
// order — a limit-order leg (Price/Size) paired with a stop-limit leg
// (StopPrice triggers, then LimitPrice is the resulting limit order).
// Unlike StopOrderRequest, ClientOid is required here.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/orders/add-oco-order
type OCOOrderRequest struct {
	Symbol     string `json:"symbol"`
	Side       string `json:"side"` // buy, sell
	ClientOid  string `json:"clientOid"`
	Price      string `json:"price"`               // limit-order leg price
	Size       string `json:"size"`                // shared by both legs
	StopPrice  string `json:"stopPrice"`           // trigger price for the stop-limit leg
	LimitPrice string `json:"limitPrice"`          // limit price applied once stopPrice triggers
	Remark     string `json:"remark,omitempty"`    // max 20 ASCII chars
	TradeType  string `json:"tradeType,omitempty"` // only TRADE is currently supported
}

// OCOOrderRef identifies a newly placed OCO order.
type OCOOrderRef struct {
	OrderID string `json:"orderId"`
}

// CancelOCOOrderResult confirms one or more cancellations. Returned by
// CancelOCOOrderByID, CancelOCOOrderByClientOid, and CancelOCOOrders
// (batch) alike — CancelledOrderIDs holds both leg IDs of each
// cancelled OCO pair.
type CancelOCOOrderResult struct {
	CancelledOrderIDs []string `json:"cancelledOrderIds"`
}

// OCOOrderInfo is the flat OCO-pair summary returned by
// GetOCOOrderByID/GetOCOOrderByClientOid and embedded in
// GetOCOOrderList. It carries no side/price — only OCOOrderDetails
// exposes the two constituent legs.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/orders/get-oco-order-info-by-orderid
type OCOOrderInfo struct {
	OrderID   string `json:"orderId"`
	Symbol    string `json:"symbol"`
	ClientOid string `json:"clientOid"`
	OrderTime int64  `json:"orderTime"`
	Status    string `json:"status"` // NEW, DONE, TRIGGERED, CANCELLED
}

// OCOOrderLeg is one constituent order (the limit leg or the stop-limit
// leg) of an OCO pair, as returned within OCOOrderDetails.Orders. Note
// the leg's own ID is keyed "id", not "orderId" — KuCoin's own naming
// inconsistency between the leg shape and the pair-summary shape,
// preserved as-is rather than silently unified.
type OCOOrderLeg struct {
	ID        string `json:"id"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"` // buy, sell
	Price     string `json:"price"`
	StopPrice string `json:"stopPrice"`
	Size      string `json:"size"`
	Status    string `json:"status"`
}

// OCOOrderDetails is the full OCO-pair record — the same summary fields
// as OCOOrderInfo plus the two constituent leg orders.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/orders/get-oco-order-details
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

// SetDCPRequest configures Disconnect Cancel Protocol — a dead-man's
// switch that auto-cancels every open HF order (Timeout seconds after
// the last SetDCP call, unless refreshed again) if the caller
// disconnects without deactivating it. There is no separate deactivate
// endpoint: call SetDCP with Timeout -1 to disable it.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/orders/set-dcp
type SetDCPRequest struct {
	Timeout int    `json:"timeout"`           // seconds, 5-86400, or -1 to disable
	Symbols string `json:"symbols,omitempty"` // comma-separated, up to 50 pairs; empty = all pairs
}

// DCPTrigger reports when SetDCP's auto-cancel will fire, returned by
// SetDCP.
type DCPTrigger struct {
	CurrentTime int64 `json:"currentTime"` // Unix seconds
	TriggerTime int64 `json:"triggerTime"` // Unix seconds
}

// DCPConfig is the current Disconnect Cancel Protocol configuration,
// returned by GetDCP.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/orders/get-dcp
type DCPConfig struct {
	Timeout     int    `json:"timeout"`
	Symbols     string `json:"symbols"`
	CurrentTime int64  `json:"currentTime"`
	TriggerTime int64  `json:"triggerTime"`
}
