// Package orders implements a seed set of KuCoin Classic Futures'
// private order-management endpoints (place/test/query — Phase 1 scope;
// cancel and the broader stop/OCO order family are not yet implemented).
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/orders/add-order
package orders

// PlaceOrderRequest submits a new Classic Futures order. clientOid is
// never generated automatically by this SDK.
//
// Exactly one of Size, Qty, or ValueQty should be set (KuCoin documents
// this as a "choose one" constraint in prose, not enforced by its
// schema) — validated locally by PlaceOrder/PlaceOrderTest.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/orders/add-order
type PlaceOrderRequest struct {
	ClientOid     string `json:"clientOid"`
	Side          string `json:"side"` // buy, sell
	Symbol        string `json:"symbol"`
	Leverage      string `json:"leverage,omitempty"`      // needed for isolated-margin orders
	Type          string `json:"type,omitempty"`          // limit (default), market
	Remark        string `json:"remark,omitempty"`        // max 100 UTF-8 chars
	Stop          string `json:"stop,omitempty"`          // down, up
	StopPriceType string `json:"stopPriceType,omitempty"` // TP, MP, IP -- required if Stop set
	StopPrice     string `json:"stopPrice,omitempty"`     // required if Stop set
	ReduceOnly    bool   `json:"reduceOnly,omitempty"`
	CloseOrder    bool   `json:"closeOrder,omitempty"` // side/size/leverage should be blank when true
	ForceHold     bool   `json:"forceHold,omitempty"`
	Stp           string `json:"stp,omitempty"`          // CN, CO, CB (DC not supported)
	MarginMode    string `json:"marginMode,omitempty"`   // ISOLATED (default), CROSS
	Price         string `json:"price,omitempty"`        // required for limit type
	Size          int64  `json:"size,omitempty"`         // lots; mutually exclusive with Qty/ValueQty
	Qty           string `json:"qty,omitempty"`          // base-currency amount; mutually exclusive with Size/ValueQty
	ValueQty      string `json:"valueQty,omitempty"`     // quote-currency amount; mutually exclusive with Size/Qty
	TimeInForce   string `json:"timeInForce,omitempty"`  // GTC (default), IOC, RPI
	PostOnly      bool   `json:"postOnly,omitempty"`     // invalid with timeInForce=IOC
	PositionSide  string `json:"positionSide,omitempty"` // BOTH, LONG, SHORT -- required in hedge mode
}

// OrderRef identifies a newly placed order.
type OrderRef struct {
	OrderID   string `json:"orderId"`
	ClientOid string `json:"clientOid"`
}

// Order is the canonical Classic Futures order record, returned by
// GetOrderByID and embedded in GetOrderList. Some enum-valued fields may
// arrive as an empty string (KuCoin's sentinel for "unset") rather than
// being omitted — decode leniently, don't assume a documented enum value.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/orders/get-order-by-orderld
type Order struct {
	ID            string `json:"id"`
	Symbol        string `json:"symbol"`
	Type          string `json:"type"` // market, limit
	Side          string `json:"side"` // buy, sell
	Price         string `json:"price"`
	Size          int64  `json:"size"`
	Value         string `json:"value"`
	DealValue     string `json:"dealValue"`
	DealSize      int64  `json:"dealSize"`
	Stp           string `json:"stp,omitempty"`
	Stop          string `json:"stop,omitempty"`
	StopPriceType string `json:"stopPriceType,omitempty"`
	StopTriggered bool   `json:"stopTriggered"`
	StopPrice     string `json:"stopPrice,omitempty"`
	// TimeInForce has been observed as GTC/GTT/IOC/FOK in responses,
	// though only GTC/IOC/RPI are documented as valid on requests.
	TimeInForce string `json:"timeInForce"`
	PostOnly    bool   `json:"postOnly"`
	Hidden      bool   `json:"hidden"`
	Iceberg     bool   `json:"iceberg"`
	// Leverage is a string here, unlike PlaceOrderRequest.Leverage.
	Leverage    string `json:"leverage"`
	ForceHold   bool   `json:"forceHold"`
	CloseOrder  bool   `json:"closeOrder"`
	VisibleSize int64  `json:"visibleSize"`
	ClientOid   string `json:"clientOid"`
	Remark      string `json:"remark,omitempty"`
	Tags        string `json:"tags,omitempty"`
	IsActive    bool   `json:"isActive"`
	CancelExist bool   `json:"cancelExist"`
	CreatedAt   int64  `json:"createdAt"`
	UpdatedAt   int64  `json:"updatedAt"`
	EndAt       int64  `json:"endAt,omitempty"`
	// OrderTime is a Unix nanosecond timestamp.
	OrderTime      int64  `json:"orderTime"`
	SettleCurrency string `json:"settleCurrency"`
	MarginMode     string `json:"marginMode"` // CROSS, ISOLATED
	AvgDealPrice   string `json:"avgDealPrice"`
	FilledSize     int64  `json:"filledSize"`
	FilledValue    string `json:"filledValue"`
	Status         string `json:"status"` // open, done
	ReduceOnly     bool   `json:"reduceOnly"`
	PositionSide   string `json:"positionSide,omitempty"` // BOTH, LONG, SHORT
}

// OrderListPage is the page-paginated envelope returned by GetOrderList.
type OrderListPage struct {
	CurrentPage int     `json:"currentPage"`
	PageSize    int     `json:"pageSize"`
	TotalNum    int     `json:"totalNum"`
	TotalPage   int     `json:"totalPage"`
	Items       []Order `json:"items"`
}
