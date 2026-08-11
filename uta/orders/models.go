// Package orders implements KuCoin UTA's private order-management
// endpoints.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/place-order
package orders

// PlaceOrderRequest submits a new UTA order. clientOid is never generated
// automatically by this SDK — pass your own for retry-safe workflows, or
// leave it empty if you don't need one.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/place-order
type PlaceOrderRequest struct {
	TradeType          string `json:"tradeType"`
	ClientOid          string `json:"clientOid,omitempty"`
	Symbol             string `json:"symbol"`
	TriggerDirection   string `json:"triggerDirection,omitempty"` // DOWN, UP
	TriggerPriceType   string `json:"triggerPriceType,omitempty"` // TP, IP, MP
	TriggerPrice       string `json:"triggerPrice,omitempty"`
	Side               string `json:"side"`      // BUY, SELL
	OrderType          string `json:"orderType"` // LIMIT, MARKET
	Size               string `json:"size"`
	SizeUnit           string `json:"sizeUnit"` // BASECCY, QUOTECCY, UNIT
	Price              string `json:"price,omitempty"`
	TimeInForce        string `json:"timeInForce,omitempty"` // GTC, IOC, GTT, FOK, RPI (default GTC)
	PostOnly           bool   `json:"postOnly,omitempty"`
	ReduceOnly         bool   `json:"reduceOnly,omitempty"`         // futures only
	Stp                string `json:"stp,omitempty"`                // DC, CO, CN, CB
	Tags               string `json:"tags,omitempty"`               // max 20 chars
	CancelAfter        int64  `json:"cancelAfter,omitempty"`        // seconds; only with timeInForce=GTT
	PositionSide       string `json:"positionSide,omitempty"`       // BOTH, LONG, SHORT
	MarginMode         string `json:"marginMode,omitempty"`         // ISOLATED, CROSS
	TpTriggerPriceType string `json:"tpTriggerPriceType,omitempty"` // futures only
	TpTriggerPrice     string `json:"tpTriggerPrice,omitempty"`     // futures only
	SlTriggerPriceType string `json:"slTriggerPriceType,omitempty"` // futures only
	SlTriggerPrice     string `json:"slTriggerPrice,omitempty"`     // futures only
	CloseOrder         bool   `json:"closeOrder,omitempty"`
}

// OrderRef identifies a newly placed order.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/place-order
type OrderRef struct {
	TradeType string `json:"tradeType"`
	OrderID   string `json:"orderId"`
	ClientOid string `json:"clientOid"`
	Ts        int64  `json:"ts,omitempty"`
}

// CancelOrderRequest cancels a single order. Either OrderID or ClientOid
// must be set; if both are set, OrderID takes priority.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/cancel-order
type CancelOrderRequest struct {
	TradeType string `json:"tradeType"`
	Symbol    string `json:"symbol"`
	OrderID   string `json:"orderId,omitempty"`
	ClientOid string `json:"clientOid,omitempty"`
}

// CancelOrderResult confirms a cancellation.
type CancelOrderResult struct {
	TradeType string `json:"tradeType"`
	OrderID   string `json:"orderId,omitempty"`
	ClientOid string `json:"clientOid,omitempty"`
	Ts        int64  `json:"ts,omitempty"`
}

// BatchCancelItemRequest identifies one order within a batch cancel.
// Either OrderID or ClientOid must be set; if both are set, OrderID takes
// priority.
type BatchCancelItemRequest struct {
	Symbol    string `json:"symbol"`
	OrderID   string `json:"orderId,omitempty"`
	ClientOid string `json:"clientOid,omitempty"`
}

// BatchCancelByIDRequest cancels up to 20 specific orders in one call.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/batch-cancel-order-by-id
type BatchCancelByIDRequest struct {
	TradeType       string                   `json:"tradeType"`
	CancelOrderList []BatchCancelItemRequest `json:"cancelOrderList"`
}

// BatchCancelBySymbolRequest cancels every order matching a symbol (and
// optionally margin mode / order filter).
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/batch-cancel-order-by-symbol
type BatchCancelBySymbolRequest struct {
	Symbol      string `json:"symbol"`
	TradeType   string `json:"tradeType"`
	MarginMode  string `json:"marginMode,omitempty"` // CROSS, ISOLATED (default CROSS)
	OrderFilter string `json:"orderFilter"`          // NORMAL, ADVANCED
}

// BatchCancelResultItem is one order's outcome within a batch cancel
// result.
type BatchCancelResultItem struct {
	Code      string `json:"code"`
	Msg       string `json:"msg"`
	OrderID   string `json:"orderId"`
	Ts        int64  `json:"ts"`
	ClientOid string `json:"clientOid,omitempty"`
}

// BatchCancelResult is the UTA-mode response shape shared by
// BatchCancelOrderByID and BatchCancelOrderBySymbol.
type BatchCancelResult struct {
	TradeType string                  `json:"tradeType"`
	Items     []BatchCancelResultItem `json:"items"`
}

// Order is the canonical order record returned by GetOrderDetails and
// embedded in the paginated lists from GetOpenOrderList/GetOrderHistory.
//
// cancelReason is typed as a string here (this SDK targets UTA-mode
// endpoints, where it's documented as a string; the deprecated Classic
// account mode returns an integer code instead).
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-order-details
type Order struct {
	TradeType string `json:"tradeType"`
	OrderID   string `json:"orderId"`
	ClientOid string `json:"clientOid"`
	// Status: 0 notTriggered (futures conditional only), 1 triggered,
	// 2 live, 3 filled, 4 partial filled, 5 canceled, 6 partial canceled.
	Status             int    `json:"status"`
	FilledSize         string `json:"filledSize,omitempty"`
	AvgPrice           string `json:"avgPrice,omitempty"`
	Fee                string `json:"fee,omitempty"`
	FeeCurrency        string `json:"feeCurrency,omitempty"`
	Tax                string `json:"tax,omitempty"`
	TradeID            string `json:"tradeId,omitempty"`
	Symbol             string `json:"symbol,omitempty"`
	Side               string `json:"side,omitempty"`
	PositionSide       string `json:"positionSide,omitempty"`
	OrderType          string `json:"orderType,omitempty"`
	Size               string `json:"size,omitempty"`
	SizeUnit           string `json:"sizeUnit,omitempty"`
	Price              string `json:"price,omitempty"`
	ReduceOnly         bool   `json:"reduceOnly,omitempty"`
	MarginMode         string `json:"marginMode,omitempty"`
	Stp                string `json:"stp,omitempty"`
	TimeInForce        string `json:"timeInForce,omitempty"`
	CancelReason       string `json:"cancelReason,omitempty"`
	CancelSize         string `json:"cancelSize,omitempty"`
	CancelAfter        int64  `json:"cancelAfter,omitempty"`
	TriggerDirection   string `json:"triggerDirection,omitempty"`
	TriggerPrice       string `json:"triggerPrice,omitempty"`
	TriggerPriceType   string `json:"triggerPriceType,omitempty"`
	TpTriggerPrice     string `json:"tpTriggerPrice,omitempty"`
	TpTriggerPriceType string `json:"tpTriggerPriceType,omitempty"`
	TpOrderPrice       string `json:"tpOrderPrice,omitempty"`
	SlTriggerPrice     string `json:"slTriggerPrice,omitempty"`
	SlTriggerPriceType string `json:"slTriggerPriceType,omitempty"`
	SlOrderPrice       string `json:"slOrderPrice,omitempty"`
	PostOnly           bool   `json:"postOnly,omitempty"`
	Tags               string `json:"tags,omitempty"`
	TriggerOrderID     string `json:"triggerOrderId,omitempty"`
	// OrderTime/UpdatedTime are Unix nanosecond timestamps.
	OrderTime   int64 `json:"orderTime,omitempty"`
	UpdatedTime int64 `json:"updatedTime,omitempty"`
}

// OpenOrderList is the envelope returned by GetOpenOrderList.
type OpenOrderList struct {
	PageNumber int     `json:"pageNumber"`
	TradeType  string  `json:"tradeType"`
	Items      []Order `json:"items"`
}

// OrderHistoryPage is the cursor-paginated envelope returned by
// GetOrderHistory.
type OrderHistoryPage struct {
	LastID    int64   `json:"lastId,omitempty"`
	TradeType string  `json:"tradeType"`
	Items     []Order `json:"items"`
}

// Trade is a single fill/execution record.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-trade-history
type Trade struct {
	Symbol       string `json:"symbol"`
	OrderID      string `json:"orderId"`
	OrderType    string `json:"orderType"` // LIMIT, MARKET
	Side         string `json:"side"`      // BUY, SELL
	PositionSide string `json:"positionSide,omitempty"`
	// FillType: NORMAL, LIQUID, ADL, SETTLEMENT, RECONCILIATION (SPOT is
	// always NORMAL).
	FillType string `json:"fillType"`
	TradeID  string `json:"tradeId"`
	Size     string `json:"size"`
	Value    string `json:"value"`
	Price    string `json:"price"`
	// ExecutionTime is a Unix nanosecond timestamp.
	ExecutionTime int64  `json:"executionTime"`
	Fee           string `json:"fee"`
	FeeCurrency   string `json:"feeCurrency"`
	LiquidityRole string `json:"liquidityRole"` // MAKER, TAKER
	MarginMode    string `json:"marginMode,omitempty"`
	Tax           string `json:"tax"`
}

// TradeHistoryPage is the cursor-paginated envelope returned by
// GetTradeHistory.
type TradeHistoryPage struct {
	LastID    int64   `json:"lastId,omitempty"`
	TradeType string  `json:"tradeType"`
	Items     []Trade `json:"items"`
}
