package streaming

import (
	"time"

	"github.com/tigusigalpa/kucoin-go/types"
)

// AccountType selects which account the balance channel reports on.
type AccountType string

// Account types of the balance channel.
const (
	AccountTypeUnified  AccountType = "UNIFIED"
	AccountTypeFunding  AccountType = "FUNDING"
	AccountTypeIsolated AccountType = "ISOLATED"
)

// Valid reports whether KuCoin offers the account type.
func (a AccountType) Valid() bool {
	switch a {
	case AccountTypeUnified, AccountTypeFunding, AccountTypeIsolated:
		return true
	}
	return false
}

// MarginMode is the margin mode of an order, position or leverage setting.
type MarginMode string

// Margin modes.
const (
	MarginModeIsolated MarginMode = "ISOLATED"
	MarginModeCross    MarginMode = "CROSS"
)

// PositionSide is the position side of a futures order; it is only meaningful in
// hedge mode and BOTH otherwise.
type PositionSide string

// Position sides.
const (
	PositionSideBoth  PositionSide = "BOTH"
	PositionSideLong  PositionSide = "LONG"
	PositionSideShort PositionSide = "SHORT"
)

// OrderType is the type of an order.
type OrderType string

// Order types. An empty value occurs on executions that no order caused
// (liquidation, auto-deleveraging, settlement).
const (
	OrderTypeLimit  OrderType = "LIMIT"
	OrderTypeMarket OrderType = "MARKET"
)

// LiquidityRole says whether a fill provided or took liquidity.
type LiquidityRole string

// Liquidity roles.
const (
	LiquidityMaker LiquidityRole = "MAKER"
	LiquidityTaker LiquidityRole = "TAKER"
)

// OrderEventType is the event that produced an order update.
type OrderEventType string

// Order event types.
const (
	OrderEventOpen    OrderEventType = "OPEN"
	OrderEventUpdate  OrderEventType = "UPDATE"
	OrderEventFill    OrderEventType = "FILL"
	OrderEventCancel  OrderEventType = "CANCEL"
	OrderEventTrigger OrderEventType = "TRIGGER"
	OrderEventMatch   OrderEventType = "MATCH"
)

// OrderStatus is the state of an order. It is a JSON integer on the wire.
type OrderStatus int

// Order statuses. Spot and margin orders use 2 to 6; futures orders 0 to 6.
const (
	// OrderStatusNotTriggered is a conditional order that has not triggered yet
	// (futures only).
	OrderStatusNotTriggered OrderStatus = 0
	// OrderStatusTriggered is a conditional order that triggered. On a unified
	// account it is the final state of the conditional order; the order it
	// created arrives under its own order ID (TriggeredOrderID names the original).
	OrderStatusTriggered OrderStatus = 1
	// OrderStatusLive is an order resting on the book without fills.
	OrderStatusLive OrderStatus = 2
	// OrderStatusFilled is a fully filled order.
	OrderStatusFilled OrderStatus = 3
	// OrderStatusPartiallyFilled is a partly filled order that is still active.
	OrderStatusPartiallyFilled OrderStatus = 4
	// OrderStatusCanceled is an order canceled without any fill.
	OrderStatusCanceled OrderStatus = 5
	// OrderStatusPartiallyCanceled is a partly filled order whose remainder was
	// canceled.
	OrderStatusPartiallyCanceled OrderStatus = 6
)

// String returns the documented name of the status.
func (s OrderStatus) String() string {
	switch s {
	case OrderStatusNotTriggered:
		return "notTriggered"
	case OrderStatusTriggered:
		return "triggered"
	case OrderStatusLive:
		return "live"
	case OrderStatusFilled:
		return "filled"
	case OrderStatusPartiallyFilled:
		return "partialFilled"
	case OrderStatusCanceled:
		return "canceled"
	case OrderStatusPartiallyCanceled:
		return "partialCanceled"
	}
	return "unknown"
}

// UnmarshalJSON accepts a JSON number or a numeric string.
func (s *OrderStatus) UnmarshalJSON(data []byte) error {
	var n types.Int64
	if err := n.UnmarshalJSON(data); err != nil {
		return err
	}
	if string(data) != "null" {
		*s = OrderStatus(n)
	}
	return nil
}

// QuantityUnit is the unit of an order quantity.
type QuantityUnit string

// Quantity units.
const (
	QuantityUnitBase  QuantityUnit = "BASECCY"
	QuantityUnitQuote QuantityUnit = "QUOTECCY"
	QuantityUnitUnit  QuantityUnit = "UNIT"
)

// TriggerDirection is the direction a conditional order triggers in.
type TriggerDirection string

// Trigger directions.
const (
	TriggerDown TriggerDirection = "DOWN"
	TriggerUp   TriggerDirection = "UP"
)

// TriggerPriceType is the price a trigger price is compared with (futures):
// the last traded price, the index price or the mark price.
type TriggerPriceType string

// Trigger price types.
const (
	TriggerPriceTrade TriggerPriceType = "TP"
	TriggerPriceIndex TriggerPriceType = "IP"
	TriggerPriceMark  TriggerPriceType = "MP"
)

// TimeInForce is the time-in-force strategy of a limit order.
type TimeInForce string

// Time-in-force strategies.
const (
	TimeInForceGTC TimeInForce = "GTC"
	TimeInForceIOC TimeInForce = "IOC"
	TimeInForceFOK TimeInForce = "FOK"
	TimeInForceGTT TimeInForce = "GTT"
	TimeInForceRPI TimeInForce = "RPI"
)

// SelfTradePrevention is the self-trade-prevention mode of an order.
type SelfTradePrevention string

// Self-trade-prevention modes.
const (
	STPDecreaseAndCancel SelfTradePrevention = "DC"
	STPCancelOld         SelfTradePrevention = "CO"
	STPCancelNew         SelfTradePrevention = "CN"
	STPCancelBoth        SelfTradePrevention = "CB"
)

// OrderSource says who created the order (UTA futures only).
type OrderSource string

// OrderSourceUser marks an order the user placed.
const OrderSourceUser OrderSource = "USER"

// CancelReason is the reason an order was canceled, as listed in KuCoin's
// "Cancel Reason for Unified Trading" table. Values KuCoin adds later arrive
// unchanged.
type CancelReason string

// Cancel reasons documented for unified trading.
const (
	CancelReasonUser                         CancelReason = "USER"
	CancelReasonIOC                          CancelReason = "IOC"
	CancelReasonFOK                          CancelReason = "FOK"
	CancelReasonPostOnly                     CancelReason = "POST_ONLY"
	CancelReasonOrderBookFull                CancelReason = "ORDER_BOOK_FULL"
	CancelReasonSTP                          CancelReason = "STP"
	CancelReasonOrderExpired                 CancelReason = "ORDER_EXPIRED"
	CancelReasonOrderNotMatch                CancelReason = "ORDER_NOT_MATCH"
	CancelReasonNoSymbol                     CancelReason = "NO_SYMBOL"
	CancelReasonNoEnoughBalance              CancelReason = "NO_ENOUGH_BALANCE"
	CancelReasonNoMaker                      CancelReason = "NO_MAKER"
	CancelReasonZeroSize                     CancelReason = "ZERO_SIZE"
	CancelReasonTooManyOrderMatched          CancelReason = "TOO_MANY_ORDER_MATCHED"
	CancelReasonPriceLimited                 CancelReason = "PRICE_LIMITED"
	CancelReasonOpExpired                    CancelReason = "OP_EXPIRED"
	CancelReasonGTTParamError                CancelReason = "GTT_PARAM_ERROR"
	CancelReasonExceptionOrder               CancelReason = "EXCEPTION_ORDER"
	CancelReasonAlterOrderCancelOldOrder     CancelReason = "ALTER_ORDER_CANCEL_OLD_ORDER"
	CancelReasonAlterOrderCancelNewOrder     CancelReason = "ALTER_ORDER_CANCEL_NEW_ORDER"
	CancelReasonFOKRepeat                    CancelReason = "FOK_REPEAT"
	CancelReasonMarket                       CancelReason = "MARKET"
	CancelReasonMatchCountLimit              CancelReason = "MATCH_COUNT_LIMIT"
	CancelReasonPriceOutOfRange              CancelReason = "PRICE_OUT_OF_RANGE"
	CancelReasonCloseOrderWithoutPosition    CancelReason = "CLOSE_ORDER_WITHOUT_POSITION"
	CancelReasonNoEnoughMarginSize           CancelReason = "NO_ENOUGH_MARGIN_SIZE"
	CancelReasonADL                          CancelReason = "ADL"
	CancelReasonNoMarginSizeLargerThanPos    CancelReason = "NO_MARGIN_SIZE_LARGER_THAN_POSITION"
	CancelReasonWorseThanBankruptPrice       CancelReason = "WORSE_THAN_BANKRUPT_PRICE"
	CancelReasonCancelByFundingSettle        CancelReason = "CANCEL_BY_FUNDING_SETTLE"
	CancelReasonChangeRiskLimit              CancelReason = "CHANGE_RISK_LIMIT"
	CancelReasonWhiteUserBalanceInsufficient CancelReason = "WHITE_USER_BALANCE_INSUFFICIENT"
	CancelReasonCancelByContractSettle       CancelReason = "CANCEL_BY_CONTRACT_SETTLE"
	CancelReasonCancelBySystemErr            CancelReason = "CANCEL_BY_SYSTEM_ERR"
	CancelReasonCancelByDuplicatedID         CancelReason = "CANCEL_BY_DUPLICATED_ID"
	CancelReasonBorrowedFundsADL             CancelReason = "BORROWED_FUNDS_ADL"
	CancelReasonExceededAccountLiability     CancelReason = "EXCEEDED_ACCOUNT_LIABILITY_LIMIT"
	CancelReasonLiquidation                  CancelReason = "LIQUIDATION"
	CancelReasonPositionShortcut             CancelReason = "POSITION_SHORTCUT"
	CancelReasonSystemCancel                 CancelReason = "SYSTEM_CANCEL"
	CancelReasonIceFrogFrozen                CancelReason = "iceFrogFrozen"
)

// OrderUpdate is an order event of the order and orderAll channels: every change
// of one of your orders, for spot, margin and futures alike. EventType says what
// happened, Status the state the order is in afterwards.
//
// Quantities are cumulative where the name says so (FilledQuantity, Fee, Tax) and
// per event otherwise (LastFilledQuantity, LastFilledPrice). Empty strings in the
// payload (a market order has no Price, a conditional order no TradeID before it
// triggers) decode to empty Decimals; check IsEmpty before reading them.
//
// Docs: https://www.kucoin.com/docs-new/3470346w0
type OrderUpdate struct {
	// PushType is the T field of the push: "order.UNIFIED" for a single-symbol
	// subscription, "orderAll.UNIFIED" for the all-symbols one.
	PushType string `json:"-"`

	OrderID       types.ID    `json:"oi"`
	ClientOrderID string      `json:"ci"`
	Status        OrderStatus `json:"os"`
	// EventType is the event that produced this update; empty on some pushes.
	EventType OrderEventType `json:"eT"`
	// TradeType is the product: SPOT, FUTURES or MARGIN.
	TradeType    TradeType    `json:"tT"`
	Symbol       string       `json:"s"`
	Side         Side         `json:"S"`
	PositionSide PositionSide `json:"pS"`
	OrderType    OrderType    `json:"oT"`
	// LiquidityRole is the role of the last fill.
	LiquidityRole LiquidityRole `json:"lR"`
	// Source says who created the order (UTA futures only).
	Source OrderSource `json:"oS"`
	// Price is the order price; empty for market orders.
	Price      types.Decimal `json:"p"`
	MarginMode MarginMode    `json:"mM"`
	// TradeID is the latest trade ID, generated by the matching engine.
	TradeID types.ID `json:"ti"`
	// Quantity is the order quantity in QuantityUnit.
	Quantity     types.Decimal `json:"q"`
	QuantityUnit QuantityUnit  `json:"qU"`
	// FilledQuantity is the cumulative filled quantity.
	FilledQuantity types.Decimal `json:"fS"`
	// LastFilledQuantity and LastFilledPrice describe the fill of this update.
	LastFilledQuantity types.Decimal `json:"lS"`
	LastFilledPrice    types.Decimal `json:"ls"`
	// AveragePrice is the average filled price.
	AveragePrice types.Decimal `json:"aP"`
	// Fee is the accumulated settled fee in FeeCurrency; Tax the accumulated settled tax.
	Fee         types.Decimal `json:"f"`
	FeeCurrency string        `json:"fC"`
	Tax         types.Decimal `json:"t"`
	// CancelReason says why the order was canceled; CanceledQuantity how much.
	CancelReason     CancelReason  `json:"cR"`
	CanceledQuantity types.Decimal `json:"cS"`
	// RemainingQuantity is what is left to fill.
	RemainingQuantity types.Decimal `json:"rS"`

	// Conditional-order fields: the trigger direction, price and price type.
	TriggerDirection TriggerDirection `json:"tD"`
	TriggerPrice     types.Decimal    `json:"tP"`
	TriggerPriceType TriggerPriceType `json:"tPT"`
	// TriggeredOrderID is the original conditional order this order was created
	// from, when it was triggered.
	TriggeredOrderID types.ID `json:"toi"`

	// Take-profit and stop-loss fields of futures orders: trigger price, trigger
	// price type and execution price.
	TakeProfitPrice      types.Decimal    `json:"pP"`
	TakeProfitPriceType  TriggerPriceType `json:"pPT"`
	TakeProfitOrderPrice types.Decimal    `json:"pOP"`
	StopLossPrice        types.Decimal    `json:"lP"`
	StopLossPriceType    TriggerPriceType `json:"lPT"`
	StopLossOrderPrice   types.Decimal    `json:"lOP"`

	SelfTradePrevention SelfTradePrevention `json:"stp"`
	// ReduceOnly is set on reduce-only orders (futures).
	ReduceOnly  bool        `json:"rO"`
	TimeInForce TimeInForce `json:"tIF"`
	// PostOnly is the post-only flag (not sent for IOC orders).
	PostOnly bool `json:"pO"`
	// CreationTimestamp is when the system created the order, in nanoseconds,
	// recorded before the order is booked.
	CreationTimestamp types.Int64 `json:"O"`
	// UpdateTimestamp is the last update of the order, in nanoseconds, recorded
	// after the order is booked.
	UpdateTimestamp types.Int64 `json:"U"`
	// Broker is the broker name.
	Broker string `json:"bT"`
	// GatewayTimestamp is the gateway push time in nanoseconds.
	GatewayTimestamp types.Int64 `json:"-"`
}

// Time converts UpdateTimestamp (nanoseconds) to a time.Time.
func (o OrderUpdate) Time() time.Time { return nanos(int64(o.UpdateTimestamp)) }

// CreatedTime converts CreationTimestamp (nanoseconds) to a time.Time.
func (o OrderUpdate) CreatedTime() time.Time { return nanos(int64(o.CreationTimestamp)) }

// GatewayTime converts GatewayTimestamp (nanoseconds) to a time.Time.
func (o OrderUpdate) GatewayTime() time.Time { return nanos(int64(o.GatewayTimestamp)) }

// FillType is the kind of an execution.
type FillType string

// Fill types of the execution channel.
const (
	FillTypeNormal     FillType = "NORMAL"
	FillTypeLiquid     FillType = "LIQUID"
	FillTypeSettlement FillType = "SETTLEMENT"
	FillTypeADL        FillType = "ADL"
)

// Execution is a fill of the execution channel: you receive it when one of your
// orders trades, and for the fills liquidation, auto-deleveraging and settlement
// cause. For those the order ID is "0" and OrderType is empty.
//
// Docs: https://www.kucoin.com/docs-new/3470406w0
type Execution struct {
	OrderID       types.ID      `json:"oi"`
	ClientOrderID string        `json:"ci"`
	TradeID       types.ID      `json:"ti"`
	Symbol        string        `json:"s"`
	Side          Side          `json:"S"`
	OrderType     OrderType     `json:"oT"`
	Price         types.Decimal `json:"p"`
	// Size is the executed quantity.
	Size          types.Decimal `json:"q"`
	LiquidityRole LiquidityRole `json:"lR"`
	// Fee is the fee amount charged in FeeCurrency.
	Fee         types.Decimal `json:"f"`
	FeeCurrency string        `json:"fC"`
	FillType    FillType      `json:"fT"`
	// ClosedPnL is the closed profit and loss of a fill that closes a futures
	// position; empty for opening fills and non-contract trades.
	ClosedPnL types.Decimal `json:"fP"`
	// ExecutionTimestamp is the last update of the execution, generated by the
	// match engine, in nanoseconds.
	ExecutionTimestamp types.Int64 `json:"E"`
	// GatewayTimestamp is the gateway push time in nanoseconds.
	GatewayTimestamp types.Int64 `json:"-"`
}

// Time converts ExecutionTimestamp (nanoseconds) to a time.Time.
func (e Execution) Time() time.Time { return nanos(int64(e.ExecutionTimestamp)) }

// GatewayTime converts GatewayTimestamp (nanoseconds) to a time.Time.
func (e Execution) GatewayTime() time.Time { return nanos(int64(e.GatewayTimestamp)) }

// ExecutionLite is a fill of the execution.lite channel, the lower-latency
// variant of Execution without the fee, fee currency, fill type and closed PnL.
// TradeID is a JSON number on spot and a string on futures; ID accepts both.
//
// Docs: https://www.kucoin.com/docs-new/3470348w0
type ExecutionLite struct {
	OrderID       types.ID      `json:"oi"`
	ClientOrderID string        `json:"ci"`
	TradeID       types.ID      `json:"ti"`
	Symbol        string        `json:"s"`
	Side          Side          `json:"S"`
	OrderType     OrderType     `json:"oT"`
	Price         types.Decimal `json:"p"`
	// Size is the executed quantity.
	Size          types.Decimal `json:"q"`
	LiquidityRole LiquidityRole `json:"lR"`
	// ExecutionTimestamp is the last update of the execution, generated by the
	// match engine, in nanoseconds.
	ExecutionTimestamp types.Int64 `json:"E"`
	// GatewayTimestamp is the gateway push time in nanoseconds.
	GatewayTimestamp types.Int64 `json:"-"`
}

// Time converts ExecutionTimestamp (nanoseconds) to a time.Time.
func (e ExecutionLite) Time() time.Time { return nanos(int64(e.ExecutionTimestamp)) }

// GatewayTime converts GatewayTimestamp (nanoseconds) to a time.Time.
func (e ExecutionLite) GatewayTime() time.Time { return nanos(int64(e.GatewayTimestamp)) }

// CollateralStatus is the platform-level collateral status of a balance.
type CollateralStatus int

// Collateral statuses.
const (
	CollateralNoRestriction      CollateralStatus = 1
	CollateralNearingRestriction CollateralStatus = 2
	CollateralRestrictionTrigger CollateralStatus = 3
)

// UnmarshalJSON accepts a JSON number or a numeric string.
func (c *CollateralStatus) UnmarshalJSON(data []byte) error {
	var n types.Int64
	if err := n.UnmarshalJSON(data); err != nil {
		return err
	}
	if string(data) != "null" {
		*c = CollateralStatus(n)
	}
	return nil
}

// BalanceUpdate is a balance change of the balance channel, one currency per
// push. The unified account fills the margin fields; the funding account only
// currency, balance, available and hold.
//
// UpdateTimestamp is in nanoseconds on the unified account but in milliseconds on
// the funding account (and a numeric string there); Time detects the unit by
// magnitude. Equity is null for some pushes and then empty.
//
// Docs: https://www.kucoin.com/docs-new/3470347w0
type BalanceUpdate struct {
	// AccountType is taken from the push type ("balance.FUNDING").
	AccountType AccountType `json:"-"`
	Currency    string      `json:"c"`
	// Equity is the total equity of the currency.
	Equity types.Decimal `json:"e"`
	// Balance is the wallet balance, Available the available balance and Hold the
	// frozen part.
	Balance   types.Decimal `json:"b"`
	Available types.Decimal `json:"a"`
	Hold      types.Decimal `json:"h"`
	// Liability is the borrowed amount (UTA futures only).
	Liability types.Decimal `json:"l"`
	// Cross-margin and isolated-margin figures of the unified account.
	TotalCrossMargin         types.Decimal `json:"tCM"`
	CrossPosMargin           types.Decimal `json:"cPM"`
	CrossOrderMargin         types.Decimal `json:"cOM"`
	CrossUnrealisedPnL       types.Decimal `json:"cUP"`
	IsolatedPosMargin        types.Decimal `json:"iPM"`
	IsolatedOrderMargin      types.Decimal `json:"iOM"`
	IsolatedFundingFeeMargin types.Decimal `json:"iFF"`
	IsolatedUnrealisedPnL    types.Decimal `json:"iUP"`
	// CollateralStatus is the platform-level collateral status; zero when absent.
	CollateralStatus CollateralStatus `json:"cS"`
	// Sequence is the message sequence number when KuCoin sends one.
	Sequence types.Int64 `json:"E"`
	// UpdateTimestamp is the last update of the balance, generated by the risk
	// engine; nanoseconds on the unified account, milliseconds on the funding
	// account.
	UpdateTimestamp types.Int64 `json:"U"`
	// GatewayTimestamp is the gateway push time in nanoseconds.
	GatewayTimestamp types.Int64 `json:"-"`
}

// Time converts UpdateTimestamp to a time.Time, detecting its unit by magnitude.
func (b BalanceUpdate) Time() time.Time { return unixAuto(int64(b.UpdateTimestamp)) }

// GatewayTime converts GatewayTimestamp (nanoseconds) to a time.Time.
func (b BalanceUpdate) GatewayTime() time.Time { return nanos(int64(b.GatewayTimestamp)) }

// PositionUpdate is a futures position update of the position and positionAll
// channels, pushed when an order is placed, canceled or executed and when a
// position changes size for other reasons (auto-deleveraging).
//
// Docs: https://www.kucoin.com/docs-new/3470350w0
type PositionUpdate struct {
	// PushType is the T field of the push: "position.UNIFIED" for a single-symbol
	// subscription, "positionAll.UNIFIED" for the all-symbols one.
	PushType string `json:"-"`

	PositionID types.ID   `json:"pi"`
	Symbol     string     `json:"s"`
	MarginMode MarginMode `json:"mM"`
	// Size is the signed position size (negative for a short position).
	Size       types.Decimal `json:"q"`
	EntryPrice types.Decimal `json:"eP"`
	// PositionValue is the value at the mark price.
	PositionValue types.Decimal `json:"pV"`
	// PositionMargin is the margin the position occupies: for cross margin the
	// position value divided by leverage, for isolated margin the margin
	// allocated to it.
	PositionMargin   types.Decimal `json:"pM"`
	MarkPrice        types.Decimal `json:"mP"`
	LiquidationPrice types.Decimal `json:"lP"`
	BankruptcyPrice  types.Decimal `json:"bP"`
	Leverage         types.Decimal `json:"l"`
	UnrealisedPnL    types.Decimal `json:"uPL"`
	RealisedPnL      types.Decimal `json:"rPL"`
	InitialMargin    types.Decimal `json:"iM"`
	// MaintenanceMarginRate and MaintenanceMargin are the maintenance margin
	// requirement as a rate and as an amount.
	MaintenanceMarginRate types.Decimal `json:"mmr"`
	MaintenanceMargin     types.Decimal `json:"mtM"`
	// RiskRatio is the risk ratio of an isolated position (0.65 is 65%);
	// liquidation triggers at 1 or more.
	RiskRatio types.Decimal `json:"r"`
	// ADL is the auto-deleveraging ranking percentile: the higher, the likelier the
	// position is deleveraged.
	ADL types.Decimal `json:"adl"`
	// UpdateTimestamp and CreationTimestamp are generated by the match engine, in
	// nanoseconds.
	UpdateTimestamp   types.Int64 `json:"U"`
	CreationTimestamp types.Int64 `json:"O"`
	// GatewayTimestamp is the gateway push time in nanoseconds.
	GatewayTimestamp types.Int64 `json:"-"`
}

// Time converts UpdateTimestamp (nanoseconds) to a time.Time.
func (p PositionUpdate) Time() time.Time { return nanos(int64(p.UpdateTimestamp)) }

// CreatedTime converts CreationTimestamp (nanoseconds) to a time.Time.
func (p PositionUpdate) CreatedTime() time.Time { return nanos(int64(p.CreationTimestamp)) }

// GatewayTime converts GatewayTimestamp (nanoseconds) to a time.Time.
func (p PositionUpdate) GatewayTime() time.Time { return nanos(int64(p.GatewayTimestamp)) }

// RiskEvent is the risk event type of a liquidation warning.
type RiskEvent string

// Risk events, in order of severity.
const (
	// RiskEventMarginCall is a risk rate between 80% and 85%.
	RiskEventMarginCall RiskEvent = "MARGIN_CALL"
	// RiskEventReduceOnly is a risk rate between 85% and 90%: reduce-only mode.
	RiskEventReduceOnly RiskEvent = "REDUCE_ONLY"
	// RiskEventLiquidationWarning is a risk rate between 90% and 100%.
	RiskEventLiquidationWarning RiskEvent = "LIQUIDATION_WARNING"
	// RiskEventForceLiquidation is a risk rate of 100% or more: liquidation triggered.
	RiskEventForceLiquidation RiskEvent = "FORCE_LIQUIDATION"
)

// LiquidationWarning is a notice of the lw channel, pushed when a position
// approaches a dangerous risk level: only while the risk ratio is 80% or more and
// the event type changes, otherwise once a minute. KuCoin documents the push type
// both as "lw.UNIFIED" and as "risk.UNIFIED"; both are delivered.
//
// Docs: https://www.kucoin.com/docs-new/3470351w0
type LiquidationWarning struct {
	// PushType is the T field of the push.
	PushType string `json:"-"`

	Event RiskEvent `json:"eT"`
	// RiskRatio is the account risk rate (0.2345 is 23.45%).
	RiskRatio types.Decimal `json:"r"`
	// AdjustedEquity, InitialMargin, MaintenanceMargin, AvailableMargin, Equity and
	// Liability are account totals in USD.
	AdjustedEquity    types.Decimal `json:"a"`
	InitialMargin     types.Decimal `json:"iM"`
	MaintenanceMargin types.Decimal `json:"mM"`
	AvailableMargin   types.Decimal `json:"aM"`
	Equity            types.Decimal `json:"e"`
	Liability         types.Decimal `json:"l"`
	// UpdateTimestamp is generated by the risk engine, in nanoseconds.
	UpdateTimestamp types.Int64 `json:"U"`
	// GatewayTimestamp is the gateway push time in nanoseconds.
	GatewayTimestamp types.Int64 `json:"-"`
}

// Time converts UpdateTimestamp (nanoseconds) to a time.Time.
func (w LiquidationWarning) Time() time.Time { return nanos(int64(w.UpdateTimestamp)) }

// GatewayTime converts GatewayTimestamp (nanoseconds) to a time.Time.
func (w LiquidationWarning) GatewayTime() time.Time { return nanos(int64(w.GatewayTimestamp)) }

// LeverageUpdate is a leverage change of the leverage channel, pushed when you
// change a leverage multiplier. A futures update names the Symbol, a cross-margin
// update the Currency.
//
// KuCoin's published example for this channel is malformed JSON (commas are
// missing); the payload is the object {s | c, l, mM, tT}.
//
// Docs: https://www.kucoin.com/docs-new/3470352w0
type LeverageUpdate struct {
	// Symbol is set for futures, Currency for cross margin.
	Symbol   string `json:"s"`
	Currency string `json:"c"`
	// Leverage is the new leverage multiplier.
	Leverage   types.Decimal `json:"l"`
	MarginMode MarginMode    `json:"mM"`
	// TradeType is FUTURES or MARGIN.
	TradeType TradeType `json:"tT"`
	// GatewayTimestamp is the gateway push time in nanoseconds.
	GatewayTimestamp types.Int64 `json:"-"`
}

// Time converts GatewayTimestamp (nanoseconds) to a time.Time.
func (l LeverageUpdate) Time() time.Time { return nanos(int64(l.GatewayTimestamp)) }
