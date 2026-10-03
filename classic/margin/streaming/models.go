package streaming

import (
	"time"

	spotstreaming "github.com/tigusigalpa/kucoin-go/classic/spot/streaming"
	"github.com/tigusigalpa/kucoin-go/types"
)

// Numeric conventions are those of the Spot package: prices, rates and amounts
// that can carry decimals are types.Decimal (exact text, never float64), counts
// and timestamps are types.Int64, and both accept the JSON string and number
// spellings KuCoin uses interchangeably (the index price is a bare number with
// 15 decimals, the position totals mix numbers and strings). Timestamp units are
// stated on every field.

func millis(ms int64) time.Time { return time.UnixMilli(ms) }

// IndexPrice is an update of the index price used for margin trading, from
// /indicator/index:{symbol},{symbol}. KuCoin pushes it once a second per symbol.
//
// Docs: https://www.kucoin.com/docs-new/3470076w0
type IndexPrice struct {
	// Symbol is the payload's symbol, for example "USDT-BTC".
	Symbol string `json:"symbol"`
	// Granularity is the interval the price is computed at, in milliseconds.
	Granularity types.Int64 `json:"granularity"`
	// Timestamp is in milliseconds.
	Timestamp types.Int64 `json:"timestamp"`
	// Value is the price, a JSON number with up to 15 decimals whose text is kept
	// exactly (0.000011830000000).
	Value types.Decimal `json:"value"`
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (p IndexPrice) Time() time.Time { return millis(int64(p.Timestamp)) }

// MarkPrice is an update of the mark price used for margin trading, from
// /indicator/markPrice:{symbol},{symbol}. KuCoin pushes it once a second per
// symbol.
//
// Docs: https://www.kucoin.com/docs-new/3470077w0
type MarkPrice struct {
	// Symbol is the payload's symbol, for example "USDT-BTC".
	Symbol string `json:"symbol"`
	// Granularity is the interval the price is computed at, in milliseconds.
	Granularity types.Int64 `json:"granularity"`
	// Timestamp is in milliseconds.
	Timestamp types.Int64 `json:"timestamp"`
	// Value is the price, a JSON number with up to 15 decimals whose text is kept
	// exactly (0.000011820000000).
	Value types.Decimal `json:"value"`
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (p MarkPrice) Time() time.Time { return millis(int64(p.Timestamp)) }

// Subjects of the cross-margin position channel.
const (
	// SubjectDebtRatio marks a CrossMarginPositionEvent that carries the current
	// debt ratio with the asset and debt lists. KuCoin pushes it every three
	// seconds while the account has a liability.
	SubjectDebtRatio = "debt.ratio"
	// SubjectPositionStatus marks a CrossMarginPositionEvent that reports a change
	// of the position status in Type.
	SubjectPositionStatus = "position.status"
)

// Position status types of CrossMarginPositionEvent.Type. The documentation lists
// the values without explaining them.
const (
	PositionStatusFrozenFL      = "FROZEN_FL"
	PositionStatusUnfrozenFL    = "UNFROZEN_FL"
	PositionStatusFrozenRenew   = "FROZEN_RENEW"
	PositionStatusUnfrozenRenew = "UNFROZEN_RENEW"
	PositionStatusLiability     = "LIABILITY"
	PositionStatusUnliability   = "UNLIABILITY"
)

// CrossMarginAsset is the balance of one currency in a cross-margin account.
type CrossMarginAsset struct {
	Total     types.Decimal `json:"total"`
	Available types.Decimal `json:"available"`
	Hold      types.Decimal `json:"hold"`
}

// CrossMarginPositionEvent is an update of the cross-margin position from
// /margin/position. The channel carries two kinds of update:
//
//   - SubjectDebtRatio: DebtRatio, TotalAsset, MarginCoefficientTotalAsset,
//     TotalDebt, AssetList, DebtList and Timestamp;
//   - SubjectPositionStatus: Type (one of the PositionStatus values) and Timestamp.
//
// Fields that a kind does not use stay empty. TotalAsset and TotalDebt are valued
// in BTC with interest included; AssetList and DebtList are keyed by currency
// ("BTC", "USDT", ...).
//
// KuCoin's own example of the status update is labelled with the subject
// "debt.ratio" although it is the status payload and the documentation lists
// "position.status" as the subject of such updates. IsPositionStatus and
// IsDebtRatio therefore decide by content: an update that carries Type is a
// status update whatever its subject says.
//
// It needs a session from DialPrivate.
//
// Docs: https://www.kucoin.com/docs-new/3470078w0
type CrossMarginPositionEvent struct {
	spotstreaming.PrivateEnvelope

	DebtRatio types.Decimal `json:"debtRatio"`
	// TotalAsset and TotalDebt are in BTC, interest included.
	TotalAsset types.Decimal `json:"totalAsset"`
	// MarginCoefficientTotalAsset is documented as a bare string without a
	// description.
	MarginCoefficientTotalAsset types.Decimal               `json:"marginCoefficientTotalAsset"`
	TotalDebt                   types.Decimal               `json:"totalDebt"`
	AssetList                   map[string]CrossMarginAsset `json:"assetList"`
	DebtList                    map[string]types.Decimal    `json:"debtList"`

	// Type is the position status of a status update.
	Type string `json:"type"`
	// Timestamp is in milliseconds.
	Timestamp types.Int64 `json:"timestamp"`
}

// IsPositionStatus reports whether the event is a position status update.
func (e CrossMarginPositionEvent) IsPositionStatus() bool {
	return e.Subject == SubjectPositionStatus || e.Type != ""
}

// IsDebtRatio reports whether the event carries the debt ratio and the asset and
// debt lists.
func (e CrossMarginPositionEvent) IsDebtRatio() bool {
	return e.Subject == SubjectDebtRatio && e.Type == ""
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (e CrossMarginPositionEvent) Time() time.Time { return millis(int64(e.Timestamp)) }

// SubjectIsolatedPositionChange is the subject of the isolated-margin position
// channel.
const SubjectIsolatedPositionChange = "positionChange"

// Isolated-margin position statuses (IsolatedMarginPositionEvent.Status).
const (
	IsolatedStatusDebt          = "DEBT"
	IsolatedStatusClear         = "CLEAR"
	IsolatedStatusInBorrow      = "IN_BORROW"
	IsolatedStatusInRepay       = "IN_REPAY"
	IsolatedStatusInLiquidation = "IN_LIQUIDATION"
	IsolatedStatusInAutoRenew   = "IN_AUTO_RENEW"
)

// Status types of an isolated-margin position change
// (IsolatedMarginPositionEvent.StatusBizType).
const (
	StatusBizForceLiquidation             = "FORCE_LIQUIDATION"
	StatusBizUserBorrow                   = "USER_BORROW"
	StatusBizTradeAutoBorrow              = "TRADE_AUTO_BORROW"
	StatusBizUserRepay                    = "USER_REPAY"
	StatusBizAutoRepay                    = "AUTO_REPAY"
	StatusBizDefaultDebt                  = "DEFAULT_DEBT"
	StatusBizDefaultClear                 = "DEFAULT_CLEAR"
	StatusBizOneClickLiquidation          = "ONE_CLICK_LIQUIDATION"
	StatusBizB2CInterestSettleLiquidation = "B2C_INTEREST_SETTLE_LIQUIDATION"
	StatusBizAirDropLiquidation           = "AIR_DROP_LIQUIDATION"
)

// IsolatedMarginAsset is the balance and the liability of one currency in an
// isolated-margin account.
type IsolatedMarginAsset struct {
	Total              types.Decimal `json:"total"`
	Hold               types.Decimal `json:"hold"`
	LiabilityPrincipal types.Decimal `json:"liabilityPrincipal"`
	LiabilityInterest  types.Decimal `json:"liabilityInterest"`
}

// IsolatedMarginPositionEvent is an update of an isolated-margin position from
// /margin/isolatedPosition:{symbol}, pushed when the position status changes and
// periodically while it carries a liability. ChangeAssets is keyed by currency
// ("BTC", "USDT").
//
// It needs a session from DialPrivate.
//
// Docs: https://www.kucoin.com/docs-new/3470079w0
type IsolatedMarginPositionEvent struct {
	spotstreaming.PrivateEnvelope

	// Tag is the isolated-margin symbol, for example "BTC-USDT".
	Tag string `json:"tag"`
	// Status is one of the IsolatedStatus values and StatusBizType one of the
	// StatusBiz values.
	Status        string `json:"status"`
	StatusBizType string `json:"statusBizType"`
	// AccumulatedPrincipal is the accumulated principal of the position.
	AccumulatedPrincipal types.Decimal                  `json:"accumulatedPrincipal"`
	ChangeAssets         map[string]IsolatedMarginAsset `json:"changeAssets"`
	// Timestamp is in milliseconds.
	Timestamp types.Int64 `json:"timestamp"`
}

// Time converts Timestamp (milliseconds) to a time.Time.
func (e IsolatedMarginPositionEvent) Time() time.Time { return millis(int64(e.Timestamp)) }
