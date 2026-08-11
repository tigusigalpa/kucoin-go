// Package positions implements KuCoin UTA's private position-management
// endpoints.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-position-list-uta
package positions

// Position is a single open futures position.
//
// KuCoin's own documentation is internally inconsistent here: the schema
// table names a required field "positionValue" ("current position value
// in settlement currency"), but the worked JSON example in the same doc
// shows "positionMargin" in that slot instead — the two never appear
// together. Both fields are decoded here; use Value() to read whichever
// KuCoin actually sent, rather than hardcoding one key name.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-position-list-uta
type Position struct {
	Symbol     string `json:"symbol"`
	ID         string `json:"id"`
	MarginMode string `json:"marginMode"` // CROSS, ISOLATED
	// Size is signed: positive = long, negative = short.
	Size              string `json:"size"`
	EntryPrice        string `json:"entryPrice"`
	PositionValue     string `json:"positionValue,omitempty"`
	PositionMargin    string `json:"positionMargin,omitempty"`
	MarkPrice         string `json:"markPrice"`
	Leverage          string `json:"leverage"`
	UnrealizedPnL     string `json:"unrealizedPnL"`
	RealizedPnL       string `json:"realizedPnL"`
	InitialMargin     string `json:"initialMargin"`
	Mmr               string `json:"mmr"` // maintenance margin rate
	MaintenanceMargin string `json:"maintenanceMargin"`
	// CreationTime is a Unix nanosecond timestamp.
	CreationTime     int64  `json:"creationTime"`
	LiquidationPrice string `json:"liquidationPrice"`
	RiskRatio        string `json:"riskRatio"`
	AdlPercentage    string `json:"adlPercentage"`
}

// Value returns PositionValue if KuCoin sent it, otherwise falls back to
// PositionMargin — see the docblock on Position for why both exist.
func (p Position) Value() string {
	if p.PositionValue != "" {
		return p.PositionValue
	}
	return p.PositionMargin
}

// ClosedPosition is a single historical (closed) position record.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-position-history-uta
type ClosedPosition struct {
	Symbol        string `json:"symbol"`
	CloseID       string `json:"closeId"`
	MarginMode    string `json:"marginMode"` // CROSS, ISOLATED
	Side          string `json:"side"`       // LONG, SHORT
	EntryPrice    string `json:"entryPrice"`
	ClosePrice    string `json:"closePrice"`
	MaxSize       string `json:"maxSize"`
	AvgClosePrice string `json:"avgClosePrice"`
	Leverage      string `json:"leverage"`
	RealizedPnL   string `json:"realizedPnL"`
	Fee           string `json:"fee"`
	Tax           string `json:"tax"`
	FundingFee    string `json:"fundingFee"`
	// ClosingTime/CreationTime are Unix nanosecond timestamps.
	ClosingTime  int64 `json:"closingTime"`
	CreationTime int64 `json:"creationTime"`
}

// PositionHistoryPage is the cursor-paginated envelope returned by
// GetPositionHistory.
type PositionHistoryPage struct {
	LastID int64            `json:"lastId,omitempty"`
	Items  []ClosedPosition `json:"items"`
}

// FundingFeeEntry is a single funding-fee settlement record.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-private-funding-fee-history
type FundingFeeEntry struct {
	Symbol         string `json:"symbol"`
	MarginMode     string `json:"marginMode"`
	FundingRate    string `json:"fundingRate"`
	MarkPrice      string `json:"markPrice"`
	Size           string `json:"size"`
	PositionValue  string `json:"positionValue"`
	FundingFee     string `json:"fundingFee"`
	SettleCurrency string `json:"settleCurrency"`
	// SettlementTime is a Unix MILLISECOND timestamp — unlike most other
	// timestamp fields in this package, which are nanoseconds.
	SettlementTime int64 `json:"settlementTime"`
}

// FundingFeeHistoryPage is the cursor-paginated envelope returned by
// GetFundingFeeHistory.
type FundingFeeHistoryPage struct {
	LastID int64             `json:"lastId,omitempty"`
	Items  []FundingFeeEntry `json:"items"`
}

// BatchModifyMarginModeRequest changes margin mode for one or more
// symbols at once.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/batch-modify-margin-mode
type BatchModifyMarginModeRequest struct {
	// Symbol accepts a comma-separated list, e.g. "ETHUSDTM, XRPUSDTM".
	Symbol     string `json:"symbol"`
	MarginMode string `json:"marginMode"` // CROSS, ISOLATED
}

// MarginModeResultItem is one symbol's outcome within a batch margin-mode
// change.
type MarginModeResultItem struct {
	Symbol     string `json:"symbol"`
	MarginMode string `json:"marginMode"`
	Code       string `json:"code"`
	Msg        string `json:"msg"`
}

// BatchModifyMarginModeResult is the response to BatchModifyMarginMode.
type BatchModifyMarginModeResult struct {
	Ts    int64                  `json:"ts"`
	Items []MarginModeResultItem `json:"items"`
}

// ModifyPositionMarginRequest deposits or withdraws margin from a
// position. tradeType's documented enum is FUTURES/MARGIN despite the
// prose ("Modify Position Margin") describing futures-only behavior —
// both values are accepted as-is here; see the note in
// docs/ENDPOINTS.md.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/modify-isolated-futures-margin
type ModifyPositionMarginRequest struct {
	Type         string `json:"type"` // DEPOSIT, WITHDRAW
	Amount       string `json:"amount"`
	Symbol       string `json:"symbol"`
	TradeType    string `json:"tradeType"`              // FUTURES, MARGIN
	PositionSide string `json:"positionSide,omitempty"` // LONG, SHORT, BOTH
}

// ModifyPositionMarginResult confirms a margin adjustment.
type ModifyPositionMarginResult struct {
	Ts int64 `json:"ts"`
}

// MarginModeEntry is a single symbol's current margin mode.
type MarginModeEntry struct {
	Symbol     string `json:"symbol"`
	MarginMode string `json:"marginMode"` // CROSS, ISOLATED
}

// MarginModeList is the envelope returned by GetMarginMode.
type MarginModeList struct {
	Ts    int64             `json:"ts"`
	Items []MarginModeEntry `json:"items"`
}
