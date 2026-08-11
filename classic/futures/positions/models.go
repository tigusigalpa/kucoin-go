// Package positions implements a seed set of KuCoin Classic Futures'
// private position-related endpoints.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-position-details
package positions

import "encoding/json"

// Details is a single open position, as returned by GetPositionDetails
// (the /api/v2/position endpoint). Despite the endpoint's singular name
// and description, KuCoin returns this wrapped in a JSON array — see
// GetPositionDetails' docblock. Money/ratio fields are strings here.
//
// Do not confuse with ListItem: GetPositionList (/api/v1/positions) is a
// distinct, older schema with different field names and JSON number
// (not string) money fields — the two are not interchangeable.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-position-details
type Details struct {
	ID                   string `json:"id"`
	Symbol               string `json:"symbol"`
	AutoDeposit          bool   `json:"autoDeposit,omitempty"`
	MaintMarginReq       string `json:"maintMarginReq"`
	RiskLimit            int64  `json:"riskLimit,omitempty"`
	RealLeverage         string `json:"realLeverage,omitempty"`
	CrossMode            bool   `json:"crossMode"`
	DelevPercentage      string `json:"delevPercentage"`
	OpeningTimestamp     int64  `json:"openingTimestamp"`
	CurrentTimestamp     int64  `json:"currentTimestamp"`
	CurrentQty           int64  `json:"currentQty"`
	CurrentCost          string `json:"currentCost"`
	CurrentComm          string `json:"currentComm"`
	UnrealisedCost       string `json:"unrealisedCost"`
	RealisedGrossCost    string `json:"realisedGrossCost"`
	RealisedCost         string `json:"realisedCost"`
	IsOpen               bool   `json:"isOpen"`
	MarkPrice            string `json:"markPrice"`
	MarkValue            string `json:"markValue"`
	PosCost              string `json:"posCost"`
	PosCross             string `json:"posCross,omitempty"`
	PosCrossMargin       string `json:"posCrossMargin,omitempty"`
	PosProfit            string `json:"posProfit,omitempty"`
	PosInit              string `json:"posInit"`
	PosComm              string `json:"posComm,omitempty"`
	PosCommCommon        string `json:"posCommCommon,omitempty"`
	PosCommFunding       string `json:"posCommFunding,omitempty"`
	PosLoss              string `json:"posLoss,omitempty"`
	PosMargin            string `json:"posMargin"`
	PosFunding           string `json:"posFunding,omitempty"`
	PosMaint             string `json:"posMaint"`
	MaintMargin          string `json:"maintMargin,omitempty"`
	RealisedGrossPnl     string `json:"realisedGrossPnl"`
	RealisedPnl          string `json:"realisedPnl"`
	UnrealisedPnl        string `json:"unrealisedPnl"`
	UnrealisedPnlPcnt    string `json:"unrealisedPnlPcnt"`
	UnrealisedRoePcnt    string `json:"unrealisedRoePcnt"`
	AvgEntryPrice        string `json:"avgEntryPrice"`
	LiquidationPrice     string `json:"liquidationPrice"`
	BankruptPrice        string `json:"bankruptPrice"`
	SettleCurrency       string `json:"settleCurrency"`
	IsInverse            bool   `json:"isInverse"`
	MaintainMargin       string `json:"maintainMargin"`
	RiskLimitLevel       int    `json:"riskLimitLevel,omitempty"`
	MarginMode           string `json:"marginMode"`   // CROSS, ISOLATED
	PositionSide         string `json:"positionSide"` // BOTH, LONG, SHORT
	Leverage             string `json:"leverage"`
	CumulativeTradeFee   string `json:"cumulativeTradeFee"`
	CumulativeFundingFee string `json:"cumulativeFundingFee"`
	WithdrawPnl          string `json:"withdrawPnl,omitempty"`
	CumulativeTax        string `json:"cumulativeTax"`
	AggRate              string `json:"aggRate,omitempty"`
	Imr                  string `json:"imr,omitempty"` // cross margin only
	Amr                  string `json:"amr,omitempty"` // hedge-mode auto-margin-replenishment, cross margin only
}

// ListItem is a single open position, as returned by GetPositionList (the
// older /api/v1/positions endpoint). Money/ratio fields are JSON numbers
// on the wire, not strings — decoded as json.Number to preserve full
// precision (a plain string field would fail to decode an unquoted JSON
// number; float64 would risk precision loss).
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-position-list
type ListItem struct {
	ID                string      `json:"id"`
	Symbol            string      `json:"symbol"`
	AutoDeposit       bool        `json:"autoDeposit,omitempty"`
	CrossMode         bool        `json:"crossMode"`
	MaintMarginReq    json.Number `json:"maintMarginReq"`
	RiskLimit         json.Number `json:"riskLimit,omitempty"`
	RealLeverage      json.Number `json:"realLeverage,omitempty"`
	DelevPercentage   json.Number `json:"delevPercentage"`
	OpeningTimestamp  int64       `json:"openingTimestamp"`
	CurrentTimestamp  int64       `json:"currentTimestamp"`
	CurrentQty        int64       `json:"currentQty"`
	CurrentCost       json.Number `json:"currentCost"`
	CurrentComm       json.Number `json:"currentComm"`
	UnrealisedCost    json.Number `json:"unrealisedCost"`
	RealisedGrossCost json.Number `json:"realisedGrossCost"`
	RealisedCost      json.Number `json:"realisedCost"`
	IsOpen            bool        `json:"isOpen"`
	MarkPrice         json.Number `json:"markPrice"`
	MarkValue         json.Number `json:"markValue"`
	PosCost           json.Number `json:"posCost"`
	PosCross          json.Number `json:"posCross,omitempty"`
	PosCrossMargin    json.Number `json:"posCrossMargin,omitempty"`
	PosInit           json.Number `json:"posInit"`
	PosComm           json.Number `json:"posComm,omitempty"`
	PosCommCommon     json.Number `json:"posCommCommon,omitempty"`
	PosLoss           json.Number `json:"posLoss,omitempty"`
	PosMargin         json.Number `json:"posMargin"`
	PosFunding        json.Number `json:"posFunding,omitempty"`
	PosMaint          json.Number `json:"posMaint"`
	MaintMargin       json.Number `json:"maintMargin,omitempty"`
	RealisedGrossPnl  json.Number `json:"realisedGrossPnl"`
	RealisedPnl       json.Number `json:"realisedPnl"`
	UnrealisedPnl     json.Number `json:"unrealisedPnl"`
	UnrealisedPnlPcnt json.Number `json:"unrealisedPnlPcnt"`
	UnrealisedRoePcnt json.Number `json:"unrealisedRoePcnt"`
	AvgEntryPrice     json.Number `json:"avgEntryPrice"`
	LiquidationPrice  json.Number `json:"liquidationPrice"`
	BankruptPrice     json.Number `json:"bankruptPrice"`
	SettleCurrency    string      `json:"settleCurrency"`
	IsInverse         bool        `json:"isInverse"`
	MaintainMargin    json.Number `json:"maintainMargin"`
	MarginMode        string      `json:"marginMode"`
	PositionSide      string      `json:"positionSide"`
	Leverage          json.Number `json:"leverage"`
	DealComm          json.Number `json:"dealComm"`
	FundingFee        json.Number `json:"fundingFee"`
	Tax               json.Number `json:"tax"`
	WithdrawPnl       json.Number `json:"withdrawPnl,omitempty"`
}

// MarginMode describes a single symbol's current margin mode.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-margin-mode
type MarginMode struct {
	Symbol     string `json:"symbol"`
	MarginMode string `json:"marginMode"` // ISOLATED, CROSS
}

// PositionMode is the account-wide (not per-symbol) position mode.
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-position-mode
type PositionMode struct {
	// PositionMode is 0 (One-Way Mode) or 1 (Hedge Mode).
	PositionMode int `json:"positionMode"`
}
