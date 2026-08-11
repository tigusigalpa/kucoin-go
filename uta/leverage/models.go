// Package leverage implements KuCoin UTA's private leverage-management
// endpoints. Despite the name, KuCoin's own documentation sidebar files
// these under "Account", not a separate "Leverage" section — this SDK
// splits them into their own package because they have a distinct
// permission model (all three require the "Unified" permission, unlike
// most read endpoints elsewhere in this SDK which only need "General").
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/modify-leverage-uta
package leverage

// ModifyFuturesLeverageRequest sets futures leverage for a symbol.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/modify-leverage-uta
type ModifyFuturesLeverageRequest struct {
	Symbol   string `json:"symbol"`
	Leverage string `json:"leverage"`
}

// FuturesLeverageResult confirms a futures leverage change. KuCoin may
// echo Leverage with different formatting than the request (e.g. "80.00"
// for a request of "80").
type FuturesLeverageResult struct {
	Code     string `json:"code,omitempty"`
	Leverage string `json:"leverage,omitempty"`
}

// ModifyCrossMarginLeverageRequest sets cross-margin leverage. Currency
// is optional — omit to apply account-wide.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/modify-cross-margin-leverage-uta
type ModifyCrossMarginLeverageRequest struct {
	Currency string `json:"currency,omitempty"`
	Leverage string `json:"leverage"`
}

// CrossMarginLeverageResult confirms a cross-margin leverage change.
type CrossMarginLeverageResult struct {
	Currency string `json:"currency,omitempty"`
	Leverage string `json:"leverage,omitempty"`
}

// LeverageEntry is a single currency's or symbol's current leverage.
//
// MarginMode's documented enum is literally "ISOLATE, CROSS" (missing
// the "D") — likely a KuCoin doc typo, since every other endpoint in this
// SDK uses "ISOLATED". Both spellings are treated as-is (not coerced) so
// callers can see exactly what KuCoin sent.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-leverage
type LeverageEntry struct {
	Currency   string `json:"currency,omitempty"`
	Symbol     string `json:"symbol,omitempty"`
	Leverage   string `json:"leverage"`
	MarginMode string `json:"marginMode"`
}
