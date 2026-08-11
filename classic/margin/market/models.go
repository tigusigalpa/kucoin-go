// Package market implements a seed set of KuCoin Classic Margin's public
// market-data endpoints: symbol specs (cross + isolated), mark price,
// margin config, and risk limits (cross + isolated). Collateral-ratio
// and market-available-inventory are not yet implemented — this SDK's
// documentation research did not confirm their exact response field
// names, and this project does not guess at wire shapes.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/market-data/introduction
package market

// Symbol is one cross-margin trading pair's specification.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/market-data/get-symbols-cross-margin
type Symbol struct {
	Symbol         string `json:"symbol"`
	Name           string `json:"name"`
	EnableTrading  bool   `json:"enableTrading"`
	Market         string `json:"market"`
	BaseCurrency   string `json:"baseCurrency"`
	QuoteCurrency  string `json:"quoteCurrency"`
	BaseIncrement  string `json:"baseIncrement"`
	BaseMinSize    string `json:"baseMinSize"`
	BaseMaxSize    string `json:"baseMaxSize"`
	QuoteIncrement string `json:"quoteIncrement"`
	QuoteMinSize   string `json:"quoteMinSize"`
	QuoteMaxSize   string `json:"quoteMaxSize"`
	PriceIncrement string `json:"priceIncrement"`
	FeeCurrency    string `json:"feeCurrency"`
	PriceLimitRate string `json:"priceLimitRate"`
	MinFunds       string `json:"minFunds"`
}

// SymbolsPage is the envelope returned by GetSymbols.
type SymbolsPage struct {
	Timestamp int64    `json:"timestamp"`
	Items     []Symbol `json:"items"`
}

// IsolatedSymbol is one isolated-margin trading pair's specification. A
// structurally distinct shape from Symbol (cross) — not just a subset of
// fields.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/market-data/get-symbols-isolated-margin
type IsolatedSymbol struct {
	Symbol                 string `json:"symbol"`
	SymbolName             string `json:"symbolName"`
	BaseCurrency           string `json:"baseCurrency"`
	QuoteCurrency          string `json:"quoteCurrency"`
	MaxLeverage            int    `json:"maxLeverage"`
	FlDebtRatio            string `json:"flDebtRatio"`
	TradeEnable            bool   `json:"tradeEnable"`
	AutoRenewMaxDebtRatio  string `json:"autoRenewMaxDebtRatio"`
	BaseBorrowEnable       bool   `json:"baseBorrowEnable"`
	QuoteBorrowEnable      bool   `json:"quoteBorrowEnable"`
	BaseTransferInEnable   bool   `json:"baseTransferInEnable"`
	QuoteTransferInEnable  bool   `json:"quoteTransferInEnable"`
	BaseBorrowCoefficient  string `json:"baseBorrowCoefficient"`
	QuoteBorrowCoefficient string `json:"quoteBorrowCoefficient"`
}

// MarkPrice is a symbol's current mark price, used by both
// GetMarkPriceList (array) and GetMarkPriceDetail (single object).
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/market-data/get-mark-price-list
type MarkPrice struct {
	Symbol string `json:"symbol"`
	// TimePoint is a Unix millisecond timestamp.
	TimePoint int64   `json:"timePoint"`
	Value     float64 `json:"value"`
}

// Config holds account-wide margin trading limits.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/market-data/get-margin-config
type Config struct {
	CurrencyList     []string `json:"currencyList"`
	MaxLeverage      int      `json:"maxLeverage"`
	WarningDebtRatio string   `json:"warningDebtRatio"`
	LiqDebtRatio     string   `json:"liqDebtRatio"`
}

// RiskLimitCrossItem is one currency's cross-margin risk limit. A
// structurally distinct shape from RiskLimitIsolatedItem — cross-margin
// risk is scoped per currency, isolated per symbol.
//
// BorrowCoefficient is documented by KuCoin as "Abandoned" — present for
// backward compatibility only, do not rely on it.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/risk-limit/margin-trading-pair-configuration
type RiskLimitCrossItem struct {
	Timestamp         int64  `json:"timestamp"`
	Currency          string `json:"currency"`
	BorrowMaxAmount   string `json:"borrowMaxAmount"`
	BorrowMinAmount   string `json:"borrowMinAmount"`
	BorrowMinUnit     string `json:"borrowMinUnit"`
	BorrowEnabled     bool   `json:"borrowEnabled"`
	BuyMaxAmount      string `json:"buyMaxAmount"`
	HoldMaxAmount     string `json:"holdMaxAmount"`
	MarginCoefficient string `json:"marginCoefficient"`
	Precision         int    `json:"precision"`
	BorrowCoefficient string `json:"borrowCoefficient"` // Abandoned by KuCoin; kept for back-compat only
}

// RiskLimitIsolatedItem is one symbol's isolated-margin risk limit,
// broken out per base/quote leg.
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/risk-limit/margin-trading-pair-configuration
type RiskLimitIsolatedItem struct {
	Timestamp              int64  `json:"timestamp"`
	Symbol                 string `json:"symbol"`
	BaseMaxBorrowAmount    string `json:"baseMaxBorrowAmount"`
	QuoteMaxBorrowAmount   string `json:"quoteMaxBorrowAmount"`
	BaseMaxBuyAmount       string `json:"baseMaxBuyAmount"`
	QuoteMaxBuyAmount      string `json:"quoteMaxBuyAmount"`
	BaseMaxHoldAmount      string `json:"baseMaxHoldAmount"`
	QuoteMaxHoldAmount     string `json:"quoteMaxHoldAmount"`
	BasePrecision          int    `json:"basePrecision"`
	QuotePrecision         int    `json:"quotePrecision"`
	BaseBorrowCoefficient  string `json:"baseBorrowCoefficient"`
	QuoteBorrowCoefficient string `json:"quoteBorrowCoefficient"`
	BaseMarginCoefficient  string `json:"baseMarginCoefficient"`
	QuoteMarginCoefficient string `json:"quoteMarginCoefficient"`
	BaseBorrowMinAmount    string `json:"baseBorrowMinAmount"`
	BaseBorrowMinUnit      string `json:"baseBorrowMinUnit"`
	QuoteBorrowMinAmount   string `json:"quoteBorrowMinAmount"`
	QuoteBorrowMinUnit     string `json:"quoteBorrowMinUnit"`
	BaseBorrowEnabled      bool   `json:"baseBorrowEnabled"`
	QuoteBorrowEnabled     bool   `json:"quoteBorrowEnabled"`
}
