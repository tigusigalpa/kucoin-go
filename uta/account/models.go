// Package account implements KuCoin UTA's private account endpoints.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/account/get-account-overview-uta
package account

// Overview is the unified account's aggregate risk/margin snapshot.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/account/get-account-overview-uta
type Overview struct {
	AccountType     string `json:"accountType"`
	RiskRatio       string `json:"riskRatio"`
	Equity          string `json:"equity"`
	AdjustedEquity  string `json:"adjustedEquity"`
	Liability       string `json:"liability"`
	AvailableMargin string `json:"availableMargin"`
	Im              string `json:"im"`
	Mm              string `json:"mm"`
}

// CurrencyBalance is a single coin's balance within the unified account.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-account-currency-assets-uta
type CurrencyBalance struct {
	Currency         string `json:"currency"`
	Equity           string `json:"equity"`
	Hold             string `json:"hold"`
	Balance          string `json:"balance"`
	Available        string `json:"available"`
	Liability        string `json:"liability"`
	PotentialBorrow  string `json:"potentialBorrow"`
	CollateralStatus string `json:"collateralStatus"`
}

// accountGroup is the wire shape of Assets.Accounts: KuCoin documents this
// as an array containing exactly one object (no accountId/accountType
// discriminator, since there's only ever one element).
type accountGroup struct {
	Currencies []CurrencyBalance `json:"currencies"`
}

// Assets is the unified account's per-coin balance snapshot.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-account-currency-assets-uta
type Assets struct {
	AccountType string         `json:"accountType"`
	Ts          int64          `json:"ts"`
	Accounts    []accountGroup `json:"accounts"`
}

// Currencies returns the balance list, regardless of KuCoin's
// single-element-array wrapping. Safe to call even if Accounts is empty.
func (a Assets) Currencies() []CurrencyBalance {
	if len(a.Accounts) == 0 {
		return nil
	}
	return a.Accounts[0].Currencies
}

// FeeRate is the maker/taker fee for a single symbol.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-actual-fee
type FeeRate struct {
	Symbol       string `json:"symbol"`
	TakerFeeRate string `json:"takerFeeRate"`
	MakerFeeRate string `json:"makerFeeRate"`
}

// FeeRateList is the envelope returned by GetFeeRate.
type FeeRateList struct {
	TradeType string    `json:"tradeType"`
	List      []FeeRate `json:"list"`
}

// LedgerEntry is a single account ledger line item.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-account-ledger
type LedgerEntry struct {
	AccountType  string `json:"accountType"`
	ID           string `json:"id"`
	Currency     string `json:"currency"`
	Direction    string `json:"direction"`
	BusinessType string `json:"businessType"`
	Amount       string `json:"amount"`
	Balance      string `json:"balance"`
	Fee          string `json:"fee"`
	Tax          string `json:"tax"`
	Remark       string `json:"remark"`
	// Ts is a Unix nanosecond timestamp.
	Ts int64 `json:"ts"`
}

// LedgerPage is the cursor-paginated envelope returned by GetLedger.
type LedgerPage struct {
	LastID int64         `json:"lastId"`
	Items  []LedgerEntry `json:"items"`
}

// Mode describes the account's Classic/Unified mode and any linked
// sub-accounts.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-account-mode
type Mode struct {
	SelfAccountMode   string  `json:"selfAccountMode"`
	UnifiedSubAccount []int64 `json:"unifiedSubAccount"`
	ClassicSubAccount []int64 `json:"classicSubAccount"`
}

// APIKeyInfo describes the calling API key.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/get-apikey-info
type APIKeyInfo struct {
	UID        int64  `json:"uid"`
	ParentUID  int64  `json:"parentUid,omitempty"`
	Region     string `json:"region"`
	KycStatus  int    `json:"kycStatus"`
	SubName    string `json:"subName,omitempty"`
	Remark     string `json:"remark"`
	ApiKey     string `json:"apiKey"`
	ApiVersion int    `json:"apiVersion"`
	// Permission is a comma-separated list, e.g.
	// "General,Spot,Margin,Unified,Futures,InnerTransfer,Transfer,Earn".
	Permission    string `json:"permission"`
	IPWhitelist   string `json:"ipWhitelist,omitempty"`
	IsMaster      bool   `json:"isMaster"`
	CreatedAt     int64  `json:"createdAt"`
	ExpiredAt     int64  `json:"expiredAt,omitempty"`
	ThirdPartyApp string `json:"thirdPartyApp,omitempty"`
	SiteType      string `json:"siteType"`
}
