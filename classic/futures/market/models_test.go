package market

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/types"
)

// documentedSymbolFields are the 84 fields of KuCoin's response schema for Get
// Symbol and Get All Symbols, in the docs' order (extracted from the docs on
// 2026-10-03).
var documentedSymbolFields = []string{
	"symbol", "displaySymbol", "rootSymbol", "type", "firstOpenDate", "expireDate",
	"settleDate", "baseCurrency", "displayBaseCurrency", "quoteCurrency", "settleCurrency", "maxOrderQty",
	"marketMaxOrderQty", "maxPrice", "lotSize", "tickSize", "indexPriceTickSize", "multiplier",
	"initialMargin", "maintainMargin", "maxRiskLimit", "minRiskLimit", "riskStep", "makerFeeRate",
	"takerFeeRate", "takerFixFee", "makerFixFee", "settlementFee", "isDeleverage", "isQuanto",
	"isInverse", "markMethod", "fairMethod", "fundingBaseSymbol", "fundingQuoteSymbol", "fundingRateSymbol",
	"indexSymbol", "settlementSymbol", "status", "fundingFeeRate", "predictedFundingFeeRate", "fundingRateGranularity",
	"effectiveFundingRateCycleStartTime", "currentFundingRateGranularity", "fundingRateCap", "fundingRateFloor", "period", "openInterest",
	"turnoverOf24h", "volumeOf24h", "markPrice", "indexPrice", "lastTradePrice", "nextFundingRateTime",
	"nextFundingRateDateTime", "maxLeverage", "sourceExchanges", "premiumsSymbol1M", "premiumsSymbol8H", "fundingBaseSymbol1M",
	"fundingQuoteSymbol1M", "lowPrice", "highPrice", "priceChgPct", "priceChg", "k",
	"m", "f", "mmrLimit", "mmrLevConstant", "supportCross", "buyLimit",
	"sellLimit", "adjustK", "adjustM", "adjustMmrLevConstant", "adjustActiveTime", "crossRiskLimit",
	"marketStage", "preMarketToPerpDate", "orderPriceRange", "marketType", "assetClass", "subMarketType",
}

// undocumentedSymbolFields are returned by the live API, and shown in the docs'
// example response, but missing from the docs' field table.
var undocumentedSymbolFields = []string{"dailyInterestRate", "lastTimeFundingRate"}

var (
	decimalType = reflect.TypeOf(types.Decimal(""))
	int64Type   = reflect.TypeOf(types.Int64(0))
)

func jsonName(f reflect.StructField) string {
	name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
	return name
}

// symbolTags returns the JSON name of every field of Symbol, in order.
func symbolTags() []string {
	rt := reflect.TypeOf(Symbol{})
	tags := make([]string, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		tags = append(tags, jsonName(rt.Field(i)))
	}
	return tags
}

func TestKline_UnmarshalJSON(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want Kline
	}{
		{
			name: "seven numbers, as the live API sends them",
			in:   `[1790982120000,84483.5,84499.4,84483.5,84499.4,322,27205.9061]`,
			want: Kline{Time: 1790982120000, Open: "84483.5", High: "84499.4", Low: "84483.5", Close: "84499.4", Volume: "322", Turnover: "27205.9061"},
		},
		{
			name: "six numbers, without the turnover column",
			in:   `[1575331200000,7495.01,8309.67,7250,7463.55,0]`,
			want: Kline{Time: 1575331200000, Open: "7495.01", High: "8309.67", Low: "7250", Close: "7463.55", Volume: "0"},
		},
		{
			name: "seven strings",
			in:   `["1790982120000","84483.5","84499.4","84483.5","84499.4","322","27205.9061"]`,
			want: Kline{Time: 1790982120000, Open: "84483.5", High: "84499.4", Low: "84483.5", Close: "84499.4", Volume: "322", Turnover: "27205.9061"},
		},
		{
			name: "six strings",
			in:   `["1790982120000","1","2","0.5","1.5","10"]`,
			want: Kline{Time: 1790982120000, Open: "1", High: "2", Low: "0.5", Close: "1.5", Volume: "10"},
		},
		{
			name: "numbers and strings mixed, exponent notation kept",
			in:   `[1.79098212E12,1.0E-4,"2.0E-4",5E-5,"1.5E-4",1E3,1.5E-1]`,
			want: Kline{Time: 1790982120000, Open: "1.0E-4", High: "2.0E-4", Low: "5E-5", Close: "1.5E-4", Volume: "1E3", Turnover: "1.5E-1"},
		},
		{
			name: "trailing zeros are preserved",
			in:   `[1790982300000,84500.4,84501.4,84477.0,84477.0,142,11998.4579]`,
			want: Kline{Time: 1790982300000, Open: "84500.4", High: "84501.4", Low: "84477.0", Close: "84477.0", Volume: "142", Turnover: "11998.4579"},
		},
		{
			name: "null prices decode as empty decimals",
			in:   `[1,null,null,null,null,null,null]`,
			want: Kline{Time: 1},
		},
		{
			name: "whitespace",
			in:   " [ 1 , 2 , 3 , 4 , 5 , 6 , 7 ] ",
			want: Kline{Time: 1, Open: "2", High: "3", Low: "4", Close: "5", Volume: "6", Turnover: "7"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got Kline
			if err := json.Unmarshal([]byte(tc.in), &got); err != nil {
				t.Fatalf("Unmarshal(%s): %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("Kline = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestKline_UnmarshalJSON_Malformed(t *testing.T) {
	cases := []struct {
		name, in, wantErr string
	}{
		{"object", `{"time":1}`, "decode futures kline array"},
		{"string", `"abc"`, "decode futures kline array"},
		{"number", `5`, "decode futures kline array"},
		{"boolean", `true`, "decode futures kline array"},
		{"empty array", `[]`, "want 6 or 7 elements, got 0"},
		{"five elements", `[1,2,3,4,5]`, "want 6 or 7 elements, got 5"},
		{"eight elements", `[1,2,3,4,5,6,7,8]`, "want 6 or 7 elements, got 8"},
		{"time is not numeric", `["abc",1,2,3,4,5,6]`, "kline time"},
		{"time is fractional", `[1.5,1,2,3,4,5,6]`, "kline time"},
		{"time is a boolean", `[true,1,2,3,4,5,6]`, "kline time"},
		{"time is null", `[null,1,2,3,4,5,6]`, "kline time: missing value"},
		{"time is an empty string", `["",1,2,3,4,5,6]`, "kline time: missing value"},
		{"open is a boolean", `[1,true,2,3,4,5,6]`, "kline open"},
		{"high is an object", `[1,2,{},3,4,5,6]`, "kline high"},
		{"low is an array", `[1,2,3,[1],4,5,6]`, "kline low"},
		{"close is a boolean", `[1,2,3,4,false,5,6]`, "kline close"},
		{"volume is an object", `[1,2,3,4,5,{},6]`, "kline volume"},
		{"turnover is an array", `[1,2,3,4,5,6,[7]]`, "kline turnover"},
		{"six elements with a bad volume", `[1,2,3,4,5,{}]`, "kline volume"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			original := Kline{Time: 7, Open: "keep"}
			k := original
			err := json.Unmarshal([]byte(tc.in), &k)
			if err == nil {
				t.Fatalf("Unmarshal(%s) = %+v, want an error", tc.in, k)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err, tc.wantErr)
			}
			if k != original {
				t.Errorf("a failed decode modified the Kline: %+v", k)
			}
		})
	}

	t.Run("an error in a list names the broken candle", func(t *testing.T) {
		var list []Kline
		err := json.Unmarshal([]byte(`[[1,2,3,4,5,6,7],[1,2,3]]`), &list)
		if err == nil || !strings.Contains(err.Error(), "got 3") {
			t.Errorf("error = %v, want one mentioning the 3-element candle", err)
		}
	})
}

func TestKline_UnmarshalJSON_Null(t *testing.T) {
	k := Kline{Time: 9, Open: "1"}
	if err := json.Unmarshal([]byte(`null`), &k); err != nil {
		t.Fatalf("Unmarshal(null): %v", err)
	}
	if k != (Kline{Time: 9, Open: "1"}) {
		t.Errorf("null changed the Kline: %+v", k)
	}

	var list []Kline
	if err := json.Unmarshal([]byte(`[null]`), &list); err != nil || len(list) != 1 || list[0] != (Kline{}) {
		t.Errorf("[null] = %+v, %v; want one zero Kline", list, err)
	}
}

func TestKline_MarshalJSON(t *testing.T) {
	seven := Kline{Time: 1790982120000, Open: "84483.5", High: "84499.4", Low: "84483.5", Close: "84499.4", Volume: "322", Turnover: "27205.9061"}
	six := Kline{Time: 1575331200000, Open: "7495.01", High: "8309.67", Low: "7250", Close: "7463.55", Volume: "0"}

	for _, tc := range []struct {
		name string
		k    Kline
		want string
	}{
		{"with turnover", seven, `[1790982120000,"84483.5","84499.4","84483.5","84499.4","322","27205.9061"]`},
		{"without turnover", six, `[1575331200000,"7495.01","8309.67","7250","7463.55","0"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.k)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("Marshal = %s, want %s", got, tc.want)
			}
			var back Kline
			if err := json.Unmarshal(got, &back); err != nil {
				t.Fatalf("Unmarshal(%s): %v", got, err)
			}
			if back != tc.k {
				t.Errorf("round trip = %+v, want %+v", back, tc.k)
			}
		})
	}

	t.Run("inside a slice and through a pointer", func(t *testing.T) {
		got, err := json.Marshal([]*Kline{&six, &seven})
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		want := `[[1575331200000,"7495.01","8309.67","7250","7463.55","0"],[1790982120000,"84483.5","84499.4","84483.5","84499.4","322","27205.9061"]]`
		if string(got) != want {
			t.Errorf("Marshal = %s, want %s", got, want)
		}
	})
}

func TestKline_Timestamp(t *testing.T) {
	got := Kline{Time: 1790982120000}.Timestamp()
	if want := time.UnixMilli(1790982120000).UTC(); !got.Equal(want) || got.Location() != time.UTC {
		t.Errorf("Timestamp() = %v, want %v in UTC", got, want)
	}
	if !(Kline{}).Timestamp().IsZero() {
		t.Error("a zero Time must give the zero time.Time")
	}
}

func TestSymbol_MapsEveryDocumentedField(t *testing.T) {
	if len(documentedSymbolFields) != 84 {
		t.Fatalf("the documented field list has %d entries, want 84", len(documentedSymbolFields))
	}
	tags := symbolTags()
	seen := map[string]int{}
	for _, tag := range tags {
		seen[tag]++
	}
	for tag, n := range seen {
		if tag == "" || n != 1 {
			t.Errorf("JSON name %q is used by %d fields", tag, n)
		}
	}
	for _, name := range documentedSymbolFields {
		if seen[name] != 1 {
			t.Errorf("documented field %q is not mapped exactly once", name)
		}
	}
	for _, name := range undocumentedSymbolFields {
		if seen[name] != 1 {
			t.Errorf("undocumented live field %q is not mapped exactly once", name)
		}
	}
	if want := len(documentedSymbolFields) + len(undocumentedSymbolFields); len(tags) != want {
		t.Errorf("Symbol has %d fields, want %d (documented plus undocumented)", len(tags), want)
	}
}

func TestSymbol_EveryFieldToleratesNull(t *testing.T) {
	parts := make([]string, 0, len(symbolTags()))
	for _, tag := range symbolTags() {
		parts = append(parts, strconv.Quote(tag)+":null")
	}
	var got Symbol
	if err := json.Unmarshal([]byte("{"+strings.Join(parts, ",")+"}"), &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, Symbol{}) {
		t.Errorf("an all-null contract is not the zero Symbol: %+v", got)
	}
}

func TestSymbol_NumericFieldsAcceptBothJSONForms(t *testing.T) {
	rt := reflect.TypeOf(Symbol{})
	for _, form := range []struct{ name, decimal, integer string }{
		{"JSON numbers", `1.5E-3`, `42`},
		{"JSON strings", `"1.5E-3"`, `"42"`},
	} {
		t.Run(form.name, func(t *testing.T) {
			var parts []string
			for i := 0; i < rt.NumField(); i++ {
				switch f := rt.Field(i); f.Type {
				case decimalType:
					parts = append(parts, strconv.Quote(jsonName(f))+":"+form.decimal)
				case int64Type:
					parts = append(parts, strconv.Quote(jsonName(f))+":"+form.integer)
				}
			}
			var got Symbol
			if err := json.Unmarshal([]byte("{"+strings.Join(parts, ",")+"}"), &got); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			decimals, ints := 0, 0
			gv := reflect.ValueOf(got)
			for i := 0; i < rt.NumField(); i++ {
				switch f := rt.Field(i); f.Type {
				case decimalType:
					decimals++
					if v := gv.Field(i).Interface().(types.Decimal); v != "1.5E-3" {
						t.Errorf("%s = %q, want the literal 1.5E-3", f.Name, v)
					}
				case int64Type:
					ints++
					if v := gv.Field(i).Interface().(types.Int64); v != 42 {
						t.Errorf("%s = %d, want 42", f.Name, v)
					}
				}
			}
			if decimals == 0 || ints == 0 {
				t.Errorf("covered %d decimal and %d integer fields", decimals, ints)
			}
		})
	}
}

func TestSymbol_IgnoresUnknownFieldsAndKeepsUndocumentedOnes(t *testing.T) {
	in := `{"symbol":"NEWUSDTM","dailyInterestRate":3.0E-4,"lastTimeFundingRate":-1.0E-6,` +
		`"someFutureField":{"nested":[1,2,{"x":null}]},"anotherOne":"x","type":"FUTURE_KIND_NOT_YET_DOCUMENTED"}`
	var got Symbol
	if err := json.Unmarshal([]byte(in), &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	checkFields(t, got, map[string]string{
		"Symbol": "NEWUSDTM", "DailyInterestRate": "3.0E-4", "LastTimeFundingRate": "-1.0E-6",
		"Type": "FUTURE_KIND_NOT_YET_DOCUMENTED", // enums stay open strings
	})
}

func TestOrderBook_DecodesLevelsInEveryObservedForm(t *testing.T) {
	t.Run("numbers, strings and exponents", func(t *testing.T) {
		in := `{"sequence":"1748100435563","symbol":"XBTUSDTM","bids":[[84476.9,488],["84476.8","4"],[84474.0,1468],[1.0E-4,"3"]],"asks":[[84477.0,38]],"ts":"1790982408793000000"}`
		var got OrderBook
		if err := json.Unmarshal([]byte(in), &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		checkFields(t, got, map[string]string{"Sequence": "1748100435563", "Symbol": "XBTUSDTM", "Timestamp": "1790982408793000000"})
		levels(t, got.Bids,
			orderbook.Level{Price: "84476.9", Size: "488"},
			orderbook.Level{Price: "84476.8", Size: "4"},
			orderbook.Level{Price: "84474.0", Size: "1468"},
			orderbook.Level{Price: "1.0E-4", Size: "3"},
		)
		levels(t, got.Asks, orderbook.Level{Price: "84477.0", Size: "38"})
	})

	t.Run("symbol is optional", func(t *testing.T) {
		var got OrderBook
		if err := json.Unmarshal([]byte(`{"sequence":1,"bids":[[1,2]],"asks":[[3,4]],"ts":5}`), &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if got.Symbol != "" || got.Sequence != 1 || got.Timestamp != 5 {
			t.Errorf("book = %+v", got)
		}
		levels(t, got.Bids, orderbook.Level{Price: "1", Size: "2"})
	})

	t.Run("null sides", func(t *testing.T) {
		var got OrderBook
		if err := json.Unmarshal([]byte(`{"sequence":1,"symbol":"X","bids":null,"asks":null,"ts":5}`), &got); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if got.Bids != nil || got.Asks != nil {
			t.Errorf("sides = %v / %v, want nil", got.Bids, got.Asks)
		}
	})

	t.Run("a malformed level is an error", func(t *testing.T) {
		var got OrderBook
		err := json.Unmarshal([]byte(`{"bids":[[84476.9]]}`), &got)
		if err == nil || !strings.Contains(err.Error(), "order-book level") {
			t.Errorf("error = %v, want one about the order-book level", err)
		}
	})
}

func TestIndexPricePage_UsesKucoinsMisspelledKey(t *testing.T) {
	in := `{"hasMore":true,"dataList":[{"symbol":".KXBTUSDT","granularity":1000,"timePoint":1730557515000,"value":69202.94,` +
		`"decomposionList":[{"exchange":"gateio","price":69209.27,"weight":0.0533},{"exchange":"bitmart","price":69230.77,"weight":0.0128}]}]}`
	var page IndexPricePage
	if err := json.Unmarshal([]byte(in), &page); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !page.HasMore || len(page.DataList) != 1 {
		t.Fatalf("page = %+v", page)
	}
	want := []IndexComponent{
		{Exchange: "gateio", Price: "69209.27", Weight: "0.0533"},
		{Exchange: "bitmart", Price: "69230.77", Weight: "0.0128"},
	}
	if got := page.DataList[0].DecompositionList; !reflect.DeepEqual(got, want) {
		t.Errorf("DecompositionList = %+v, want %+v", got, want)
	}

	// The wire name is KuCoin's, not the Go field's.
	out, err := json.Marshal(page.DataList[0])
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(out), `"decomposionList"`) || strings.Contains(string(out), `"decompositionList"`) {
		t.Errorf("Marshal = %s, want the key decomposionList", out)
	}

	t.Run("the list is optional", func(t *testing.T) {
		var p IndexPricePage
		if err := json.Unmarshal([]byte(`{"dataList":[{"symbol":".KXBTUSDT","value":1}],"hasMore":false}`), &p); err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if len(p.DataList) != 1 || p.DataList[0].DecompositionList != nil {
			t.Errorf("page = %+v", p)
		}
	})
}

func TestFundingRatePoint_UsesLowercaseTimepointKey(t *testing.T) {
	var got []FundingRatePoint
	if err := json.Unmarshal([]byte(`[{"symbol":"XBTUSDTM","fundingRate":2.1E-4,"timepoint":1702296000000}]`), &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(got) != 1 || got[0].TimePoint != 1702296000000 || got[0].FundingRate != "2.1E-4" {
		t.Errorf("points = %+v", got)
	}
}

func TestTimeHelpers(t *testing.T) {
	const ms, ns = 1790982404079, 1790982404079123456
	wantMs, wantNs := time.UnixMilli(ms).UTC(), time.Unix(0, ns).UTC()

	for _, tc := range []struct {
		name      string
		got, zero time.Time
		want      time.Time
	}{
		{"Ticker", Ticker{Timestamp: ns}.Time(), Ticker{}.Time(), wantNs},
		{"Trade", Trade{Timestamp: ns}.Time(), Trade{}.Time(), wantNs},
		{"OrderBook", OrderBook{Timestamp: ns}.Time(), OrderBook{}.Time(), wantNs},
		{"MarkPrice", MarkPrice{TimePoint: ms}.Time(), MarkPrice{}.Time(), wantMs},
		{"IndexPrice", IndexPrice{TimePoint: ms}.Time(), IndexPrice{}.Time(), wantMs},
		{"InterestRate", InterestRate{TimePoint: ms}.Time(), InterestRate{}.Time(), wantMs},
		{"PremiumIndex", PremiumIndex{TimePoint: ms}.Time(), PremiumIndex{}.Time(), wantMs},
		{"FundingRate.Time", FundingRate{TimePoint: ms}.Time(), FundingRate{}.Time(), wantMs},
		{"FundingRate.NextFundingTime", FundingRate{FundingTime: ms}.NextFundingTime(), FundingRate{}.NextFundingTime(), wantMs},
		{"FundingRatePoint", FundingRatePoint{TimePoint: ms}.Time(), FundingRatePoint{}.Time(), wantMs},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.got.Equal(tc.want) || tc.got.Location() != time.UTC {
				t.Errorf("time = %v, want %v in UTC", tc.got, tc.want)
			}
			if !tc.zero.IsZero() {
				t.Errorf("an absent timestamp gave %v, want the zero time", tc.zero)
			}
		})
	}
	if got := (Ticker{Timestamp: ns}).Time(); got.Nanosecond() != 79123456 {
		t.Errorf("sub-second part = %d, want 79123456 (nanosecond precision kept)", got.Nanosecond())
	}
}

func TestGranularity(t *testing.T) {
	valid := []struct {
		g       Granularity
		minutes int
		label   string
	}{
		{Granularity1Min, 1, "1min"}, {Granularity3Min, 3, "3min"}, {Granularity5Min, 5, "5min"}, {Granularity15Min, 15, "15min"}, {Granularity30Min, 30, "30min"},
		{Granularity1Hour, 60, "1hour"}, {Granularity2Hour, 120, "2hour"}, {Granularity4Hour, 240, "4hour"}, {Granularity8Hour, 480, "8hour"},
		{Granularity12Hour, 720, "12hour"}, {Granularity1Day, 1440, "1day"}, {Granularity1Week, 10080, "1week"}, {Granularity1Month, 43200, "1month"},
	}
	for _, tc := range valid {
		if int(tc.g) != tc.minutes {
			t.Errorf("%s = %d minutes, want %d", tc.label, int(tc.g), tc.minutes)
		}
		if !tc.g.Valid() {
			t.Errorf("%s is not Valid", tc.label)
		}
		if tc.g.String() != tc.label {
			t.Errorf("String() = %q, want %q", tc.g.String(), tc.label)
		}
	}
	for _, g := range []Granularity{0, -1, 2, 4, 61, 10081, 43201} {
		if g.Valid() {
			t.Errorf("Granularity(%d) is Valid", int(g))
		}
		if want := fmt.Sprintf("Granularity(%d)", int(g)); g.String() != want {
			t.Errorf("String() = %q, want %q", g.String(), want)
		}
	}
}

func TestEnumConstantsMatchTheDocumentedValues(t *testing.T) {
	for _, tc := range []struct{ got, want string }{
		{string(ContractTypeFFWCSX), "FFWCSX"}, {string(ContractTypeFFICSX), "FFICSX"},
		{string(ContractStatusInit), "Init"}, {string(ContractStatusOpen), "Open"}, {string(ContractStatusBeingSettled), "BeingSettled"},
		{string(ContractStatusSettled), "Settled"}, {string(ContractStatusPaused), "Paused"}, {string(ContractStatusClosed), "Closed"},
		{string(ContractStatusCancelOnly), "CancelOnly"},
		{string(MarketStageNormal), "NORMAL"}, {string(MarketStagePreMarket), "PRE_MARKET"},
		{string(MarketTypeCrypto), "CRYPTO"}, {string(MarketTypeNasdaq), "NASDAQ"},
		{string(AssetClassCrypto), "CRYPTO"}, {string(AssetClassMetal), "METAL"}, {string(AssetClassCommodity), "COMMODITY"}, {string(AssetClassStock), "STOCK"},
		{string(SubMarketTypeUSStock), "US.STOCK"}, {string(SubMarketTypeKRStock), "KR.STOCK"},
		{string(SubMarketTypeHKStock), "HK.STOCK"}, {string(SubMarketTypeJPStock), "JP.STOCK"},
		{string(SideBuy), "buy"}, {string(SideSell), "sell"},
		{string(ServiceStateOpen), "open"}, {string(ServiceStateClose), "close"}, {string(ServiceStateCancelOnly), "cancelonly"},
	} {
		if tc.got != tc.want {
			t.Errorf("constant = %q, want %q", tc.got, tc.want)
		}
	}
}

func TestModelsHaveNoFloatFields(t *testing.T) {
	roots := []any{
		Symbol{}, Ticker{}, Trade{}, OrderBook{}, Kline{}, MarkPrice{}, IndexPricePage{}, InterestRatePage{}, PremiumIndexPage{},
		Stats24h{}, ServiceStatus{}, FundingRate{}, FundingRatePoint{},
		KlineOptions{}, IndexOptions{}, FundingHistoryOptions{},
	}
	seen := map[reflect.Type]bool{}
	var walk func(path string, rt reflect.Type)
	walk = func(path string, rt reflect.Type) {
		switch rt.Kind() {
		case reflect.Float32, reflect.Float64:
			t.Errorf("%s is a %v; use types.Decimal for fractional numbers", path, rt)
		case reflect.Slice, reflect.Array, reflect.Pointer:
			walk(path+"[]", rt.Elem())
		case reflect.Struct:
			if seen[rt] {
				return
			}
			seen[rt] = true
			for i := 0; i < rt.NumField(); i++ {
				walk(path+"."+rt.Field(i).Name, rt.Field(i).Type)
			}
		}
	}
	for _, root := range roots {
		walk(reflect.TypeOf(root).Name(), reflect.TypeOf(root))
	}
}

var idType = reflect.TypeOf(types.ID(""))

// isNumericScalar reports whether a JSON scalar is a number, or a string that
// holds one, which is how KuCoin sends prices, sizes, rates and identifiers.
func isNumericScalar(raw json.RawMessage) bool {
	text := strings.TrimSpace(string(raw))
	if strings.HasPrefix(text, `"`) {
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return false
		}
		text = s
	}
	return types.Decimal(text).Valid()
}

// numberTypeViolations walks a raw JSON value together with the Go type it
// decodes into and names every place where a JSON number, or a numeric string,
// lands in a type other than types.Decimal, types.Int64 or types.ID, which are
// the only types that keep it exact and tolerate both of KuCoin's spellings.
// Keys the model does not map are ignored, as encoding/json ignores them.
func numberTypeViolations(path string, rt reflect.Type, raw json.RawMessage) []string {
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	text := strings.TrimSpace(string(raw))
	switch {
	case strings.HasPrefix(text, "{") && rt.Kind() == reflect.Struct:
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return []string{path + ": " + err.Error()}
		}
		byName := map[string]reflect.Type{}
		for i := 0; i < rt.NumField(); i++ {
			byName[jsonName(rt.Field(i))] = rt.Field(i).Type
		}
		var out []string
		for key, value := range fields {
			if ft, ok := byName[key]; ok {
				out = append(out, numberTypeViolations(path+"."+key, ft, value)...)
			}
		}
		return out
	case strings.HasPrefix(text, "[") && rt.Kind() == reflect.Slice:
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return []string{path + ": " + err.Error()}
		}
		var out []string
		for i, item := range items {
			out = append(out, numberTypeViolations(fmt.Sprintf("%s[%d]", path, i), rt.Elem(), item)...)
		}
		return out
	case strings.HasPrefix(text, "[") || strings.HasPrefix(text, "{"):
		return nil // arrays with their own decoder (orderbook.Level, Kline)
	}
	if isNumericScalar(raw) && rt != decimalType && rt != int64Type && rt != idType {
		return []string{fmt.Sprintf("%s carries %s but is a %v", path, text, rt)}
	}
	return nil
}

func TestNumberTypeViolations_DetectsUnsafeFields(t *testing.T) {
	// A negative control for the guard below: it must flag exactly the fields
	// that would lose precision or reject one of KuCoin's number spellings.
	type unsafe struct {
		Float  float64       `json:"float"`
		Text   string        `json:"text"`
		Plain  int64         `json:"plain"`
		Narrow int           `json:"narrow"`
		Raw    json.Number   `json:"raw"`
		Good1  types.Decimal `json:"good1"`
		Good2  types.Int64   `json:"good2"`
		Good3  types.ID      `json:"good3"`
		Word   string        `json:"word"`
		Nested []struct {
			Price float64 `json:"price"`
		} `json:"nested"`
	}
	raw := json.RawMessage(`{"float":1.5,"text":"84477","plain":3,"narrow":4,"raw":5,"good1":1.5,"good2":"7","good3":"9",` +
		`"word":"XBTUSDTM","nested":[{"price":2.5}],"unknown":6}`)
	got := numberTypeViolations("unsafe", reflect.TypeOf(unsafe{}), raw)
	if len(got) != 6 {
		t.Fatalf("violations = %q, want 6 (float, text, plain, narrow, raw and nested price)", got)
	}
	for _, field := range []string{".float", ".text", ".plain", ".narrow", ".raw", ".nested[0].price"} {
		found := false
		for _, v := range got {
			found = found || strings.Contains(v, field+" carries")
		}
		if !found {
			t.Errorf("no violation reported for %s: %q", field, got)
		}
	}

	safe := struct {
		A types.Decimal `json:"a"`
		B types.Int64   `json:"b"`
	}{}
	if v := numberTypeViolations("safe", reflect.TypeOf(safe), json.RawMessage(`{"a":"1.0E-4","b":2}`)); len(v) != 0 {
		t.Errorf("violations for safe types = %q", v)
	}
}

func TestResponseModels_NumericFieldsUseTolerantTypes(t *testing.T) {
	cases := []struct {
		fixture string
		model   reflect.Type
	}{
		{"contracts_XBTUSDTM.json", reflect.TypeOf(Symbol{})},
		{"contracts_active_trimmed.json", reflect.TypeOf([]Symbol{})},
		{"ticker.json", reflect.TypeOf(Ticker{})},
		{"all_tickers_trimmed.json", reflect.TypeOf([]Ticker{})},
		{"trade_history_trimmed.json", reflect.TypeOf([]Trade{})},
		{"level2_snapshot_trimmed.json", reflect.TypeOf(OrderBook{})},
		{"level2_depth20.json", reflect.TypeOf(OrderBook{})},
		{"level2_depth100.json", reflect.TypeOf(OrderBook{})},
		{"mark_price.json", reflect.TypeOf(MarkPrice{})},
		{"index_query.json", reflect.TypeOf(IndexPricePage{})},
		{"interest_query.json", reflect.TypeOf(InterestRatePage{})},
		{"premium_query.json", reflect.TypeOf(PremiumIndexPage{})},
		{"funding_current.json", reflect.TypeOf(FundingRate{})},
		{"funding_history.json", reflect.TypeOf([]FundingRatePoint{})},
		{"status.json", reflect.TypeOf(ServiceStatus{})},
	}
	for _, tc := range cases {
		t.Run(tc.fixture, func(t *testing.T) {
			var env struct {
				Data json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(fixture(t, tc.fixture), &env); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}
			for _, v := range numberTypeViolations(tc.fixture, tc.model, env.Data) {
				t.Error(v)
			}
		})
	}

	// The two payloads that are not in the fixtures.
	for _, v := range numberTypeViolations("stats", reflect.TypeOf(Stats24h{}), json.RawMessage(`{"turnoverOf24h":1.1155733413273683E9}`)) {
		t.Error(v)
	}
	for _, v := range numberTypeViolations("server time", int64Type, json.RawMessage(`1790982415541`)) {
		t.Error(v)
	}
}

// The tests that read live captures are opt-in: set KUCOIN_FUTURES_LIVE_CAPTURE_DIR
// to a directory holding responses recorded from the live public API (among them
// the complete list of contracts, contracts_active.json, which is too large to
// keep as a fixture). Without it they skip, so CI never depends on the network or
// on a particular machine.
const liveCaptureEnv = "KUCOIN_FUTURES_LIVE_CAPTURE_DIR"

func liveCapture(t *testing.T, name string) []byte {
	t.Helper()
	dir := os.Getenv(liveCaptureEnv)
	if dir == "" {
		t.Skipf("set %s to run the test against live captures", liveCaptureEnv)
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	if errors.Is(err, fs.ErrNotExist) {
		t.Skipf("live capture %s is not available in %s", name, dir)
	}
	if err != nil {
		t.Fatalf("read live capture %s: %v", name, err)
	}
	return data
}

// liveData returns the raw "data" member of a captured response.
func liveData(t *testing.T, name string) json.RawMessage {
	t.Helper()
	var env struct {
		Code string          `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(liveCapture(t, name), &env); err != nil {
		t.Fatalf("decode envelope of %s: %v", name, err)
	}
	if env.Code != "200000" {
		t.Fatalf("%s has code %q, want 200000", name, env.Code)
	}
	return env.Data
}

// literal returns the text a JSON value carries: the unquoted text of a string,
// the source text of a number, and "" for null.
func literal(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	text := strings.TrimSpace(string(raw))
	switch {
	case text == "null":
		return ""
	case strings.HasPrefix(text, `"`):
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			t.Fatalf("decode string %s: %v", raw, err)
		}
		return s
	}
	return text
}

func TestSymbol_DecodesAllLiveContracts(t *testing.T) {
	data := liveData(t, "contracts_active.json")

	var objects []json.RawMessage
	if err := json.Unmarshal(data, &objects); err != nil {
		t.Fatalf("split contracts: %v", err)
	}
	if len(objects) == 0 {
		t.Fatal("the capture holds no contracts")
	}
	t.Logf("decoding %d live contracts", len(objects))

	known := map[string]bool{}
	for _, tag := range symbolTags() {
		known[tag] = true
	}
	rt := reflect.TypeOf(Symbol{})

	for i, obj := range objects {
		var s Symbol
		if err := json.Unmarshal(obj, &s); err != nil {
			t.Fatalf("contract %d: %v", i, err)
		}
		if s.Symbol == "" || !s.MarkPrice.Valid() || !s.TickSize.Valid() || !s.Multiplier.Valid() {
			t.Errorf("contract %d (%q) decoded without its basic fields: %+v", i, s.Symbol, s)
			continue
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(obj, &fields); err != nil {
			t.Fatalf("contract %d: %v", i, err)
		}
		sv := reflect.ValueOf(s)
		for key := range fields {
			if !known[key] {
				t.Errorf("%s: live field %q is not mapped by Symbol", s.Symbol, key)
			}
		}
		// Nothing was rounded or reformatted: every Decimal keeps its source
		// literal and every Int64 its source integer.
		for j := 0; j < rt.NumField(); j++ {
			field := rt.Field(j)
			raw, present := fields[jsonName(field)]
			if !present {
				t.Errorf("%s: field %q is absent from the live object", s.Symbol, jsonName(field))
				continue
			}
			switch field.Type {
			case decimalType:
				if got, want := string(sv.Field(j).Interface().(types.Decimal)), literal(t, raw); got != want {
					t.Errorf("%s.%s = %q, want the source literal %q", s.Symbol, field.Name, got, want)
				}
			case int64Type:
				if want := literal(t, raw); want != "" {
					if got := strconv.FormatInt(int64(sv.Field(j).Interface().(types.Int64)), 10); got != want {
						t.Errorf("%s.%s = %s, want the source integer %s", s.Symbol, field.Name, got, want)
					}
				}
			}
		}
	}

	// The whole list decodes in one pass, as GetAllSymbols does it.
	var all []Symbol
	if err := json.Unmarshal(data, &all); err != nil {
		t.Fatalf("decode the full list: %v", err)
	}
	if len(all) != len(objects) {
		t.Errorf("decoded %d contracts, the capture holds %d", len(all), len(objects))
	}
}

func TestLiveCapture_TickersTradesAndBooks(t *testing.T) {
	t.Run("all tickers", func(t *testing.T) {
		var tickers []Ticker
		if err := json.Unmarshal(liveData(t, "allTickers.json"), &tickers); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(tickers) == 0 {
			t.Fatal("no tickers")
		}
		for _, tk := range tickers {
			if tk.Symbol == "" || tk.TradeID == "" || tk.Sequence <= 0 || tk.Timestamp < 1e18 ||
				(tk.Side != SideBuy && tk.Side != SideSell) || !tk.Price.Valid() || !tk.BestBidPrice.Valid() || !tk.BestAskPrice.Valid() {
				t.Errorf("ticker decoded incompletely: %+v", tk)
			}
		}
	})

	t.Run("trade history", func(t *testing.T) {
		var trades []Trade
		if err := json.Unmarshal(liveData(t, "trade_history.json"), &trades); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(trades) != 100 {
			t.Fatalf("got %d trades, want the documented last 100", len(trades))
		}
		for i, tr := range trades {
			if tr.TradeID == "" || tr.MakerOrderID == "" || tr.TakerOrderID == "" || tr.Size <= 0 || tr.Timestamp < 1e18 || !tr.Price.Valid() {
				t.Errorf("trade %d decoded incompletely: %+v", i, tr)
			}
			if i > 0 && trades[i-1].Sequence < tr.Sequence {
				t.Errorf("trade %d is newer than trade %d; history is newest first", i, i-1)
			}
		}
	})

	t.Run("full order book", func(t *testing.T) {
		var book OrderBook
		if err := json.Unmarshal(liveData(t, "level2_snapshot.json"), &book); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(book.Bids) != 1000 || len(book.Asks) != 1000 {
			t.Fatalf("got %d bids / %d asks, want 1000 each", len(book.Bids), len(book.Asks))
		}
		for _, side := range []struct {
			name   string
			levels []orderbook.Level
			want   int // required sign of cmp(previous, next)
		}{{"bids", book.Bids, 1}, {"asks", book.Asks, -1}} {
			for i, l := range side.levels {
				if !l.Price.Valid() || !l.Size.Valid() {
					t.Fatalf("%s[%d] = %+v is not numeric", side.name, i, l)
				}
				if i == 0 {
					continue
				}
				if c, err := side.levels[i-1].Price.Cmp(l.Price); err != nil || c != side.want {
					t.Fatalf("%s[%d] = %s does not follow %s in the documented order", side.name, i, l.Price, side.levels[i-1].Price)
				}
			}
		}
		if c, err := book.Bids[0].Price.Cmp(book.Asks[0].Price); err != nil || c >= 0 {
			t.Errorf("best bid %s is not below best ask %s", book.Bids[0].Price, book.Asks[0].Price)
		}
		if book.Symbol == "" || book.Sequence <= 0 || book.Timestamp < 1e18 {
			t.Errorf("book header decoded incompletely: %+v", book)
		}
	})
}
