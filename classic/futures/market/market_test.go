package market

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/transport"
)

// The files in testdata are responses captured from KuCoin's live public
// Classic Futures API on 2026-10-03, trimmed to a few records where the
// original is large; every value that was kept is byte-identical to the
// capture. They show where the live wire format differs from the docs: prices
// and order-book levels as bare JSON numbers, rates in Java exponent form
// (1.0E-4), JSON null for inapplicable fields, nanosecond timestamps that do not
// fit a float64.
//
//go:embed testdata/*.json
var testdata embed.FS

// testCredentials are configured on every test client so that the tests prove
// the public endpoints stay unsigned even when credentials are available.
var testCredentials = transport.Credentials{
	APIKey:        "test-key",
	APISecret:     "test-secret",
	APIPassphrase: "test-pass",
	APIKeyVersion: "2",
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := testdata.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

// request is what the test server received, as it appeared on the wire.
type request struct {
	Method string
	Path   string // still percent-encoded
	Query  string // raw query string
	Header http.Header
}

type recorder struct {
	mu       sync.Mutex
	requests []request
}

func (r *recorder) record(req *http.Request) {
	path, query, _ := strings.Cut(req.RequestURI, "?")
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, request{Method: req.Method, Path: path, Query: query, Header: req.Header.Clone()})
}

func (r *recorder) all() []request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]request(nil), r.requests...)
}

func respond(status int, body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}
}

func okData(data string) []byte {
	return []byte(`{"code":"200000","data":` + data + `}`)
}

func newClient(t *testing.T, creds transport.Credentials, handler http.HandlerFunc) (*Client, *recorder) {
	t.Helper()
	rec := &recorder{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	executor := transport.NewExecutor(transport.ExecutorConfig{
		BaseURL:     server.URL,
		Credentials: creds,
		RetryPolicy: transport.NoRetry(),
	})
	return NewClient(executor), rec
}

// serve answers every request with HTTP 200 and body.
func serve(t *testing.T, body []byte) (*Client, *recorder) {
	t.Helper()
	return newClient(t, testCredentials, respond(http.StatusOK, body))
}

func serveFixture(t *testing.T, name string) (*Client, *recorder) {
	t.Helper()
	return serve(t, fixture(t, name))
}

func serveData(t *testing.T, data string) (*Client, *recorder) {
	t.Helper()
	return serve(t, okData(data))
}

// newForbiddenClient returns a client whose server fails the test if any
// request reaches it, for proving that validation happens locally.
func newForbiddenClient(t *testing.T) (*Client, *recorder) {
	t.Helper()
	return newClient(t, testCredentials, func(_ http.ResponseWriter, r *http.Request) {
		t.Errorf("a request reached the server although validation must fail locally: %s %s", r.Method, r.RequestURI)
	})
}

func assertUnsigned(t *testing.T, req request) {
	t.Helper()
	for name := range req.Header {
		if strings.HasPrefix(strings.ToUpper(name), "KC-API-") {
			t.Errorf("public request carries the auth header %s", name)
		}
	}
}

// expectRequest asserts that the server saw exactly one unsigned GET with the
// given percent-encoded path and raw query string.
func expectRequest(t *testing.T, rec *recorder, wantPath, wantQuery string) {
	t.Helper()
	reqs := rec.all()
	if len(reqs) != 1 {
		t.Fatalf("server saw %d requests, want exactly 1: %+v", len(reqs), reqs)
	}
	got := reqs[0]
	if got.Method != http.MethodGet {
		t.Errorf("method = %s, want GET", got.Method)
	}
	if got.Path != wantPath {
		t.Errorf("path = %q, want %q", got.Path, wantPath)
	}
	if got.Query != wantQuery {
		t.Errorf("query = %q, want %q", got.Query, wantQuery)
	}
	assertUnsigned(t, got)
}

// checkFields compares exported fields of the struct v with their expected
// text, so that every Decimal keeps its exact literal (for example "1.0E-4" or
// "84474.0") and every Int64 prints as its integer.
func checkFields(t *testing.T, v any, want map[string]string) {
	t.Helper()
	rv := reflect.ValueOf(v)
	names := make([]string, 0, len(want))
	for name := range want {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		field := rv.FieldByName(name)
		if !field.IsValid() {
			t.Errorf("%T has no field %s", v, name)
			continue
		}
		if got := fmt.Sprint(field.Interface()); got != want[name] {
			t.Errorf("%s = %q, want %q", name, got, want[name])
		}
	}
}

// assertStructEqual fails for every exported field of two values of the same
// struct type that differs, naming the field.
func assertStructEqual(t *testing.T, got, want any) {
	t.Helper()
	gv, wv := reflect.ValueOf(got), reflect.ValueOf(want)
	if gv.Type() != wv.Type() {
		t.Fatalf("type = %v, want %v", gv.Type(), wv.Type())
	}
	for i := 0; i < gv.NumField(); i++ {
		g, w := gv.Field(i).Interface(), wv.Field(i).Interface()
		if !reflect.DeepEqual(g, w) {
			t.Errorf("%s = %#v, want %#v", gv.Type().Field(i).Name, g, w)
		}
	}
}

func levels(t *testing.T, got []orderbook.Level, want ...orderbook.Level) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d levels, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("level %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// apiCall is one of the 17 methods invoked with valid arguments.
type apiCall struct {
	name string
	run  func(ctx context.Context, c *Client) error
}

func allCalls() []apiCall {
	return []apiCall{
		{"GetSymbol", func(ctx context.Context, c *Client) error { _, err := c.GetSymbol(ctx, "XBTUSDTM"); return err }},
		{"GetAllSymbols", func(ctx context.Context, c *Client) error { _, err := c.GetAllSymbols(ctx); return err }},
		{"GetTicker", func(ctx context.Context, c *Client) error { _, err := c.GetTicker(ctx, "XBTUSDTM"); return err }},
		{"GetAllTickers", func(ctx context.Context, c *Client) error { _, err := c.GetAllTickers(ctx); return err }},
		{"GetFullOrderBook", func(ctx context.Context, c *Client) error { _, err := c.GetFullOrderBook(ctx, "XBTUSDTM"); return err }},
		{"GetPartOrderBook", func(ctx context.Context, c *Client) error {
			_, err := c.GetPartOrderBook(ctx, "XBTUSDTM", 20)
			return err
		}},
		{"GetTradeHistory", func(ctx context.Context, c *Client) error { _, err := c.GetTradeHistory(ctx, "XBTUSDTM"); return err }},
		{"GetKlines", func(ctx context.Context, c *Client) error {
			_, err := c.GetKlines(ctx, KlineOptions{Symbol: "XBTUSDTM", Granularity: Granularity1Min})
			return err
		}},
		{"GetMarkPrice", func(ctx context.Context, c *Client) error { _, err := c.GetMarkPrice(ctx, "XBTUSDTM"); return err }},
		{"GetSpotIndexPrice", func(ctx context.Context, c *Client) error {
			_, err := c.GetSpotIndexPrice(ctx, IndexOptions{Symbol: ".KXBTUSDT"})
			return err
		}},
		{"GetInterestRateIndex", func(ctx context.Context, c *Client) error {
			_, err := c.GetInterestRateIndex(ctx, IndexOptions{Symbol: ".XBTINT8H"})
			return err
		}},
		{"GetPremiumIndex", func(ctx context.Context, c *Client) error {
			_, err := c.GetPremiumIndex(ctx, IndexOptions{Symbol: ".XBTUSDTMPI"})
			return err
		}},
		{"Get24hStats", func(ctx context.Context, c *Client) error { _, err := c.Get24hStats(ctx); return err }},
		{"GetServerTime", func(ctx context.Context, c *Client) error { _, err := c.GetServerTime(ctx); return err }},
		{"GetServiceStatus", func(ctx context.Context, c *Client) error { _, err := c.GetServiceStatus(ctx); return err }},
		{"GetCurrentFundingRate", func(ctx context.Context, c *Client) error {
			_, err := c.GetCurrentFundingRate(ctx, "XBTUSDTM")
			return err
		}},
		{"GetPublicFundingHistory", func(ctx context.Context, c *Client) error {
			_, err := c.GetPublicFundingHistory(ctx, FundingHistoryOptions{Symbol: "XBTUSDTM", From: 1790800000000, To: 1790990000000})
			return err
		}},
	}
}

func TestGetSymbol(t *testing.T) {
	client, rec := serveFixture(t, "contracts_XBTUSDTM.json")

	got, err := client.GetSymbol(context.Background(), "XBTUSDTM")
	if err != nil {
		t.Fatalf("GetSymbol: %v", err)
	}
	expectRequest(t, rec, "/api/v1/contracts/XBTUSDTM", "")

	// Every one of the 86 fields, with the exact text of the live capture.
	assertStructEqual(t, *got, Symbol{
		Symbol:                             "XBTUSDTM",
		DisplaySymbol:                      "XBTUSDTM",
		RootSymbol:                         "USDT",
		Type:                               ContractTypeFFWCSX,
		FirstOpenDate:                      1585555200000,
		ExpireDate:                         0, // null
		SettleDate:                         0, // null
		BaseCurrency:                       "XBT",
		DisplayBaseCurrency:                "XBT",
		QuoteCurrency:                      "USDT",
		SettleCurrency:                     "USDT",
		MaxOrderQty:                        1000000,
		MarketMaxOrderQty:                  "1000000",
		MaxPrice:                           "1000000.0",
		LotSize:                            1,
		TickSize:                           "0.1",
		IndexPriceTickSize:                 "0.01",
		Multiplier:                         "0.001",
		InitialMargin:                      "0.008",
		MaintainMargin:                     "0.004",
		MaxRiskLimit:                       "250000",
		MinRiskLimit:                       "250000",
		RiskStep:                           "125000",
		MakerFeeRate:                       "2.0E-4",
		TakerFeeRate:                       "6.0E-4",
		TakerFixFee:                        "0.0",
		MakerFixFee:                        "0.0",
		SettlementFee:                      "", // null
		IsDeleverage:                       true,
		IsQuanto:                           true,
		IsInverse:                          false,
		MarkMethod:                         "FairPrice",
		FairMethod:                         "FundingRate",
		FundingBaseSymbol:                  ".XBTINT8H",
		FundingQuoteSymbol:                 ".USDTINT8H",
		FundingRateSymbol:                  ".XBTUSDTMFPI8H",
		IndexSymbol:                        ".KXBTUSDT",
		SettlementSymbol:                   "",
		Status:                             ContractStatusOpen,
		FundingFeeRate:                     "8.1E-5",
		PredictedFundingFeeRate:            "", // null
		FundingRateGranularity:             28800000,
		EffectiveFundingRateCycleStartTime: 1750147200000,
		CurrentFundingRateGranularity:      28800000,
		FundingRateCap:                     "0.003",
		FundingRateFloor:                   "-0.003",
		Period:                             1,
		OpenInterest:                       "9622858", // a JSON string on the wire
		TurnoverOf24h:                      "6.845151044888E8",
		VolumeOf24h:                        "7991.204",
		MarkPrice:                          "84478.96",
		IndexPrice:                         "84526.25",
		LastTradePrice:                     "84477", // a bare integer on the wire
		NextFundingRateTime:                3196728,
		NextFundingRateDateTime:            1790985600000,
		MaxLeverage:                        125,
		SourceExchanges:                    []string{"okex", "binance", "kucoin", "bybit", "gateio"},
		PremiumsSymbol1M:                   ".XBTUSDTMPI",
		PremiumsSymbol8H:                   ".XBTUSDTMPI8H",
		FundingBaseSymbol1M:                ".XBTINT",
		FundingQuoteSymbol1M:               ".USDTINT",
		LowPrice:                           "83864.7",
		HighPrice:                          "87274.8",
		PriceChgPct:                        "-0.0022",
		PriceChg:                           "-189.9",
		K:                                  "490.0",
		M:                                  "300.0",
		F:                                  "1.3",
		MmrLimit:                           "0.3",
		MmrLevConstant:                     "125.0",
		SupportCross:                       true,
		BuyLimit:                           "88702.9",
		SellLimit:                          "80255",
		AdjustK:                            "",
		AdjustM:                            "",
		AdjustMmrLevConstant:               "",
		AdjustActiveTime:                   0,
		CrossRiskLimit:                     "1.5E9",
		MarketStage:                        MarketStageNormal,
		PreMarketToPerpDate:                0,
		OrderPriceRange:                    "0.05",
		MarketType:                         MarketTypeCrypto,
		AssetClass:                         AssetClassCrypto,
		SubMarketType:                      "",
		DailyInterestRate:                  "3.0E-4", // observed live, not in the docs' field table
		LastTimeFundingRate:                "3.4E-5", // observed live, not in the docs' field table
	})
}

func TestGetAllSymbols(t *testing.T) {
	client, rec := serveFixture(t, "contracts_active_trimmed.json")

	got, err := client.GetAllSymbols(context.Background())
	if err != nil {
		t.Fatalf("GetAllSymbols: %v", err)
	}
	expectRequest(t, rec, "/api/v1/contracts/active", "")

	wantOrder := []string{"XBTUSDTM", "XBTMZ26", "XBTUSDM", "PROMPTUSDTM", "BPUSDTM", "AAOIUSDTM", "BYDUSDTM", "YFIUSDTM", "DOGEUSDTM", "PAXGUSDTM", "SOLUSDCM"}
	if len(got) != len(wantOrder) {
		t.Fatalf("got %d contracts, want %d", len(got), len(wantOrder))
	}
	bySymbol := map[string]Symbol{}
	for i, s := range got {
		if s.Symbol != wantOrder[i] {
			t.Errorf("contract %d = %s, want %s", i, s.Symbol, wantOrder[i])
		}
		bySymbol[s.Symbol] = s
	}

	// The dated, coin-margined future: every funding field is null, and nothing
	// but the listed fields may be set.
	t.Run("dated future decodes nulls as zero values", func(t *testing.T) {
		assertStructEqual(t, bySymbol["XBTMZ26"], Symbol{
			Symbol: "XBTMZ26", DisplaySymbol: "XBTMZ26", RootSymbol: "XBT", Type: ContractTypeFFICSX,
			FirstOpenDate: 1790071200000, ExpireDate: 1798185600000, SettleDate: 1798185600000,
			BaseCurrency: "XBT", DisplayBaseCurrency: "XBT", QuoteCurrency: "USD", SettleCurrency: "XBT",
			MaxOrderQty: 100000, MarketMaxOrderQty: "100000", MaxPrice: "1000000.0", LotSize: 1,
			TickSize: "0.1", IndexPriceTickSize: "0.1", Multiplier: "-1.0",
			InitialMargin: "0.05", MaintainMargin: "0.025",
			MaxRiskLimit: "4", MinRiskLimit: "4", RiskStep: "2",
			MakerFeeRate: "2.0E-4", TakerFeeRate: "6.0E-4", TakerFixFee: "0.0", MakerFixFee: "0.0",
			IsDeleverage: true, IsQuanto: false, IsInverse: true,
			MarkMethod: "FairPrice", IndexSymbol: ".BXBT", SettlementSymbol: ".BXBT30M", Status: ContractStatusOpen,
			OpenInterest: "16356", TurnoverOf24h: "0.0", VolumeOf24h: "0.0", MarkPrice: "85438.2", IndexPrice: "84508.04",
			MaxLeverage:      20,
			SourceExchanges:  []string{"kraken", "bitstamp", "crypto", "coinbase", "binance"},
			PremiumsSymbol1M: ".XBTUSDMPI", PremiumsSymbol8H: ".XBTUSDMPI8H",
			LowPrice: "0.0", HighPrice: "0.0", PriceChgPct: "0", PriceChg: "0",
			K: "531041.0", M: "327200.0", F: "1.3", MmrLimit: "0.3", MmrLevConstant: "20.0", SupportCross: true,
			BuyLimit: "94005.7", SellLimit: "76913.7", CrossRiskLimit: "45.0",
			MarketStage: MarketStageNormal, OrderPriceRange: "0.1", MarketType: MarketTypeCrypto, AssetClass: AssetClassCrypto,
		})
	})

	spot := []struct {
		symbol string
		want   map[string]string
	}{
		{"XBTUSDM", map[string]string{
			"Multiplier": "-1.0", "IsInverse": "true", "SettlementSymbol": "", // null
			"SettleCurrency": "XBT", "RootSymbol": "XBT", "FundingRateCap": "0.00525", "K": "2645000.0", "VolumeOf24h": "4320446.0",
		}},
		{"PROMPTUSDTM", map[string]string{ // a perpetual swap with a scheduled expiry
			"Type": "FFWCSX", "ExpireDate": "1791183600000", "SettleDate": "1791183600000", "SettlementSymbol": ".KPROMPT30M",
			"TickSize": "1.0E-5", "PriceChg": "1.1E-4", "FundingRateGranularity": "14400000", "CurrentFundingRateGranularity": "14400000",
		}},
		{"BPUSDTM", map[string]string{
			"MarketStage": "PRE_MARKET", "PreMarketToPerpDate": "0", "MaxLeverage": "5", "SourceExchanges": "[bybit_mark_price]",
			"MarketMaxOrderQty": "10000", "MaxOrderQty": "10000",
		}},
		{"AAOIUSDTM", map[string]string{
			"MarketType": "NASDAQ", "AssetClass": "STOCK", "SubMarketType": "US.STOCK", "FundingFeeRate": "0.0", "DailyInterestRate": "0.0", "LastTimeFundingRate": "0.0",
		}},
		{"BYDUSDTM", map[string]string{
			"SubMarketType": "HK.STOCK", "FundingFeeRate": "-3.64E-4", "LastTimeFundingRate": "-1.47E-4", "Multiplier": "0.1",
		}},
		{"YFIUSDTM", map[string]string{ // prices that are bare integers on the wire
			"LowPrice": "2436", "HighPrice": "2581", "BuyLimit": "2720", "SellLimit": "2226", "LastTradePrice": "2472", "TickSize": "1.0", "Multiplier": "1.0E-4", "PriceChg": "20.0",
		}},
		{"DOGEUSDTM", map[string]string{ // two funding fields that are null on a perpetual
			"EffectiveFundingRateCycleStartTime": "0", "CurrentFundingRateGranularity": "0", "FundingRateGranularity": "28800000",
			"TurnoverOf24h": "2.9397181322E7", "VolumeOf24h": "3.106688E8", "K": "1.92E7", "CrossRiskLimit": "1.5E8",
		}},
		{"PAXGUSDTM", map[string]string{"AssetClass": "METAL", "MarketType": "CRYPTO", "SubMarketType": ""}},
		{"SOLUSDCM", map[string]string{
			"RootSymbol": "USDC", "SettleCurrency": "USDC", "QuoteCurrency": "USDC", "EffectiveFundingRateCycleStartTime": "0", "CrossRiskLimit": "1.2E7",
		}},
	}
	for _, tc := range spot {
		t.Run(tc.symbol, func(t *testing.T) {
			checkFields(t, bySymbol[tc.symbol], tc.want)
		})
	}
}

func TestGetTicker(t *testing.T) {
	client, rec := serveFixture(t, "ticker.json")

	got, err := client.GetTicker(context.Background(), "XBTUSDTM")
	if err != nil {
		t.Fatalf("GetTicker: %v", err)
	}
	expectRequest(t, rec, "/api/v1/ticker", "symbol=XBTUSDTM")

	checkFields(t, *got, map[string]string{
		"Sequence": "1748100435155", "Symbol": "XBTUSDTM", "Side": "buy", "Size": "3", "TradeID": "1945734806737",
		"Price": "84477", "BestBidPrice": "84476.9", "BestBidSize": "488", "BestAskPrice": "84477", "BestAskSize": "52",
		"Timestamp": "1790982404079000000",
	})
	if got.Side != SideBuy {
		t.Errorf("Side = %q, want %q", got.Side, SideBuy)
	}
	// A 19-digit nanosecond timestamp must not be rounded through a float64.
	if got.Timestamp.Value() != 1790982404079000000 || got.Time().UnixNano() != 1790982404079000000 {
		t.Errorf("timestamp lost precision: %d / %d", got.Timestamp.Value(), got.Time().UnixNano())
	}
}

func TestGetAllTickers(t *testing.T) {
	client, rec := serveFixture(t, "all_tickers_trimmed.json")

	got, err := client.GetAllTickers(context.Background())
	if err != nil {
		t.Fatalf("GetAllTickers: %v", err)
	}
	expectRequest(t, rec, "/api/v1/allTickers", "")

	if len(got) != 5 {
		t.Fatalf("got %d tickers, want 5", len(got))
	}
	checkFields(t, got[0], map[string]string{
		"Sequence": "1738647796318", "Symbol": "ANIMEUSDTM", "Side": "sell", "Size": "128", "TradeID": "1738713165337",
		"Price": "0.0033", "BestBidPrice": "0.003299", "BestBidSize": "1303", "BestAskPrice": "0.003304", "BestAskSize": "1314",
		"Timestamp": "1790982320766000000",
	})
	checkFields(t, got[3], map[string]string{
		"Symbol": "XBTUSDM", "Side": "buy", "Size": "100", "Price": "84485.4", "BestBidPrice": "84458.9", "BestBidSize": "1650",
		"BestAskPrice": "84473", "BestAskSize": "86", "Timestamp": "1790981543440000000",
	})
	for i, want := range []string{"ANIMEUSDTM", "MPUSDTM", "XBTUSDTM", "XBTUSDM", "SNXUSDTM"} {
		if got[i].Symbol != want {
			t.Errorf("ticker %d = %s, want %s", i, got[i].Symbol, want)
		}
	}
}

func TestGetFullOrderBook(t *testing.T) {
	client, rec := serveFixture(t, "level2_snapshot_trimmed.json")

	got, err := client.GetFullOrderBook(context.Background(), "XBTUSDTM")
	if err != nil {
		t.Fatalf("GetFullOrderBook: %v", err)
	}
	expectRequest(t, rec, "/api/v1/level2/snapshot", "symbol=XBTUSDTM")

	checkFields(t, *got, map[string]string{"Sequence": "1748100435452", "Symbol": "XBTUSDTM", "Timestamp": "1790982408137000000"})
	// Levels arrive as JSON numbers; a trailing ".0" survives.
	levels(t, got.Bids,
		orderbook.Level{Price: "84476.9", Size: "488"},
		orderbook.Level{Price: "84476.8", Size: "4"},
		orderbook.Level{Price: "84476.2", Size: "12"},
		orderbook.Level{Price: "84476.1", Size: "11"},
		orderbook.Level{Price: "84475.5", Size: "29"},
		orderbook.Level{Price: "84474.1", Size: "535"},
		orderbook.Level{Price: "84474.0", Size: "1468"},
		orderbook.Level{Price: "84473.3", Size: "64"},
	)
	levels(t, got.Asks,
		orderbook.Level{Price: "84477.0", Size: "30"},
		orderbook.Level{Price: "84477.5", Size: "35"},
		orderbook.Level{Price: "84478.4", Size: "29"},
		orderbook.Level{Price: "84479.7", Size: "38"},
		orderbook.Level{Price: "84483.1", Size: "80"},
		orderbook.Level{Price: "84483.2", Size: "19"},
		orderbook.Level{Price: "84483.4", Size: "81"},
		orderbook.Level{Price: "84484.4", Size: "7"},
	)
	if got.Time().UnixNano() != 1790982408137000000 {
		t.Errorf("Time().UnixNano() = %d", got.Time().UnixNano())
	}
}

func TestGetPartOrderBook(t *testing.T) {
	cases := []struct {
		depth                                int
		fixture, path                        string
		sequence, timestamp                  string
		firstBid, lastBid, firstAsk, lastAsk orderbook.Level
	}{
		{
			depth: 20, fixture: "level2_depth20.json", path: "/api/v1/level2/depth20",
			sequence: "1748100435563", timestamp: "1790982408793000000",
			firstBid: orderbook.Level{Price: "84476.9", Size: "488"}, lastBid: orderbook.Level{Price: "84467.4", Size: "82"},
			firstAsk: orderbook.Level{Price: "84477.0", Size: "38"}, lastAsk: orderbook.Level{Price: "84490.0", Size: "167"},
		},
		{
			depth: 100, fixture: "level2_depth100.json", path: "/api/v1/level2/depth100",
			sequence: "1748100435662", timestamp: "1790982409612000000",
			firstBid: orderbook.Level{Price: "84476.9", Size: "568"}, lastBid: orderbook.Level{Price: "84409.4", Size: "440"},
			firstAsk: orderbook.Level{Price: "84477.0", Size: "81"}, lastAsk: orderbook.Level{Price: "84541.0", Size: "1954"},
		},
	}
	for _, tc := range cases {
		t.Run(strconv.Itoa(tc.depth), func(t *testing.T) {
			client, rec := serveFixture(t, tc.fixture)

			got, err := client.GetPartOrderBook(context.Background(), "XBTUSDTM", tc.depth)
			if err != nil {
				t.Fatalf("GetPartOrderBook: %v", err)
			}
			expectRequest(t, rec, tc.path, "symbol=XBTUSDTM")

			checkFields(t, *got, map[string]string{"Sequence": tc.sequence, "Symbol": "XBTUSDTM", "Timestamp": tc.timestamp})
			if len(got.Bids) != tc.depth || len(got.Asks) != tc.depth {
				t.Fatalf("levels = %d bids / %d asks, want %d each", len(got.Bids), len(got.Asks), tc.depth)
			}
			for name, pair := range map[string][2]orderbook.Level{
				"first bid": {got.Bids[0], tc.firstBid}, "last bid": {got.Bids[tc.depth-1], tc.lastBid},
				"first ask": {got.Asks[0], tc.firstAsk}, "last ask": {got.Asks[tc.depth-1], tc.lastAsk},
			} {
				if pair[0] != pair[1] {
					t.Errorf("%s = %+v, want %+v", name, pair[0], pair[1])
				}
			}
		})
	}
}

func TestGetPartOrderBook_InvalidDepth(t *testing.T) {
	client, rec := newForbiddenClient(t)

	for _, depth := range []int{0, -20, 1, 19, 21, 50, 99, 101, 200, 1000} {
		_, err := client.GetPartOrderBook(context.Background(), "XBTUSDTM", depth)
		if !errors.Is(err, ErrInvalidDepth) {
			t.Errorf("depth %d: error = %v, want ErrInvalidDepth", depth, err)
			continue
		}
		if want := "got " + strconv.Itoa(depth); !strings.Contains(err.Error(), want) {
			t.Errorf("depth %d: error %q does not mention %q", depth, err, want)
		}
	}
	if n := len(rec.all()); n != 0 {
		t.Errorf("%d requests reached the server", n)
	}
}

func TestGetTradeHistory(t *testing.T) {
	client, rec := serveFixture(t, "trade_history_trimmed.json")

	got, err := client.GetTradeHistory(context.Background(), "XBTUSDTM")
	if err != nil {
		t.Fatalf("GetTradeHistory: %v", err)
	}
	expectRequest(t, rec, "/api/v1/trade/history", "symbol=XBTUSDTM")

	if len(got) != 5 {
		t.Fatalf("got %d trades, want 5", len(got))
	}
	checkFields(t, got[0], map[string]string{
		"Sequence": "1945734807555", "ContractID": "101", "TradeID": "1945734807555",
		"MakerOrderID": "495637522475470849", "TakerOrderID": "495637567627091968",
		"Size": "6", "Price": "84477", "Side": "buy", "Timestamp": "1790982406625000000",
	})
	checkFields(t, got[2], map[string]string{
		"Sequence": "1945734806737", "MakerOrderID": "495637437821755392", "TakerOrderID": "495637556948500480",
		"Size": "3", "Timestamp": "1790982404079000000",
	})
	// 18-digit order IDs and 19-digit timestamps survive exactly.
	if got[0].Time().UnixNano() != 1790982406625000000 {
		t.Errorf("Time().UnixNano() = %d", got[0].Time().UnixNano())
	}
}

func TestGetKlines(t *testing.T) {
	t.Run("minimal request omits from and to", func(t *testing.T) {
		client, rec := serveFixture(t, "kline.json")

		got, err := client.GetKlines(context.Background(), KlineOptions{Symbol: "XBTUSDTM", Granularity: Granularity1Min})
		if err != nil {
			t.Fatalf("GetKlines: %v", err)
		}
		expectRequest(t, rec, "/api/v1/kline/query", "granularity=1&symbol=XBTUSDTM")

		if len(got) != 5 {
			t.Fatalf("got %d candles, want 5", len(got))
		}
		want := []Kline{
			{Time: 1790982120000, Open: "84483.5", High: "84499.4", Low: "84483.5", Close: "84499.4", Volume: "322", Turnover: "27205.9061"},
			{Time: 1790982180000, Open: "84499.4", High: "84499.4", Low: "84499.3", Close: "84499.4", Volume: "106", Turnover: "8956.936"},
			{Time: 1790982240000, Open: "84499.4", High: "84500.4", Low: "84499.4", Close: "84500.4", Volume: "165", Turnover: "13942.507"},
			{Time: 1790982300000, Open: "84500.4", High: "84501.4", Low: "84477.0", Close: "84477.0", Volume: "142", Turnover: "11998.4579"},
			{Time: 1790982360000, Open: "84477.0", High: "84477.0", Low: "84476.9", Close: "84477.0", Volume: "253", Turnover: "21372.6659"},
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("candle %d = %+v, want %+v", i, got[i], want[i])
			}
		}
		if got[0].Timestamp().UnixMilli() != 1790982120000 {
			t.Errorf("Timestamp().UnixMilli() = %d", got[0].Timestamp().UnixMilli())
		}
	})

	t.Run("from and to are sent in milliseconds", func(t *testing.T) {
		client, rec := serveFixture(t, "kline.json")

		_, err := client.GetKlines(context.Background(), KlineOptions{
			Symbol: ".KXBTUSDT", Granularity: Granularity1Hour, From: 1790900000000, To: 1790990000000,
		})
		if err != nil {
			t.Fatalf("GetKlines: %v", err)
		}
		expectRequest(t, rec, "/api/v1/kline/query", "from=1790900000000&granularity=60&symbol=.KXBTUSDT&to=1790990000000")
	})

	t.Run("only from", func(t *testing.T) {
		client, rec := serveFixture(t, "kline.json")
		if _, err := client.GetKlines(context.Background(), KlineOptions{Symbol: "XBTUSDTM", Granularity: Granularity1Day, From: 1790900000000}); err != nil {
			t.Fatalf("GetKlines: %v", err)
		}
		expectRequest(t, rec, "/api/v1/kline/query", "from=1790900000000&granularity=1440&symbol=XBTUSDTM")
	})

	t.Run("only to", func(t *testing.T) {
		client, rec := serveFixture(t, "kline.json")
		if _, err := client.GetKlines(context.Background(), KlineOptions{Symbol: "XBTUSDTM", Granularity: Granularity1Week, To: 1790990000000}); err != nil {
			t.Fatalf("GetKlines: %v", err)
		}
		expectRequest(t, rec, "/api/v1/kline/query", "granularity=10080&symbol=XBTUSDTM&to=1790990000000")
	})
}

func TestGetKlines_Validation(t *testing.T) {
	t.Run("missing symbol", func(t *testing.T) {
		client, rec := newForbiddenClient(t)
		_, err := client.GetKlines(context.Background(), KlineOptions{Granularity: Granularity1Min})
		if !errors.Is(err, ErrSymbolRequired) {
			t.Fatalf("error = %v, want ErrSymbolRequired", err)
		}
		if n := len(rec.all()); n != 0 {
			t.Errorf("%d requests reached the server", n)
		}
	})

	t.Run("unsupported granularity", func(t *testing.T) {
		client, rec := newForbiddenClient(t)
		for _, g := range []Granularity{0, -1, 2, 4, 10, 59, 61, 100, 1000, 1441, 10079, 10081, 43199, 43201} {
			_, err := client.GetKlines(context.Background(), KlineOptions{Symbol: "XBTUSDTM", Granularity: g})
			if !errors.Is(err, ErrInvalidGranularity) {
				t.Errorf("granularity %d: error = %v, want ErrInvalidGranularity", g, err)
			}
		}
		if n := len(rec.all()); n != 0 {
			t.Errorf("%d requests reached the server", n)
		}
	})

	t.Run("every supported granularity is sent as its minute count", func(t *testing.T) {
		for _, g := range []Granularity{
			Granularity1Min, Granularity3Min, Granularity5Min, Granularity15Min, Granularity30Min, Granularity1Hour, Granularity2Hour,
			Granularity4Hour, Granularity8Hour, Granularity12Hour, Granularity1Day, Granularity1Week, Granularity1Month,
		} {
			client, rec := serveData(t, `[]`)
			if _, err := client.GetKlines(context.Background(), KlineOptions{Symbol: "XBTUSDTM", Granularity: g}); err != nil {
				t.Fatalf("granularity %v: %v", g, err)
			}
			expectRequest(t, rec, "/api/v1/kline/query", fmt.Sprintf("granularity=%d&symbol=XBTUSDTM", int(g)))
		}
	})
}

func TestGetMarkPrice(t *testing.T) {
	client, rec := serveFixture(t, "mark_price.json")

	got, err := client.GetMarkPrice(context.Background(), "XBTUSDTM")
	if err != nil {
		t.Fatalf("GetMarkPrice: %v", err)
	}
	expectRequest(t, rec, "/api/v1/mark-price/XBTUSDTM/current", "")

	checkFields(t, *got, map[string]string{
		"Symbol": "XBTUSDTM", "Granularity": "1000", "TimePoint": "1790982411000", "Value": "84478.75", "IndexPrice": "84526.22",
	})
	if got.Time().UnixMilli() != 1790982411000 {
		t.Errorf("Time().UnixMilli() = %d", got.Time().UnixMilli())
	}
}

func TestGetSpotIndexPrice(t *testing.T) {
	client, rec := serveFixture(t, "index_query.json")

	got, err := client.GetSpotIndexPrice(context.Background(), IndexOptions{Symbol: ".KXBTUSDT"})
	if err != nil {
		t.Fatalf("GetSpotIndexPrice: %v", err)
	}
	expectRequest(t, rec, "/api/v1/index/query", "symbol=.KXBTUSDT")

	if !got.HasMore || len(got.DataList) != 3 {
		t.Fatalf("page = hasMore %v, %d points; want true, 3", got.HasMore, len(got.DataList))
	}
	first := got.DataList[0]
	checkFields(t, first, map[string]string{"Symbol": ".KXBTUSDT", "Granularity": "1000", "TimePoint": "1790982412000", "Value": "84526.22"})
	if first.Time().UnixMilli() != 1790982412000 {
		t.Errorf("Time().UnixMilli() = %d", first.Time().UnixMilli())
	}
	want := []IndexComponent{
		{Exchange: "okex", ExchangeName: "OKX", Price: "84526.9", Weight: "0.3293"},
		{Exchange: "binance", ExchangeName: "Binance", Price: "84528.0", Weight: "0.4115"},
		{Exchange: "kucoin", ExchangeName: "KuCoin", Price: "84525.6", Weight: "0.0864"},
		{Exchange: "bybit", ExchangeName: "Bybit", Price: "84517.3", Weight: "0.0864"},
		{Exchange: "gateio", ExchangeName: "GateIO", Price: "84524.7", Weight: "0.0864"},
	}
	if !reflect.DeepEqual(first.DecompositionList, want) {
		t.Errorf("DecompositionList = %+v, want %+v", first.DecompositionList, want)
	}
	for i, p := range got.DataList {
		if len(p.DecompositionList) != 5 {
			t.Errorf("point %d has %d components, want 5", i, len(p.DecompositionList))
		}
	}
}

func TestGetInterestRateIndex(t *testing.T) {
	client, rec := serveFixture(t, "interest_query.json")

	got, err := client.GetInterestRateIndex(context.Background(), IndexOptions{Symbol: ".XBTINT8H"})
	if err != nil {
		t.Fatalf("GetInterestRateIndex: %v", err)
	}
	expectRequest(t, rec, "/api/v1/interest/query", "symbol=.XBTINT8H")

	if !got.HasMore || len(got.DataList) != 3 {
		t.Fatalf("page = hasMore %v, %d points; want true, 3", got.HasMore, len(got.DataList))
	}
	for i, wantTime := range []string{"1790956800000", "1790928000000", "1790899200000"} {
		checkFields(t, got.DataList[i], map[string]string{
			"Symbol": ".XBTINT8H", "Granularity": "28800000", "TimePoint": wantTime, "Value": "3.0E-4",
		})
	}
	if got.DataList[0].Time().UnixMilli() != 1790956800000 {
		t.Errorf("Time().UnixMilli() = %d", got.DataList[0].Time().UnixMilli())
	}
}

func TestGetPremiumIndex(t *testing.T) {
	client, rec := serveFixture(t, "premium_query.json")

	got, err := client.GetPremiumIndex(context.Background(), IndexOptions{Symbol: ".XBTUSDTMPI"})
	if err != nil {
		t.Fatalf("GetPremiumIndex: %v", err)
	}
	expectRequest(t, rec, "/api/v1/premium/query", "symbol=.XBTUSDTMPI")

	if !got.HasMore || len(got.DataList) != 3 {
		t.Fatalf("page = hasMore %v, %d points; want true, 3", got.HasMore, len(got.DataList))
	}
	for i, want := range []struct{ timePoint, value string }{
		{"1790982360000", "-5.82E-4"}, {"1790982300000", "-5.36E-4"}, {"1790982240000", "-5.47E-4"},
	} {
		checkFields(t, got.DataList[i], map[string]string{
			"Symbol": ".XBTUSDTMPI", "Granularity": "60000", "TimePoint": want.timePoint, "Value": want.value,
		})
	}
	if got.DataList[0].Time().UnixMilli() != 1790982360000 {
		t.Errorf("Time().UnixMilli() = %d", got.DataList[0].Time().UnixMilli())
	}
}

// indexMethods are the three paged endpoints that share IndexOptions.
var indexMethods = []struct {
	name, path string
	call       func(ctx context.Context, c *Client, opts IndexOptions) error
}{
	{"GetSpotIndexPrice", "/api/v1/index/query", func(ctx context.Context, c *Client, opts IndexOptions) error {
		_, err := c.GetSpotIndexPrice(ctx, opts)
		return err
	}},
	{"GetInterestRateIndex", "/api/v1/interest/query", func(ctx context.Context, c *Client, opts IndexOptions) error {
		_, err := c.GetInterestRateIndex(ctx, opts)
		return err
	}},
	{"GetPremiumIndex", "/api/v1/premium/query", func(ctx context.Context, c *Client, opts IndexOptions) error {
		_, err := c.GetPremiumIndex(ctx, opts)
		return err
	}},
}

func TestIndexEndpoints_QueryParameters(t *testing.T) {
	yes, no := true, false
	cases := []struct {
		name  string
		opts  IndexOptions
		query string
	}{
		{"symbol only", IndexOptions{Symbol: ".KXBTUSDT"}, "symbol=.KXBTUSDT"},
		{"explicit zero values are omitted", IndexOptions{Symbol: ".KXBTUSDT", StartAt: 0, EndAt: 0, Offset: 0, MaxCount: 0}, "symbol=.KXBTUSDT"},
		{
			"every option",
			IndexOptions{Symbol: ".KXBTUSDT", StartAt: 1790900000000, EndAt: 1790990000000, Reverse: &no, Forward: &yes, Offset: 1790982900000, MaxCount: 100},
			"endAt=1790990000000&forward=true&maxCount=100&offset=1790982900000&reverse=false&startAt=1790900000000&symbol=.KXBTUSDT",
		},
		{"reverse true, forward false", IndexOptions{Symbol: "S", Reverse: &yes, Forward: &no}, "forward=false&reverse=true&symbol=S"},
		{"false flags are sent, not dropped", IndexOptions{Symbol: "S", Reverse: &no, Forward: &no}, "forward=false&reverse=false&symbol=S"},
		{"max count 1", IndexOptions{Symbol: "S", MaxCount: 1}, "maxCount=1&symbol=S"},
		{"time range only", IndexOptions{Symbol: "S", StartAt: 5, EndAt: 9}, "endAt=9&startAt=5&symbol=S"},
	}
	for _, m := range indexMethods {
		for _, tc := range cases {
			t.Run(m.name+"/"+tc.name, func(t *testing.T) {
				client, rec := serveData(t, `{"dataList":[],"hasMore":false}`)
				if err := m.call(context.Background(), client, tc.opts); err != nil {
					t.Fatalf("%s: %v", m.name, err)
				}
				expectRequest(t, rec, m.path, tc.query)
			})
		}
	}
}

func TestIndexEndpoints_Validation(t *testing.T) {
	for _, m := range indexMethods {
		t.Run(m.name, func(t *testing.T) {
			client, rec := newForbiddenClient(t)

			if err := m.call(context.Background(), client, IndexOptions{}); !errors.Is(err, ErrSymbolRequired) {
				t.Errorf("missing symbol: error = %v, want ErrSymbolRequired", err)
			}
			for _, count := range []int{101, 150, 1000, -1, -100} {
				err := m.call(context.Background(), client, IndexOptions{Symbol: ".KXBTUSDT", MaxCount: count})
				if !errors.Is(err, ErrInvalidMaxCount) {
					t.Errorf("MaxCount %d: error = %v, want ErrInvalidMaxCount", count, err)
				}
			}
			if n := len(rec.all()); n != 0 {
				t.Errorf("%d requests reached the server", n)
			}
		})
	}
}

func TestGet24hStats_SignsWhenCredentialsAreConfigured(t *testing.T) {
	client, rec := newClient(t, testCredentials, respond(http.StatusOK, okData(`{"turnoverOf24h":1.1155733413273683E9}`)))

	got, err := client.Get24hStats(context.Background())
	if err != nil {
		t.Fatalf("Get24hStats: %v", err)
	}

	reqs := rec.all()
	if len(reqs) != 1 {
		t.Fatalf("server saw %d requests, want 1", len(reqs))
	}
	req := reqs[0]
	if req.Method != http.MethodGet || req.Path != "/api/v1/trade-statistics" || req.Query != "" {
		t.Errorf("request = %s %s?%s, want GET /api/v1/trade-statistics", req.Method, req.Path, req.Query)
	}
	if got := req.Header.Get("KC-API-KEY"); got != "test-key" {
		t.Errorf("KC-API-KEY = %q, want test-key", got)
	}
	if got := req.Header.Get("KC-API-KEY-VERSION"); got != "2" {
		t.Errorf("KC-API-KEY-VERSION = %q, want 2", got)
	}
	for _, name := range []string{"KC-API-SIGN", "KC-API-TIMESTAMP", "KC-API-PASSPHRASE"} {
		if req.Header.Get(name) == "" {
			t.Errorf("%s header is missing", name)
		}
	}
	if req.Header.Get("KC-API-PASSPHRASE") == "test-pass" {
		t.Error("the passphrase was sent in clear text")
	}
	// The docs' example value, in Java exponent form.
	if got.TurnoverOf24h != "1.1155733413273683E9" {
		t.Errorf("TurnoverOf24h = %q, want 1.1155733413273683E9", got.TurnoverOf24h)
	}
}

func TestGet24hStats_UnsignedWithoutCredentialsReturnsKucoinsError(t *testing.T) {
	// The live API answers an unsigned call with HTTP 400 and this body even
	// though the docs call the endpoint public.
	client, rec := newClient(t, transport.Credentials{}, respond(http.StatusBadRequest, fixture(t, "trade_statistics_unauth.json")))

	got, err := client.Get24hStats(context.Background())
	if err == nil {
		t.Fatal("Get24hStats succeeded, want KuCoin's rejection")
	}
	if got != nil {
		t.Errorf("result = %+v, want nil", got)
	}
	if errors.Is(err, transport.ErrCredentialsRequired) {
		t.Error("error is the local ErrCredentialsRequired; the request must be sent so KuCoin's own error is returned")
	}
	if !errors.Is(err, transport.ErrBadRequest) {
		t.Errorf("error %v does not match transport.ErrBadRequest", err)
	}
	var kerr *transport.KucoinError
	if !errors.As(err, &kerr) {
		t.Fatalf("error %v does not carry a *transport.KucoinError", err)
	}
	if kerr.HTTPStatus != http.StatusBadRequest || kerr.Code != "400001" || !strings.HasPrefix(kerr.Message, "Please check the header of your request for KC-API-KEY") {
		t.Errorf("KucoinError = %+v", kerr)
	}
	expectRequest(t, rec, "/api/v1/trade-statistics", "")
}

func TestGet24hStats_IncompleteCredentialsAreNotSigned(t *testing.T) {
	// Only an API key: the executor does not treat this as usable credentials,
	// so the request goes out unsigned rather than half-signed.
	client, rec := newClient(t, transport.Credentials{APIKey: "only-a-key"}, respond(http.StatusOK, okData(`{"turnoverOf24h":"5.5E8"}`)))

	got, err := client.Get24hStats(context.Background())
	if err != nil {
		t.Fatalf("Get24hStats: %v", err)
	}
	expectRequest(t, rec, "/api/v1/trade-statistics", "")
	// This is also what happens once KuCoin serves the endpoint publicly, as it
	// documents: the unsigned call then simply succeeds.
	if got.TurnoverOf24h != "5.5E8" {
		t.Errorf("TurnoverOf24h = %q, want 5.5E8", got.TurnoverOf24h)
	}
}

func TestGetServerTime(t *testing.T) {
	t.Run("live capture", func(t *testing.T) {
		client, rec := serveFixture(t, "timestamp.json")

		got, err := client.GetServerTime(context.Background())
		if err != nil {
			t.Fatalf("GetServerTime: %v", err)
		}
		expectRequest(t, rec, "/api/v1/timestamp", "")
		if got != 1790982415541 {
			t.Errorf("server time = %d, want 1790982415541", got)
		}
	})

	t.Run("numeric string", func(t *testing.T) {
		client, _ := serveData(t, `"1790982415541"`)
		got, err := client.GetServerTime(context.Background())
		if err != nil || got != 1790982415541 {
			t.Errorf("GetServerTime = %d, %v; want 1790982415541, nil", got, err)
		}
	})
}

func TestGetServiceStatus(t *testing.T) {
	t.Run("open", func(t *testing.T) {
		client, rec := serveFixture(t, "status.json")

		got, err := client.GetServiceStatus(context.Background())
		if err != nil {
			t.Fatalf("GetServiceStatus: %v", err)
		}
		expectRequest(t, rec, "/api/v1/status", "")
		if got.Status != ServiceStateOpen || got.Message != "" {
			t.Errorf("status = %+v, want open with an empty message", got)
		}
	})

	t.Run("cancel only with a message", func(t *testing.T) {
		client, _ := serveData(t, `{"msg":"matching engine upgrade","status":"cancelonly"}`)
		got, err := client.GetServiceStatus(context.Background())
		if err != nil {
			t.Fatalf("GetServiceStatus: %v", err)
		}
		if got.Status != ServiceStateCancelOnly || got.Message != "matching engine upgrade" {
			t.Errorf("status = %+v", got)
		}
	})
}

func TestGetCurrentFundingRate(t *testing.T) {
	t.Run("live capture", func(t *testing.T) {
		client, rec := serveFixture(t, "funding_current.json")

		got, err := client.GetCurrentFundingRate(context.Background(), "XBTUSDTM")
		if err != nil {
			t.Fatalf("GetCurrentFundingRate: %v", err)
		}
		expectRequest(t, rec, "/api/v1/funding-rate/XBTUSDTM/current", "")

		assertStructEqual(t, *got, FundingRate{
			Symbol:              ".XBTUSDTMFPI8H",
			Granularity:         28800000,
			TimePoint:           1790956800000,
			Value:               "8.1E-5",
			PredictedValue:      "", // documented, but not returned live
			DailyInterestRate:   "3.0E-4",
			FundingRateCap:      "0.003",
			FundingRateFloor:    "-0.003",
			Period:              1,
			FundingTime:         1790985600000,
			LastTimeFundingRate: "3.4E-5",
		})
		if got.Time().UnixMilli() != 1790956800000 || got.NextFundingTime().UnixMilli() != 1790985600000 {
			t.Errorf("Time() / NextFundingTime() = %v / %v", got.Time(), got.NextFundingTime())
		}
	})

	t.Run("documented example with predictedValue", func(t *testing.T) {
		client, rec := serveData(t, `{"symbol":".XBTUSDTMFPI8H","granularity":28800000,"timePoint":1748462400000,"value":6.1E-5,"predictedValue":1.09E-4,"fundingRateCap":0.003,"fundingRateFloor":-0.003,"period":0,"fundingTime":1748491200000}`)

		got, err := client.GetCurrentFundingRate(context.Background(), ".XBTUSDTMFPI8H")
		if err != nil {
			t.Fatalf("GetCurrentFundingRate: %v", err)
		}
		// A funding-rate symbol works as well, and its dots are not escaped.
		expectRequest(t, rec, "/api/v1/funding-rate/.XBTUSDTMFPI8H/current", "")
		checkFields(t, *got, map[string]string{
			"Value": "6.1E-5", "PredictedValue": "1.09E-4", "Period": "0", "DailyInterestRate": "", "LastTimeFundingRate": "",
		})
	})
}

func TestGetPublicFundingHistory(t *testing.T) {
	client, rec := serveFixture(t, "funding_history.json")

	got, err := client.GetPublicFundingHistory(context.Background(), FundingHistoryOptions{
		Symbol: "XBTUSDTM", From: 1790800000000, To: 1790990000000,
	})
	if err != nil {
		t.Fatalf("GetPublicFundingHistory: %v", err)
	}
	expectRequest(t, rec, "/api/v1/contract/funding-rates", "from=1790800000000&symbol=XBTUSDTM&to=1790990000000")

	want := []FundingRatePoint{
		{Symbol: "XBTUSDTM", FundingRate: "3.4E-5", TimePoint: 1790956800000},
		{Symbol: "XBTUSDTM", FundingRate: "1.0E-4", TimePoint: 1790928000000},
		{Symbol: "XBTUSDTM", FundingRate: "-3.4E-5", TimePoint: 1790899200000},
		{Symbol: "XBTUSDTM", FundingRate: "4.2E-5", TimePoint: 1790870400000},
		{Symbol: "XBTUSDTM", FundingRate: "3.0E-5", TimePoint: 1790841600000},
		{Symbol: "XBTUSDTM", FundingRate: "7.2E-5", TimePoint: 1790812800000},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("history = %+v, want %+v", got, want)
	}
	if got[0].Time().UnixMilli() != 1790956800000 {
		t.Errorf("Time().UnixMilli() = %d", got[0].Time().UnixMilli())
	}
}

func TestGetPublicFundingHistory_Validation(t *testing.T) {
	client, rec := newForbiddenClient(t)

	if _, err := client.GetPublicFundingHistory(context.Background(), FundingHistoryOptions{From: 1, To: 2}); !errors.Is(err, ErrSymbolRequired) {
		t.Errorf("missing symbol: error = %v, want ErrSymbolRequired", err)
	}
	for _, tc := range []struct {
		name     string
		from, to int64
	}{
		{"missing from", 0, 1790990000000},
		{"missing to", 1790800000000, 0},
		{"both missing", 0, 0},
		{"negative from", -1, 1790990000000},
		{"negative to", 1790800000000, -5},
		{"from after to", 1790990000000, 1790800000000},
	} {
		_, err := client.GetPublicFundingHistory(context.Background(), FundingHistoryOptions{Symbol: "XBTUSDTM", From: tc.from, To: tc.to})
		if !errors.Is(err, ErrInvalidRange) {
			t.Errorf("%s: error = %v, want ErrInvalidRange", tc.name, err)
		}
	}
	if n := len(rec.all()); n != 0 {
		t.Errorf("%d requests reached the server", n)
	}

	// A zero-width window is a valid range.
	okClient, okRec := serveData(t, `[]`)
	if _, err := okClient.GetPublicFundingHistory(context.Background(), FundingHistoryOptions{Symbol: "XBTUSDTM", From: 5, To: 5}); err != nil {
		t.Fatalf("from == to: %v", err)
	}
	expectRequest(t, okRec, "/api/v1/contract/funding-rates", "from=5&symbol=XBTUSDTM&to=5")
}

func TestSymbolIsRequired(t *testing.T) {
	client, rec := newForbiddenClient(t)
	ctx := context.Background()

	calls := []apiCall{
		{"GetSymbol", func(ctx context.Context, c *Client) error { _, err := c.GetSymbol(ctx, ""); return err }},
		{"GetTicker", func(ctx context.Context, c *Client) error { _, err := c.GetTicker(ctx, ""); return err }},
		{"GetFullOrderBook", func(ctx context.Context, c *Client) error { _, err := c.GetFullOrderBook(ctx, ""); return err }},
		{"GetPartOrderBook", func(ctx context.Context, c *Client) error { _, err := c.GetPartOrderBook(ctx, "", 20); return err }},
		{"GetTradeHistory", func(ctx context.Context, c *Client) error { _, err := c.GetTradeHistory(ctx, ""); return err }},
		{"GetKlines", func(ctx context.Context, c *Client) error {
			_, err := c.GetKlines(ctx, KlineOptions{Granularity: Granularity1Min})
			return err
		}},
		{"GetMarkPrice", func(ctx context.Context, c *Client) error { _, err := c.GetMarkPrice(ctx, ""); return err }},
		{"GetSpotIndexPrice", func(ctx context.Context, c *Client) error {
			_, err := c.GetSpotIndexPrice(ctx, IndexOptions{})
			return err
		}},
		{"GetInterestRateIndex", func(ctx context.Context, c *Client) error {
			_, err := c.GetInterestRateIndex(ctx, IndexOptions{})
			return err
		}},
		{"GetPremiumIndex", func(ctx context.Context, c *Client) error {
			_, err := c.GetPremiumIndex(ctx, IndexOptions{})
			return err
		}},
		{"GetCurrentFundingRate", func(ctx context.Context, c *Client) error { _, err := c.GetCurrentFundingRate(ctx, ""); return err }},
		{"GetPublicFundingHistory", func(ctx context.Context, c *Client) error {
			_, err := c.GetPublicFundingHistory(ctx, FundingHistoryOptions{From: 1, To: 2})
			return err
		}},
	}
	for _, c := range calls {
		if err := c.run(ctx, client); !errors.Is(err, ErrSymbolRequired) {
			t.Errorf("%s: error = %v, want ErrSymbolRequired", c.name, err)
		}
	}
	if n := len(rec.all()); n != 0 {
		t.Errorf("%d requests reached the server", n)
	}
}

func TestPathParameters_AreEscaped(t *testing.T) {
	const symbol = "XBT/USDT M"
	cases := []struct {
		name     string
		call     func(c *Client) error
		wantPath string
	}{
		{"GetSymbol", func(c *Client) error { _, err := c.GetSymbol(context.Background(), symbol); return err }, "/api/v1/contracts/XBT%2FUSDT%20M"},
		{"GetMarkPrice", func(c *Client) error { _, err := c.GetMarkPrice(context.Background(), symbol); return err }, "/api/v1/mark-price/XBT%2FUSDT%20M/current"},
		{"GetCurrentFundingRate", func(c *Client) error { _, err := c.GetCurrentFundingRate(context.Background(), symbol); return err }, "/api/v1/funding-rate/XBT%2FUSDT%20M/current"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, rec := serveData(t, `null`)
			if err := tc.call(client); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			expectRequest(t, rec, tc.wantPath, "")
		})
	}
}

func TestQueryParameters_AreEscaped(t *testing.T) {
	const symbol = "A B&C=D"
	const escaped = "A+B%26C%3DD"
	cases := []struct {
		name      string
		call      func(c *Client) error
		wantPath  string
		wantQuery string
	}{
		{"GetTicker", func(c *Client) error { _, err := c.GetTicker(context.Background(), symbol); return err }, "/api/v1/ticker", "symbol=" + escaped},
		{"GetFullOrderBook", func(c *Client) error { _, err := c.GetFullOrderBook(context.Background(), symbol); return err }, "/api/v1/level2/snapshot", "symbol=" + escaped},
		{"GetPartOrderBook", func(c *Client) error { _, err := c.GetPartOrderBook(context.Background(), symbol, 100); return err }, "/api/v1/level2/depth100", "symbol=" + escaped},
		{"GetTradeHistory", func(c *Client) error { _, err := c.GetTradeHistory(context.Background(), symbol); return err }, "/api/v1/trade/history", "symbol=" + escaped},
		{"GetKlines", func(c *Client) error {
			_, err := c.GetKlines(context.Background(), KlineOptions{Symbol: symbol, Granularity: Granularity5Min})
			return err
		}, "/api/v1/kline/query", "granularity=5&symbol=" + escaped},
		{"GetPublicFundingHistory", func(c *Client) error {
			_, err := c.GetPublicFundingHistory(context.Background(), FundingHistoryOptions{Symbol: symbol, From: 1, To: 2})
			return err
		}, "/api/v1/contract/funding-rates", "from=1&symbol=" + escaped + "&to=2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, rec := serveData(t, `null`)
			if err := tc.call(client); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			expectRequest(t, rec, tc.wantPath, tc.wantQuery)
		})
	}
}

func TestBusinessError_ReachesCallerAsKucoinError(t *testing.T) {
	// HTTP 200 with a business code other than 200000: the shape the live API
	// uses for an unknown symbol.
	client, rec := newClient(t, testCredentials, respond(http.StatusOK, []byte(`{"msg":"Invalid symbol.","code":"200003"}`)))

	got, err := client.GetTicker(context.Background(), "NOPE")
	if err == nil {
		t.Fatal("GetTicker succeeded, want KuCoin's business error")
	}
	if got != nil {
		t.Errorf("result = %+v, want nil", got)
	}
	var kerr *transport.KucoinError
	if !errors.As(err, &kerr) {
		t.Fatalf("error %v does not carry a *transport.KucoinError", err)
	}
	if kerr.Code != "200003" || kerr.Message != "Invalid symbol." || kerr.HTTPStatus != http.StatusOK {
		t.Errorf("KucoinError = %+v", kerr)
	}
	expectRequest(t, rec, "/api/v1/ticker", "symbol=NOPE")
}

func TestHTTPErrorStatuses_MapToTransportSentinels(t *testing.T) {
	cases := []struct {
		name     string
		status   int
		body     string
		sentinel error
		code     string
	}{
		{"400 live shape for an unsupported granularity", http.StatusBadRequest, `{"msg":"Unsupported granularity","code":"300000"}`, transport.ErrBadRequest, "300000"},
		{"403", http.StatusForbidden, `{"code":"403000","msg":"Forbidden"}`, transport.ErrForbiddenOrLimited, "403000"},
		{"404 with a non-JSON body", http.StatusNotFound, `<html>not found</html>`, transport.ErrNotFound, ""},
		{"429", http.StatusTooManyRequests, `{"code":"429000","msg":"Too Many Requests"}`, transport.ErrRateLimited, "429000"},
		{"500", http.StatusInternalServerError, `{"code":"500000","msg":"Internal Server Error"}`, transport.ErrServerError, "500000"},
		{"503", http.StatusServiceUnavailable, `{"code":"503000","msg":"Service unavailable"}`, transport.ErrServiceUnavailable, "503000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, rec := newClient(t, testCredentials, respond(tc.status, []byte(tc.body)))

			got, err := client.GetTicker(context.Background(), "XBTUSDTM")
			if got != nil {
				t.Errorf("result = %+v, want nil", got)
			}
			if !errors.Is(err, tc.sentinel) {
				t.Fatalf("error %v does not match %v", err, tc.sentinel)
			}
			var kerr *transport.KucoinError
			if !errors.As(err, &kerr) {
				t.Fatalf("error %v does not carry a *transport.KucoinError", err)
			}
			if kerr.HTTPStatus != tc.status || kerr.Code != tc.code {
				t.Errorf("KucoinError = %+v, want HTTP %d code %q", kerr, tc.status, tc.code)
			}
			if n := len(rec.all()); n != 1 {
				t.Errorf("server saw %d requests, want 1", n)
			}
		})
	}
}

func TestEveryMethod_ReturnsKucoinsBusinessError(t *testing.T) {
	client, _ := newClient(t, testCredentials, respond(http.StatusOK, []byte(`{"msg":"max count should be greater than zero","code":"400100"}`)))

	for _, c := range allCalls() {
		t.Run(c.name, func(t *testing.T) {
			err := c.run(context.Background(), client)
			var kerr *transport.KucoinError
			if !errors.As(err, &kerr) || kerr.Code != "400100" {
				t.Errorf("error = %v, want a *transport.KucoinError with code 400100", err)
			}
		})
	}
}

func TestEveryMethod_ReportsUndecodableData(t *testing.T) {
	client, _ := serveData(t, `"not a payload"`)

	for _, c := range allCalls() {
		t.Run(c.name, func(t *testing.T) {
			err := c.run(context.Background(), client)
			if err == nil || !strings.Contains(err.Error(), "decode response data") {
				t.Errorf("error = %v, want a decode error", err)
			}
		})
	}
}

func TestEveryMethod_HonoursContextCancellation(t *testing.T) {
	client, rec := serveData(t, `null`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for _, c := range allCalls() {
		t.Run(c.name, func(t *testing.T) {
			if err := c.run(ctx, client); !errors.Is(err, context.Canceled) {
				t.Errorf("error = %v, want context.Canceled", err)
			}
		})
	}
	if n := len(rec.all()); n != 0 {
		t.Errorf("%d requests reached the server with a cancelled context", n)
	}
}

func TestEveryPublicMethod_StaysUnsignedDespiteCredentials(t *testing.T) {
	for _, c := range allCalls() {
		if c.name == "Get24hStats" {
			continue // the one endpoint that signs; see its own tests
		}
		t.Run(c.name, func(t *testing.T) {
			client, rec := serveData(t, `null`)
			if err := c.run(context.Background(), client); err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
			reqs := rec.all()
			if len(reqs) != 1 {
				t.Fatalf("server saw %d requests, want 1", len(reqs))
			}
			assertUnsigned(t, reqs[0])
			if reqs[0].Method != http.MethodGet {
				t.Errorf("method = %s, want GET", reqs[0].Method)
			}
		})
	}
}
