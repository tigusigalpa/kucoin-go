package market

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/tigusigalpa/kucoin-go/transport"
)

func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	executor := transport.NewExecutor(transport.ExecutorConfig{
		BaseURL: server.URL,
		// GetOrderBook is the one Market method that's authenticated
		// (see its docblock); harmless test credentials so that test
		// doesn't need special-casing.
		Credentials: transport.Credentials{APIKey: "test-key", APISecret: "test-secret", APIPassphrase: "test-pass"},
	})
	return NewClient(executor)
}

func TestGetInstruments_SendsTradeTypeAndSymbol(t *testing.T) {
	var gotQuery url.Values
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"tradeType":"SPOT","list":[{"symbol":"BTC-USDT"}]}}`))
	})

	result, err := client.GetInstruments(context.Background(), TradeTypeSpot, "BTC-USDT")
	if err != nil {
		t.Fatalf("GetInstruments: %v", err)
	}
	if gotQuery.Get("tradeType") != "SPOT" || gotQuery.Get("symbol") != "BTC-USDT" {
		t.Errorf("unexpected query: %v", gotQuery)
	}
	if len(result.List) != 1 || result.List[0].Symbol != "BTC-USDT" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestGetTickers_OmitsEmptySymbol(t *testing.T) {
	var gotQuery url.Values
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"tradeType":"SPOT","list":[]}}`))
	})

	_, err := client.GetTickers(context.Background(), TradeTypeSpot, "")
	if err != nil {
		t.Fatalf("GetTickers: %v", err)
	}
	if _, ok := gotQuery["symbol"]; ok {
		t.Error("expected symbol to be omitted from query")
	}
}

func TestGetOrderBook_DecodesBidsAndAsks(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"tradeType":"SPOT","symbol":"BTC-USDT","sequence":123,"bids":[["50000","1.5"]],"asks":[["50001","2.0"]]}}`))
	})

	book, err := client.GetOrderBook(context.Background(), TradeTypeSpot, "BTC-USDT", GetOrderBookOptions{Limit: 20})
	if err != nil {
		t.Fatalf("GetOrderBook: %v", err)
	}
	if len(book.Bids) != 1 || book.Bids[0][0] != "50000" {
		t.Errorf("unexpected bids: %+v", book.Bids)
	}
	if len(book.Asks) != 1 || book.Asks[0][1] != "2.0" {
		t.Errorf("unexpected asks: %+v", book.Asks)
	}
}

func TestGetKlines_DecodesArrayFormat(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[[1622185200,"100","110","95","105","1000","105000"]]}`))
	})

	klines, err := client.GetKlines(context.Background(), TradeTypeSpot, "BTC-USDT", "1hour", 1622185200, 1622188800)
	if err != nil {
		t.Fatalf("GetKlines: %v", err)
	}
	if len(klines) != 1 {
		t.Fatalf("expected 1 kline, got %d", len(klines))
	}
	k := klines[0]
	if k.Timestamp != 1622185200 || k.Open != "100" || k.High != "110" || k.Low != "95" || k.Close != "105" || k.Volume != "1000" || k.Turnover != "105000" {
		t.Errorf("unexpected kline: %+v", k)
	}
}

func TestKline_UnmarshalJSON_RoundTrip(t *testing.T) {
	var k Kline
	raw := `[1700000000,"1","2","0.5","1.5","10","15"]`
	if err := json.Unmarshal([]byte(raw), &k); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if k.Timestamp != 1700000000 || k.Open != "1" || k.High != "2" || k.Low != "0.5" || k.Close != "1.5" || k.Volume != "10" || k.Turnover != "15" {
		t.Errorf("unexpected kline: %+v", k)
	}
}

func TestGetTrades(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"tradeType":"SPOT","list":[{"sequence":"1","tradeId":"t1","price":"50000","size":"0.1","side":"BUY","ts":1700000000000000000}]}}`))
	})

	result, err := client.GetTrades(context.Background(), TradeTypeSpot, "BTC-USDT")
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(result.List) != 1 || result.List[0].TradeID != "t1" || result.List[0].Side != "BUY" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestGetCurrencies_SendsFilters(t *testing.T) {
	var gotQuery url.Values
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"currency":"BTC","items":[{"chainName":"Bitcoin"}]}]}`))
	})

	result, err := client.GetCurrencies(context.Background(), "BTC,ETH", "btc")
	if err != nil {
		t.Fatalf("GetCurrencies: %v", err)
	}
	if gotQuery.Get("currencyList") != "BTC,ETH" || gotQuery.Get("chain") != "btc" {
		t.Errorf("unexpected query: %v", gotQuery)
	}
	if len(result) != 1 || result[0].Currency != "BTC" || len(result[0].Items) != 1 {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestGetCurrency_SingleLookup(t *testing.T) {
	var gotQuery url.Values
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"currency":"BTC"}]}`))
	})

	result, err := client.GetCurrency(context.Background(), "BTC", "")
	if err != nil {
		t.Fatalf("GetCurrency: %v", err)
	}
	if gotQuery.Get("currency") != "BTC" {
		t.Errorf("unexpected query: %v", gotQuery)
	}
	if len(result) != 1 || result[0].Currency != "BTC" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestGetAnnouncements_SendsPaginationParams(t *testing.T) {
	var gotQuery url.Values
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"totalNumber":1,"totalPage":1,"pageNumber":1,"pageSize":10,"list":[{"id":"1","title":"Maintenance"}]}}`))
	})

	result, err := client.GetAnnouncements(context.Background(), GetAnnouncementsOptions{
		Language: "en_US", PageNumber: 1, PageSize: 10,
	})
	if err != nil {
		t.Fatalf("GetAnnouncements: %v", err)
	}
	if gotQuery.Get("language") != "en_US" || gotQuery.Get("pageNumber") != "1" || gotQuery.Get("pageSize") != "10" {
		t.Errorf("unexpected query: %v", gotQuery)
	}
	if len(result.List) != 1 || result.List[0].Title != "Maintenance" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestGetServiceStatus(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"tradeType":"SPOT","serverStatus":"open","msg":""}}`))
	})

	status, err := client.GetServiceStatus(context.Background(), TradeTypeSpot)
	if err != nil {
		t.Fatalf("GetServiceStatus: %v", err)
	}
	if status.ServerStatus != "open" {
		t.Errorf("ServerStatus = %q, want open", status.ServerStatus)
	}
}

func TestGetTradeStatistics(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"spot":{"turnoverOf24h":"1000000"},"futures":{"turnoverOf24h":"2000000"}}}`))
	})

	stats, err := client.GetTradeStatistics(context.Background())
	if err != nil {
		t.Fatalf("GetTradeStatistics: %v", err)
	}
	if stats.Spot.TurnoverOf24h != "1000000" || stats.Futures.TurnoverOf24h != "2000000" {
		t.Errorf("unexpected stats: %+v", stats)
	}
}
