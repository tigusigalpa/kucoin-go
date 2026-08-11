package market

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tigusigalpa/kucoin-go/transport"
)

func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	executor := transport.NewExecutor(transport.ExecutorConfig{
		BaseURL:     server.URL,
		Credentials: transport.Credentials{APIKey: "test-key", APISecret: "test-secret", APIPassphrase: "test-pass"},
	})
	return NewClient(executor)
}

func TestGetCurrency_DecodesSingleObjectNotArray(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"currency":"BTC","name":"BTC","fullName":"Bitcoin","precision":8,"isMarginEnabled":true,"isDebitEnabled":true,"chains":[{"chainName":"BTC","withdrawFeeRate":"0","isWithdrawEnabled":true,"isDepositEnabled":true}]}}`))
	})

	currency, err := client.GetCurrency(context.Background(), "BTC", "")
	if err != nil {
		t.Fatalf("GetCurrency: %v", err)
	}
	if gotPath != "/api/v3/currencies/BTC" {
		t.Errorf("unexpected path: %s", gotPath)
	}
	if currency.Currency != "BTC" || len(currency.Chains) != 1 {
		t.Errorf("unexpected currency: %+v", currency)
	}
}

func TestGetAllSymbols(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"symbol":"BTC-USDT","baseCurrency":"BTC","quoteCurrency":"USDT","enableTrading":true}]}`))
	})

	symbols, err := client.GetAllSymbols(context.Background(), "")
	if err != nil {
		t.Fatalf("GetAllSymbols: %v", err)
	}
	if len(symbols) != 1 || symbols[0].Symbol != "BTC-USDT" {
		t.Errorf("unexpected symbols: %+v", symbols)
	}
}

func TestGetTicker(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"time":1700000000000,"sequence":"1","price":"50000","size":"0.1","bestBid":"49999","bestBidSize":"1","bestAsk":"50001","bestAskSize":"1"}}`))
	})

	ticker, err := client.GetTicker(context.Background(), "BTC-USDT")
	if err != nil {
		t.Fatalf("GetTicker: %v", err)
	}
	if ticker.Price != "50000" || ticker.BestBid != "49999" {
		t.Errorf("unexpected ticker: %+v", ticker)
	}
}

func TestGetAllTickers(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"time":1700000000000,"ticker":[{"symbol":"BTC-USDT","last":"50000","high":"51000","low":"49000"}]}}`))
	})

	tickers, err := client.GetAllTickers(context.Background())
	if err != nil {
		t.Fatalf("GetAllTickers: %v", err)
	}
	if len(tickers.Ticker) != 1 || tickers.Ticker[0].Symbol != "BTC-USDT" || tickers.Ticker[0].Last != "50000" {
		t.Errorf("unexpected tickers: %+v", tickers)
	}
}

func TestGetKlines_DecodesOpenCloseHighLowOrder(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[["1622185200","100","105","110","95","1000","105000"]]}`))
	})

	klines, err := client.GetKlines(context.Background(), "BTC-USDT", Interval1Hour, 0, 0)
	if err != nil {
		t.Fatalf("GetKlines: %v", err)
	}
	if len(klines) != 1 {
		t.Fatalf("expected 1 kline, got %d", len(klines))
	}
	k := klines[0]
	// wire order is [startTime, open, close, high, low, volume, turnover]
	if k.StartTime != 1622185200 || k.Open != "100" || k.Close != "105" || k.High != "110" || k.Low != "95" {
		t.Errorf("unexpected kline field mapping: %+v", k)
	}
}

func TestGetPartOrderBook_ValidatesSize(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no network call should have been made for an invalid size")
	})

	_, err := client.GetPartOrderBook(context.Background(), "BTC-USDT", 50)
	if err == nil {
		t.Fatal("expected error for invalid size")
	}
}

func TestGetPartOrderBook_UsesSizeInPath(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"time":1,"sequence":"1","bids":[["49999","1"]],"asks":[["50001","1"]]}}`))
	})

	book, err := client.GetPartOrderBook(context.Background(), "BTC-USDT", 20)
	if err != nil {
		t.Fatalf("GetPartOrderBook: %v", err)
	}
	if gotPath != "/api/v1/market/orderbook/level2_20" {
		t.Errorf("unexpected path: %s", gotPath)
	}
	if len(book.Bids) != 1 || book.Bids[0][0] != "49999" {
		t.Errorf("unexpected book: %+v", book)
	}
}

func TestGetFullOrderBook_RequiresCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("no network call should have been made")
	}))
	defer server.Close()

	executor := transport.NewExecutor(transport.ExecutorConfig{BaseURL: server.URL})
	client := NewClient(executor)

	_, err := client.GetFullOrderBook(context.Background(), "BTC-USDT")
	if !errors.Is(err, transport.ErrCredentialsRequired) {
		t.Fatalf("expected ErrCredentialsRequired, got %v", err)
	}
}

func TestGetServerTime_DecodesBareNumber(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":1729100692873}`))
	})

	ts, err := client.GetServerTime(context.Background())
	if err != nil {
		t.Fatalf("GetServerTime: %v", err)
	}
	if ts != 1729100692873 {
		t.Errorf("ts = %d, want 1729100692873", ts)
	}
}

func TestKline_UnmarshalJSON_RoundTrip(t *testing.T) {
	var k Kline
	raw := `["1700000000","1","1.5","2","0.5","10","15"]`
	if err := json.Unmarshal([]byte(raw), &k); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if k.StartTime != 1700000000 || k.Open != "1" || k.Close != "1.5" || k.High != "2" || k.Low != "0.5" {
		t.Errorf("unexpected kline: %+v", k)
	}
}
