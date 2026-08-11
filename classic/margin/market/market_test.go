package market

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tigusigalpa/kucoin-go/transport"
)

func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	executor := transport.NewExecutor(transport.ExecutorConfig{BaseURL: server.URL})
	return NewClient(executor)
}

func TestGetSymbols(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"timestamp":1700000000000,"items":[{"symbol":"BTC-USDT","baseCurrency":"BTC"}]}}`))
	})

	page, err := client.GetSymbols(context.Background())
	if err != nil {
		t.Fatalf("GetSymbols: %v", err)
	}
	if gotPath != "/api/v3/margin/symbols" || len(page.Items) != 1 || page.Items[0].Symbol != "BTC-USDT" {
		t.Errorf("unexpected: path=%s page=%+v", gotPath, page)
	}
}

func TestGetIsolatedSymbols(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"symbol":"BTC-USDT","maxLeverage":10}]}`))
	})

	symbols, err := client.GetIsolatedSymbols(context.Background())
	if err != nil {
		t.Fatalf("GetIsolatedSymbols: %v", err)
	}
	if gotPath != "/api/v1/isolated/symbols" || len(symbols) != 1 || symbols[0].MaxLeverage != 10 {
		t.Errorf("unexpected: path=%s symbols=%+v", gotPath, symbols)
	}
}

func TestGetMarkPriceList(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"symbol":"BTC-USDT","timePoint":1700000000000,"value":50000.5}]}`))
	})

	list, err := client.GetMarkPriceList(context.Background())
	if err != nil {
		t.Fatalf("GetMarkPriceList: %v", err)
	}
	if len(list) != 1 || list[0].Value != 50000.5 {
		t.Errorf("unexpected: %+v", list)
	}
}

func TestGetMarkPriceDetail(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"symbol":"BTC-USDT","timePoint":1700000000000,"value":50000.5}}`))
	})

	price, err := client.GetMarkPriceDetail(context.Background(), "BTC-USDT")
	if err != nil {
		t.Fatalf("GetMarkPriceDetail: %v", err)
	}
	if gotPath != "/api/v1/mark-price/BTC-USDT/current" || price.Symbol != "BTC-USDT" {
		t.Errorf("unexpected: path=%s price=%+v", gotPath, price)
	}
}

func TestGetConfig(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"currencyList":["BTC","ETH"],"maxLeverage":10,"warningDebtRatio":"0.97","liqDebtRatio":"0.98"}}`))
	})

	cfg, err := client.GetConfig(context.Background())
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if cfg.MaxLeverage != 10 || len(cfg.CurrencyList) != 2 {
		t.Errorf("unexpected: %+v", cfg)
	}
}

func TestGetRiskLimitCross_SetsIsIsolatedFalse(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"isIsolated": r.URL.Query().Get("isIsolated"), "currency": r.URL.Query().Get("currency")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"currency":"BTC","borrowMaxAmount":"10"}]}`))
	})

	items, err := client.GetRiskLimitCross(context.Background(), "BTC")
	if err != nil {
		t.Fatalf("GetRiskLimitCross: %v", err)
	}
	if gotQuery["isIsolated"] != "false" || gotQuery["currency"] != "BTC" || len(items) != 1 {
		t.Errorf("unexpected: query=%v items=%+v", gotQuery, items)
	}
}

func TestGetRiskLimitIsolated_SetsIsIsolatedTrue(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"isIsolated": r.URL.Query().Get("isIsolated"), "symbol": r.URL.Query().Get("symbol")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"symbol":"BTC-USDT","baseMaxBorrowAmount":"10"}]}`))
	})

	items, err := client.GetRiskLimitIsolated(context.Background(), "BTC-USDT")
	if err != nil {
		t.Fatalf("GetRiskLimitIsolated: %v", err)
	}
	if gotQuery["isIsolated"] != "true" || gotQuery["symbol"] != "BTC-USDT" || len(items) != 1 {
		t.Errorf("unexpected: query=%v items=%+v", gotQuery, items)
	}
}
