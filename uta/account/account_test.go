package account

import (
	"context"
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
		BaseURL:     server.URL,
		Credentials: transport.Credentials{APIKey: "test-key", APISecret: "test-secret", APIPassphrase: "test-pass"},
	})
	return NewClient(executor)
}

func TestGetOverview(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"accountType":"UNIFIED","riskRatio":"0.1","equity":"1000","adjustedEquity":"950","liability":"0","availableMargin":"900","im":"50","mm":"10"}}`))
	})

	overview, err := client.GetOverview(context.Background())
	if err != nil {
		t.Fatalf("GetOverview: %v", err)
	}
	if overview.Equity != "1000" || overview.AvailableMargin != "900" {
		t.Errorf("unexpected overview: %+v", overview)
	}
}

func TestGetAssets_CurrenciesHelperUnwrapsSingleElementArray(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"accountType":"UNIFIED","ts":123,"accounts":[{"currencies":[{"currency":"USDT","balance":"100"}]}]}}`))
	})

	assets, err := client.GetAssets(context.Background())
	if err != nil {
		t.Fatalf("GetAssets: %v", err)
	}
	currencies := assets.Currencies()
	if len(currencies) != 1 || currencies[0].Currency != "USDT" || currencies[0].Balance != "100" {
		t.Errorf("unexpected currencies: %+v", currencies)
	}
}

func TestGetAssets_CurrenciesHelperHandlesEmptyAccounts(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"accountType":"UNIFIED","ts":123,"accounts":[]}}`))
	})

	assets, err := client.GetAssets(context.Background())
	if err != nil {
		t.Fatalf("GetAssets: %v", err)
	}
	if got := assets.Currencies(); got != nil {
		t.Errorf("expected nil currencies, got %+v", got)
	}
}

func TestGetFeeRate_SendsRequiredParams(t *testing.T) {
	var gotQuery url.Values
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"tradeType":"SPOT","list":[{"symbol":"BTC-USDT","takerFeeRate":"0.001","makerFeeRate":"0.0008"}]}}`))
	})

	result, err := client.GetFeeRate(context.Background(), "SPOT", "BTC-USDT")
	if err != nil {
		t.Fatalf("GetFeeRate: %v", err)
	}
	if gotQuery.Get("tradeType") != "SPOT" || gotQuery.Get("symbol") != "BTC-USDT" {
		t.Errorf("unexpected query: %v", gotQuery)
	}
	if len(result.List) != 1 || result.List[0].TakerFeeRate != "0.001" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestGetLedger_SendsFilters(t *testing.T) {
	var gotQuery url.Values
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"lastId":42,"items":[{"accountType":"UNIFIED","id":"1","currency":"USDT","direction":"IN","businessType":"TRADE_EXCHANGE","amount":"10","balance":"110","fee":"0","tax":"0","remark":"","ts":1700000000000000000}]}}`))
	})

	result, err := client.GetLedger(context.Background(), "UNIFIED", GetLedgerOptions{Direction: "IN", PageSize: 50})
	if err != nil {
		t.Fatalf("GetLedger: %v", err)
	}
	if gotQuery.Get("accountType") != "UNIFIED" || gotQuery.Get("direction") != "IN" || gotQuery.Get("pageSize") != "50" {
		t.Errorf("unexpected query: %v", gotQuery)
	}
	if result.LastID != 42 || len(result.Items) != 1 || result.Items[0].Currency != "USDT" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestGetMode(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"selfAccountMode":"UNIFIED","unifiedSubAccount":[123],"classicSubAccount":[]}}`))
	})

	mode, err := client.GetMode(context.Background())
	if err != nil {
		t.Fatalf("GetMode: %v", err)
	}
	if mode.SelfAccountMode != "UNIFIED" || len(mode.UnifiedSubAccount) != 1 || mode.UnifiedSubAccount[0] != 123 {
		t.Errorf("unexpected mode: %+v", mode)
	}
}

func TestGetAPIKeyInfo(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"uid":12345,"region":"US","kycStatus":1,"remark":"main","apiKey":"abc","apiVersion":3,"permission":"General,Unified","isMaster":true,"createdAt":1700000000000,"siteType":"global"}}`))
	})

	info, err := client.GetAPIKeyInfo(context.Background())
	if err != nil {
		t.Fatalf("GetAPIKeyInfo: %v", err)
	}
	if info.UID != 12345 || info.ApiVersion != 3 || info.Permission != "General,Unified" || !info.IsMaster {
		t.Errorf("unexpected info: %+v", info)
	}
}

func TestAllMethods_RequireCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("no network call should have been made without credentials")
	}))
	defer server.Close()

	executor := transport.NewExecutor(transport.ExecutorConfig{BaseURL: server.URL})
	client := NewClient(executor)
	ctx := context.Background()

	if _, err := client.GetOverview(ctx); err == nil {
		t.Error("GetOverview: expected error without credentials")
	}
	if _, err := client.GetAssets(ctx); err == nil {
		t.Error("GetAssets: expected error without credentials")
	}
	if _, err := client.GetFeeRate(ctx, "SPOT", "BTC-USDT"); err == nil {
		t.Error("GetFeeRate: expected error without credentials")
	}
	if _, err := client.GetLedger(ctx, "UNIFIED", GetLedgerOptions{}); err == nil {
		t.Error("GetLedger: expected error without credentials")
	}
	if _, err := client.GetMode(ctx); err == nil {
		t.Error("GetMode: expected error without credentials")
	}
	if _, err := client.GetAPIKeyInfo(ctx); err == nil {
		t.Error("GetAPIKeyInfo: expected error without credentials")
	}
}
