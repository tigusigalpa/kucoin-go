package leverage

import (
	"context"
	"encoding/json"
	"io"
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

func TestModifyFuturesLeverage(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"leverage":"80.00"}}`))
	})

	result, err := client.ModifyFuturesLeverage(context.Background(), ModifyFuturesLeverageRequest{Symbol: "BTCUSDTM", Leverage: "80"})
	if err != nil {
		t.Fatalf("ModifyFuturesLeverage: %v", err)
	}
	if result.Leverage != "80.00" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestModifyCrossMarginLeverage_CurrencyIsOptional(t *testing.T) {
	var gotBody map[string]interface{}
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"leverage":"5"}}`))
	})

	result, err := client.ModifyCrossMarginLeverage(context.Background(), ModifyCrossMarginLeverageRequest{Leverage: "5"})
	if err != nil {
		t.Fatalf("ModifyCrossMarginLeverage: %v", err)
	}
	if result.Leverage != "5" {
		t.Errorf("unexpected result: %+v", result)
	}
	if _, ok := gotBody["currency"]; ok {
		t.Error("expected currency to be omitted from the request body")
	}
}

func TestGetLeverage_ToleratesIsolateTypoAndIsolatedSpelling(t *testing.T) {
	var gotQuery url.Values
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"symbol":"BTCUSDTM","leverage":"10","marginMode":"ISOLATE"},{"currency":"USDT","leverage":"3","marginMode":"CROSS"}]}`))
	})

	entries, err := client.GetLeverage(context.Background(), "FUTURES", "", "BTCUSDTM", "")
	if err != nil {
		t.Fatalf("GetLeverage: %v", err)
	}
	if gotQuery.Get("tradeType") != "FUTURES" || gotQuery.Get("symbol") != "BTCUSDTM" {
		t.Errorf("unexpected query: %v", gotQuery)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	// Decoded as-is, typo and all -- the SDK must not silently normalize
	// or reject an unexpected-but-documented enum value.
	if entries[0].MarginMode != "ISOLATE" {
		t.Errorf("expected raw ISOLATE value preserved, got %q", entries[0].MarginMode)
	}
	if entries[1].MarginMode != "CROSS" {
		t.Errorf("unexpected marginMode: %q", entries[1].MarginMode)
	}
}
