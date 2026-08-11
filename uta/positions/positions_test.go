package positions

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
		BaseURL:     server.URL,
		Credentials: transport.Credentials{APIKey: "test-key", APISecret: "test-secret", APIPassphrase: "test-pass"},
	})
	return NewClient(executor)
}

// TestPosition_Value_HandlesBothDocumentedKeyNames covers the known
// KuCoin documentation inconsistency: the schema names "positionValue"
// but the worked example shows "positionMargin" in the same slot.
func TestPosition_Value_HandlesBothDocumentedKeyNames(t *testing.T) {
	var withPositionValue Position
	if err := json.Unmarshal([]byte(`{"symbol":"BTCUSDTM","positionValue":"1000"}`), &withPositionValue); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if withPositionValue.Value() != "1000" {
		t.Errorf("Value() = %q, want 1000", withPositionValue.Value())
	}

	var withPositionMargin Position
	if err := json.Unmarshal([]byte(`{"symbol":"BTCUSDTM","positionMargin":"2000"}`), &withPositionMargin); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if withPositionMargin.Value() != "2000" {
		t.Errorf("Value() = %q, want 2000", withPositionMargin.Value())
	}
}

func TestGetPositions_ReturnsPlainArray(t *testing.T) {
	var gotQuery url.Values
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"symbol":"BTCUSDTM","id":"p1","marginMode":"CROSS","size":"1","entryPrice":"50000"}]}`))
	})

	positions, err := client.GetPositions(context.Background(), GetPositionsOptions{Symbol: "BTCUSDTM"})
	if err != nil {
		t.Fatalf("GetPositions: %v", err)
	}
	if gotQuery.Get("symbol") != "BTCUSDTM" {
		t.Errorf("unexpected query: %v", gotQuery)
	}
	if len(positions) != 1 || positions[0].ID != "p1" || positions[0].MarginMode != "CROSS" {
		t.Errorf("unexpected positions: %+v", positions)
	}
}

func TestGetPositionHistory(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"lastId":10,"items":[{"symbol":"BTCUSDTM","closeId":"c1","side":"LONG","realizedPnL":"100"}]}}`))
	})

	page, err := client.GetPositionHistory(context.Background(), GetPositionHistoryOptions{})
	if err != nil {
		t.Fatalf("GetPositionHistory: %v", err)
	}
	if page.LastID != 10 || len(page.Items) != 1 || page.Items[0].RealizedPnL != "100" {
		t.Errorf("unexpected page: %+v", page)
	}
}

func TestGetFundingFeeHistory_MillisecondSettlementTime(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"lastId":1,"items":[{"symbol":"BTCUSDTM","fundingFee":"0.5","settlementTime":1700000000000}]}}`))
	})

	page, err := client.GetFundingFeeHistory(context.Background(), GetFundingFeeHistoryOptions{})
	if err != nil {
		t.Fatalf("GetFundingFeeHistory: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].SettlementTime != 1700000000000 {
		t.Errorf("unexpected page: %+v", page)
	}
}

func TestBatchModifyMarginMode(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"ts":123,"items":[{"symbol":"BTCUSDTM","marginMode":"CROSS","code":"200000","msg":"success"}]}}`))
	})

	result, err := client.BatchModifyMarginMode(context.Background(), BatchModifyMarginModeRequest{Symbol: "BTCUSDTM", MarginMode: "CROSS"})
	if err != nil {
		t.Fatalf("BatchModifyMarginMode: %v", err)
	}
	if len(result.Items) != 1 || result.Items[0].Symbol != "BTCUSDTM" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestModifyPositionMargin(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"ts":456}}`))
	})

	result, err := client.ModifyPositionMargin(context.Background(), ModifyPositionMarginRequest{
		Type: "DEPOSIT", Amount: "10", Symbol: "BTCUSDTM", TradeType: "FUTURES",
	})
	if err != nil {
		t.Fatalf("ModifyPositionMargin: %v", err)
	}
	if result.Ts != 456 {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestGetMarginMode(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"ts":1,"items":[{"symbol":"BTCUSDTM","marginMode":"ISOLATED"}]}}`))
	})

	result, err := client.GetMarginMode(context.Background(), "BTCUSDTM")
	if err != nil {
		t.Fatalf("GetMarginMode: %v", err)
	}
	if len(result.Items) != 1 || result.Items[0].MarginMode != "ISOLATED" {
		t.Errorf("unexpected result: %+v", result)
	}
}
