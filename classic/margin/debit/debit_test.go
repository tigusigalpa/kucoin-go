package debit

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tigusigalpa/kucoin-go/transport"
)

func newTestClient(t *testing.T, handler http.HandlerFunc, withCreds bool) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	cfg := transport.ExecutorConfig{BaseURL: server.URL}
	if withCreds {
		cfg.Credentials = transport.Credentials{APIKey: "test-key", APISecret: "test-secret", APIPassphrase: "test-pass"}
	}
	executor := transport.NewExecutor(cfg)
	return NewClient(executor)
}

func TestGetBorrowRate_NoCredentialsNeeded(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"currency": r.URL.Query().Get("currency"), "vipLevel": r.URL.Query().Get("vipLevel")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"vipLevel":0,"items":[{"currency":"BTC","hourlyBorrowRate":"0.0001","annualizedBorrowRate":"0.876"}]}}`))
	}, false)

	page, err := client.GetBorrowRate(context.Background(), 1, "BTC")
	if err != nil {
		t.Fatalf("GetBorrowRate: %v", err)
	}
	if gotQuery["currency"] != "BTC" || gotQuery["vipLevel"] != "1" || len(page.Items) != 1 {
		t.Errorf("unexpected: query=%v page=%+v", gotQuery, page)
	}
}

func TestBorrow(t *testing.T) {
	var gotBody map[string]interface{}
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		if r.URL.Path != "/api/v3/margin/borrow" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderNo":"b1","actualSize":"0.5"}}`))
	}, true)

	ref, err := client.Borrow(context.Background(), BorrowRequest{Currency: "BTC", Size: "0.5", TimeInForce: "FOK", IsIsolated: true, Symbol: "BTC-USDT"})
	if err != nil {
		t.Fatalf("Borrow: %v", err)
	}
	if gotBody["isIsolated"] != true || ref.OrderNo != "b1" {
		t.Errorf("unexpected: body=%v ref=%+v", gotBody, ref)
	}
}

func TestGetBorrowHistory_SendsFilters(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"currency": r.URL.Query().Get("currency"), "isIsolated": r.URL.Query().Get("isIsolated"), "pageSize": r.URL.Query().Get("pageSize")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"timestamp":1,"currentPage":1,"pageSize":10,"totalNum":1,"totalPage":1,"items":[{"orderNo":"b1","status":"SUCCESS"}]}}`))
	}, true)

	page, err := client.GetBorrowHistory(context.Background(), "BTC", HistoryOptions{IsIsolated: true, PageSize: 10})
	if err != nil {
		t.Fatalf("GetBorrowHistory: %v", err)
	}
	if gotQuery["currency"] != "BTC" || gotQuery["isIsolated"] != "true" || gotQuery["pageSize"] != "10" || len(page.Items) != 1 {
		t.Errorf("unexpected: query=%v page=%+v", gotQuery, page)
	}
}

func TestRepay(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v3/margin/repay" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"timestamp":1700000000000,"orderNo":"r1","actualSize":"0.5"}}`))
	}, true)

	ref, err := client.Repay(context.Background(), RepayRequest{Currency: "BTC", Size: "0.5"})
	if err != nil {
		t.Fatalf("Repay: %v", err)
	}
	if ref.OrderNo != "r1" {
		t.Errorf("unexpected: %+v", ref)
	}
}

func TestGetRepayHistory(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"timestamp":1,"currentPage":1,"pageSize":50,"totalNum":1,"totalPage":1,"items":[{"orderNo":"r1","principal":"0.5","interest":"0.001"}]}}`))
	}, true)

	page, err := client.GetRepayHistory(context.Background(), "BTC", HistoryOptions{})
	if err != nil {
		t.Fatalf("GetRepayHistory: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].Interest != "0.001" {
		t.Errorf("unexpected: %+v", page)
	}
}

func TestGetInterestHistory_IgnoresOrderNo(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"orderNo": r.URL.Query().Get("orderNo"), "currency": r.URL.Query().Get("currency")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"timestamp":1,"currentPage":1,"pageSize":50,"totalNum":1,"totalPage":1,"items":[{"currency":"BTC","dayRatio":"0.0001","interestAmount":"0.00005"}]}}`))
	}, true)

	page, err := client.GetInterestHistory(context.Background(), "BTC", HistoryOptions{OrderNo: "should-be-ignored"})
	if err != nil {
		t.Fatalf("GetInterestHistory: %v", err)
	}
	if gotQuery["orderNo"] != "" || gotQuery["currency"] != "BTC" || len(page.Items) != 1 {
		t.Errorf("unexpected: query=%v page=%+v", gotQuery, page)
	}
}

func TestModifyLeverage(t *testing.T) {
	var gotBody map[string]interface{}
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		if r.URL.Path != "/api/v3/position/update-user-leverage" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":null}`))
	}, true)

	err := client.ModifyLeverage(context.Background(), ModifyLeverageRequest{Leverage: "5", IsIsolated: true, Symbol: "BTC-USDT"})
	if err != nil {
		t.Fatalf("ModifyLeverage: %v", err)
	}
	if gotBody["leverage"] != "5" {
		t.Errorf("unexpected body: %v", gotBody)
	}
}
