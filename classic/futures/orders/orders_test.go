package orders

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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

func TestPlaceOrder_RejectsMultipleSizeFields(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no network call should have been made")
	})

	_, err := client.PlaceOrder(context.Background(), PlaceOrderRequest{
		ClientOid: "c1", Side: "buy", Symbol: "XBTUSDTM", Price: "50000", Size: 1, Qty: "1",
	})
	if !errors.Is(err, ErrMutuallyExclusiveSize) {
		t.Fatalf("expected ErrMutuallyExclusiveSize, got %v", err)
	}
}

func TestPlaceOrder_LimitRequiresPrice(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no network call should have been made")
	})

	_, err := client.PlaceOrder(context.Background(), PlaceOrderRequest{ClientOid: "c1", Side: "buy", Symbol: "XBTUSDTM", Size: 1})
	if !errors.Is(err, ErrLimitOrderRequiresPrice) {
		t.Fatalf("expected ErrLimitOrderRequiresPrice, got %v", err)
	}
}

func TestPlaceOrder_CloseOrderDoesNotRequirePrice(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderId":"o1","clientOid":"c1"}}`))
	})

	_, err := client.PlaceOrder(context.Background(), PlaceOrderRequest{ClientOid: "c1", Symbol: "XBTUSDTM", CloseOrder: true})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
}

func TestPlaceOrder_SendsClientOidAsGiven(t *testing.T) {
	var gotBody map[string]interface{}
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderId":"o1","clientOid":"my-oid"}}`))
	})

	ref, err := client.PlaceOrder(context.Background(), PlaceOrderRequest{
		ClientOid: "my-oid", Side: "buy", Symbol: "XBTUSDTM", Price: "50000", Size: 1,
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if gotBody["clientOid"] != "my-oid" || ref.ClientOid != "my-oid" {
		t.Errorf("clientOid not passed through as given: body=%v ref=%+v", gotBody["clientOid"], ref)
	}
}

func TestPlaceOrderTest_SharesValidation(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/orders/test" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderId":"o1","clientOid":"c1"}}`))
	})

	_, err := client.PlaceOrderTest(context.Background(), PlaceOrderRequest{ClientOid: "c1", Side: "buy", Symbol: "XBTUSDTM", Size: 1})
	if !errors.Is(err, ErrLimitOrderRequiresPrice) {
		t.Fatalf("expected ErrLimitOrderRequiresPrice, got %v", err)
	}

	_, err = client.PlaceOrderTest(context.Background(), PlaceOrderRequest{ClientOid: "c1", Side: "buy", Symbol: "XBTUSDTM", Price: "50000", Size: 1})
	if err != nil {
		t.Fatalf("PlaceOrderTest: %v", err)
	}
}

func TestGetOrderByID(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"id":"o1","symbol":"XBTUSDTM","type":"limit","side":"buy","status":"open"}}`))
	})

	order, err := client.GetOrderByID(context.Background(), "o1")
	if err != nil {
		t.Fatalf("GetOrderByID: %v", err)
	}
	if gotPath != "/api/v1/orders/o1" || order.Status != "open" {
		t.Errorf("unexpected: path=%s order=%+v", gotPath, order)
	}
}

func TestGetOrderList_SendsFilters(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"status": r.URL.Query().Get("status"), "pageSize": r.URL.Query().Get("pageSize")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"currentPage":1,"pageSize":50,"totalNum":1,"totalPage":1,"items":[{"id":"o1"}]}}`))
	})

	page, err := client.GetOrderList(context.Background(), GetOrderListOptions{Status: "active", PageSize: 50})
	if err != nil {
		t.Fatalf("GetOrderList: %v", err)
	}
	if gotQuery["status"] != "active" || gotQuery["pageSize"] != "50" {
		t.Errorf("unexpected query: %v", gotQuery)
	}
	if len(page.Items) != 1 {
		t.Errorf("unexpected page: %+v", page)
	}
}
