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

func TestPlaceOrder_LimitRequiresPrice(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no network call should have been made")
	})

	_, err := client.PlaceOrder(context.Background(), PlaceOrderRequest{Type: "limit", Symbol: "BTC-USDT", Side: "buy", Size: "1"})
	if !errors.Is(err, ErrLimitOrderRequiresPrice) {
		t.Fatalf("expected ErrLimitOrderRequiresPrice, got %v", err)
	}
}

func TestPlaceOrder_MarketOrderDoesNotRequirePrice(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderId":"o1","clientOid":"c1"}}`))
	})

	_, err := client.PlaceOrder(context.Background(), PlaceOrderRequest{Type: "market", Symbol: "BTC-USDT", Side: "buy", Funds: "10"})
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
		Type: "limit", Symbol: "BTC-USDT", Side: "buy", Price: "10000", Size: "1", ClientOid: "my-oid",
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if gotBody["clientOid"] != "my-oid" || ref.ClientOid != "my-oid" {
		t.Errorf("clientOid not passed through as given: body=%v ref=%+v", gotBody["clientOid"], ref)
	}
}

func TestPlaceOrderTest_ValidatesSameAsPlaceOrder(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/hf/orders/test" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderId":"o1","clientOid":"c1"}}`))
	})

	_, err := client.PlaceOrderTest(context.Background(), PlaceOrderRequest{Type: "limit", Symbol: "BTC-USDT", Side: "buy", Size: "1"})
	if !errors.Is(err, ErrLimitOrderRequiresPrice) {
		t.Fatalf("expected ErrLimitOrderRequiresPrice, got %v", err)
	}

	_, err = client.PlaceOrderTest(context.Background(), PlaceOrderRequest{Type: "limit", Symbol: "BTC-USDT", Side: "buy", Price: "10000", Size: "1"})
	if err != nil {
		t.Fatalf("PlaceOrderTest: %v", err)
	}
}

func TestBatchAddOrders_RejectsTooManyItems(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no network call should have been made")
	})

	items := make([]PlaceOrderRequest, 21)
	for i := range items {
		items[i] = PlaceOrderRequest{Type: "market", Symbol: "BTC-USDT", Side: "buy", Funds: "10"}
	}

	_, err := client.BatchAddOrders(context.Background(), items)
	if !errors.Is(err, ErrTooManyBatchOrders) {
		t.Fatalf("expected ErrTooManyBatchOrders, got %v", err)
	}
}

func TestBatchAddOrders_DecodesMixedSuccessFailure(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"success":true,"orderId":"o1","clientOid":"c1"},{"success":false,"failMsg":"insufficient balance"}]}`))
	})

	results, err := client.BatchAddOrders(context.Background(), []PlaceOrderRequest{
		{Type: "market", Symbol: "BTC-USDT", Side: "buy", Funds: "10"},
		{Type: "market", Symbol: "BTC-USDT", Side: "buy", Funds: "999999"},
	})
	if err != nil {
		t.Fatalf("BatchAddOrders: %v", err)
	}
	if len(results) != 2 || results[0].OrderID != "o1" || results[1].Success || results[1].FailMsg == "" {
		t.Errorf("unexpected results: %+v", results)
	}
}

func TestCancelOrderByID_UsesPathAndSymbolQuery(t *testing.T) {
	var gotPath, gotQuery string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.Query().Get("symbol")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderId":"o1"}}`))
	})

	result, err := client.CancelOrderByID(context.Background(), "o1", "BTC-USDT")
	if err != nil {
		t.Fatalf("CancelOrderByID: %v", err)
	}
	if gotPath != "/api/v1/hf/orders/o1" || gotQuery != "BTC-USDT" {
		t.Errorf("unexpected request: path=%s symbol=%s", gotPath, gotQuery)
	}
	if result.OrderID != "o1" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestCancelOrderByClientOid_UsesPathAndSymbolQuery(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"clientOid":"c1"}}`))
	})

	result, err := client.CancelOrderByClientOid(context.Background(), "c1", "BTC-USDT")
	if err != nil {
		t.Fatalf("CancelOrderByClientOid: %v", err)
	}
	if gotPath != "/api/v1/hf/orders/client-order/c1" {
		t.Errorf("unexpected path: %s", gotPath)
	}
	if result.ClientOid != "c1" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestCancelAllOrders(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"succeedSymbols":["BTC-USDT"],"failedSymbols":[]}}`))
	})

	result, err := client.CancelAllOrders(context.Background())
	if err != nil {
		t.Fatalf("CancelAllOrders: %v", err)
	}
	if len(result.SucceedSymbols) != 1 || result.SucceedSymbols[0] != "BTC-USDT" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestGetOrderByID_DecodesFullOrder(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"id":"o1","symbol":"BTC-USDT","type":"limit","side":"buy","price":"50000","size":"1","active":false,"cancelReason":5}}`))
	})

	order, err := client.GetOrderByID(context.Background(), "o1", "BTC-USDT")
	if err != nil {
		t.Fatalf("GetOrderByID: %v", err)
	}
	if order.ID != "o1" || order.CancelReason != 5 || order.Active {
		t.Errorf("unexpected order: %+v", order)
	}
}

func TestGetOpenOrders(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"id":"o1","symbol":"BTC-USDT","active":true}]}`))
	})

	orders, err := client.GetOpenOrders(context.Background(), "BTC-USDT")
	if err != nil {
		t.Fatalf("GetOpenOrders: %v", err)
	}
	if len(orders) != 1 || !orders[0].Active {
		t.Errorf("unexpected orders: %+v", orders)
	}
}

func TestGetClosedOrders_SendsFilters(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"side": r.URL.Query().Get("side"), "limit": r.URL.Query().Get("limit")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"lastId":10,"items":[{"id":"o1"}]}}`))
	})

	page, err := client.GetClosedOrders(context.Background(), "BTC-USDT", GetClosedOrdersOptions{Side: "buy", Limit: 50})
	if err != nil {
		t.Fatalf("GetClosedOrders: %v", err)
	}
	if gotQuery["side"] != "buy" || gotQuery["limit"] != "50" {
		t.Errorf("unexpected query: %v", gotQuery)
	}
	if page.LastID != 10 || len(page.Items) != 1 {
		t.Errorf("unexpected page: %+v", page)
	}
}

func TestGetTradeHistory_UsesIntegerIDs(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"lastId":1,"items":[{"id":123,"orderId":"o1","tradeId":456,"symbol":"BTC-USDT","liquidity":"taker"}]}}`))
	})

	page, err := client.GetTradeHistory(context.Background(), "BTC-USDT", GetTradeHistoryOptions{})
	if err != nil {
		t.Fatalf("GetTradeHistory: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != 123 || page.Items[0].TradeID != 456 {
		t.Errorf("unexpected page: %+v", page)
	}
}
