package orders

import (
	"context"
	"encoding/json"
	"errors"
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

func TestPlaceOrder_SendsClientOidAsGiven(t *testing.T) {
	var gotBody map[string]interface{}
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"tradeType":"SPOT","orderId":"o1","clientOid":"my-oid-1"}}`))
	})

	ref, err := client.PlaceOrder(context.Background(), PlaceOrderRequest{
		TradeType: "SPOT",
		Symbol:    "BTC-USDT",
		Side:      "BUY",
		OrderType: "LIMIT",
		Size:      "0.001",
		SizeUnit:  "BASECCY",
		Price:     "10000",
		ClientOid: "my-oid-1",
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if gotBody["clientOid"] != "my-oid-1" {
		t.Errorf("clientOid not sent as given: %v", gotBody["clientOid"])
	}
	if ref.OrderID != "o1" || ref.ClientOid != "my-oid-1" {
		t.Errorf("unexpected ref: %+v", ref)
	}
}

func TestPlaceOrder_OmitsClientOidWhenNotProvided(t *testing.T) {
	var gotBody map[string]interface{}
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"tradeType":"SPOT","orderId":"o1","clientOid":""}}`))
	})

	_, err := client.PlaceOrder(context.Background(), PlaceOrderRequest{
		TradeType: "SPOT", Symbol: "BTC-USDT", Side: "BUY", OrderType: "MARKET", Size: "10", SizeUnit: "QUOTECCY",
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if _, ok := gotBody["clientOid"]; ok {
		t.Error("expected clientOid to be omitted, not auto-generated")
	}
}

func TestCancelOrder_RequiresOrderIDOrClientOid(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no network call should have been made")
	})

	_, err := client.CancelOrder(context.Background(), CancelOrderRequest{TradeType: "SPOT", Symbol: "BTC-USDT"})
	if !errors.Is(err, ErrOrderIDOrClientOidRequired) {
		t.Fatalf("expected ErrOrderIDOrClientOidRequired, got %v", err)
	}
}

func TestCancelOrder_Success(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"tradeType":"SPOT","orderId":"o1"}}`))
	})

	result, err := client.CancelOrder(context.Background(), CancelOrderRequest{TradeType: "SPOT", Symbol: "BTC-USDT", OrderID: "o1"})
	if err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}
	if result.OrderID != "o1" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestBatchCancelOrderByID_RejectsTooManyItems(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no network call should have been made")
	})

	items := make([]BatchCancelItemRequest, 21)
	for i := range items {
		items[i] = BatchCancelItemRequest{Symbol: "BTC-USDT", OrderID: "o"}
	}

	_, err := client.BatchCancelOrderByID(context.Background(), BatchCancelByIDRequest{TradeType: "SPOT", CancelOrderList: items})
	if !errors.Is(err, ErrTooManyBatchCancelItems) {
		t.Fatalf("expected ErrTooManyBatchCancelItems, got %v", err)
	}
}

func TestBatchCancelOrderByID_RequiresIDOrClientOidPerItem(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no network call should have been made")
	})

	_, err := client.BatchCancelOrderByID(context.Background(), BatchCancelByIDRequest{
		TradeType:       "SPOT",
		CancelOrderList: []BatchCancelItemRequest{{Symbol: "BTC-USDT"}},
	})
	if !errors.Is(err, ErrOrderIDOrClientOidRequired) {
		t.Fatalf("expected ErrOrderIDOrClientOidRequired, got %v", err)
	}
}

func TestBatchCancelOrderByID_DecodesUTAResultShape(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"tradeType":"SPOT","items":[{"code":"200000","msg":"success","orderId":"o1","ts":1700000000000000000}]}}`))
	})

	result, err := client.BatchCancelOrderByID(context.Background(), BatchCancelByIDRequest{
		TradeType:       "SPOT",
		CancelOrderList: []BatchCancelItemRequest{{Symbol: "BTC-USDT", OrderID: "o1"}},
	})
	if err != nil {
		t.Fatalf("BatchCancelOrderByID: %v", err)
	}
	if len(result.Items) != 1 || result.Items[0].OrderID != "o1" || result.Items[0].Code != "200000" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestBatchCancelOrderBySymbol_SendsFilters(t *testing.T) {
	var gotBody map[string]interface{}
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"tradeType":"SPOT","items":[]}}`))
	})

	_, err := client.BatchCancelOrderBySymbol(context.Background(), BatchCancelBySymbolRequest{
		Symbol: "BTC-USDT", TradeType: "SPOT", OrderFilter: "NORMAL",
	})
	if err != nil {
		t.Fatalf("BatchCancelOrderBySymbol: %v", err)
	}
	if gotBody["orderFilter"] != "NORMAL" || gotBody["symbol"] != "BTC-USDT" {
		t.Errorf("unexpected body: %+v", gotBody)
	}
}

func TestGetOrderDetails_RequiresOrderIDOrClientOid(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no network call should have been made")
	})

	_, err := client.GetOrderDetails(context.Background(), "SPOT", "BTC-USDT", "", "")
	if !errors.Is(err, ErrOrderIDOrClientOidRequired) {
		t.Fatalf("expected ErrOrderIDOrClientOidRequired, got %v", err)
	}
}

func TestGetOrderDetails_DecodesFullOrder(t *testing.T) {
	var gotQuery url.Values
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"tradeType":"SPOT","orderId":"o1","clientOid":"c1","status":3,"filledSize":"0.001","avgPrice":"50000","symbol":"BTC-USDT","side":"BUY","orderType":"LIMIT","size":"0.001","price":"50000","timeInForce":"GTC","orderTime":1700000000000000000}}`))
	})

	order, err := client.GetOrderDetails(context.Background(), "SPOT", "BTC-USDT", "o1", "")
	if err != nil {
		t.Fatalf("GetOrderDetails: %v", err)
	}
	if gotQuery.Get("orderId") != "o1" {
		t.Errorf("unexpected query: %v", gotQuery)
	}
	if order.Status != 3 || order.Symbol != "BTC-USDT" || order.AvgPrice != "50000" {
		t.Errorf("unexpected order: %+v", order)
	}
}

func TestGetOpenOrderList_SendsPagination(t *testing.T) {
	var gotQuery url.Values
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"pageNumber":1,"tradeType":"SPOT","items":[]}}`))
	})

	result, err := client.GetOpenOrderList(context.Background(), "SPOT", GetOpenOrderListOptions{PageNumber: 2, PageSize: 50})
	if err != nil {
		t.Fatalf("GetOpenOrderList: %v", err)
	}
	if gotQuery.Get("pageNumber") != "2" || gotQuery.Get("pageSize") != "50" {
		t.Errorf("unexpected query: %v", gotQuery)
	}
	if result.TradeType != "SPOT" {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestGetOrderHistory_SendsFilters(t *testing.T) {
	var gotQuery url.Values
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"lastId":99,"tradeType":"SPOT","items":[{"orderId":"o1","status":3}]}}`))
	})

	result, err := client.GetOrderHistory(context.Background(), "SPOT", GetOrderHistoryOptions{Side: "BUY", PageSize: 10})
	if err != nil {
		t.Fatalf("GetOrderHistory: %v", err)
	}
	if gotQuery.Get("side") != "BUY" || gotQuery.Get("pageSize") != "10" {
		t.Errorf("unexpected query: %v", gotQuery)
	}
	if result.LastID != 99 || len(result.Items) != 1 {
		t.Errorf("unexpected result: %+v", result)
	}
}

func TestGetTradeHistory_SendsFilters(t *testing.T) {
	var gotQuery url.Values
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"lastId":5,"tradeType":"SPOT","items":[{"symbol":"BTC-USDT","orderId":"o1","orderType":"LIMIT","side":"BUY","fillType":"NORMAL","tradeId":"t1","size":"0.001","value":"50","price":"50000","executionTime":1700000000000000000,"fee":"0.01","feeCurrency":"USDT","liquidityRole":"TAKER","tax":"0"}]}}`))
	})

	result, err := client.GetTradeHistory(context.Background(), "SPOT", GetTradeHistoryOptions{OrderID: "o1"})
	if err != nil {
		t.Fatalf("GetTradeHistory: %v", err)
	}
	if gotQuery.Get("orderId") != "o1" {
		t.Errorf("unexpected query: %v", gotQuery)
	}
	if len(result.Items) != 1 || result.Items[0].TradeID != "t1" || result.Items[0].LiquidityRole != "TAKER" {
		t.Errorf("unexpected result: %+v", result)
	}
}
