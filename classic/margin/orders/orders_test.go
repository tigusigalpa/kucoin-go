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

	_, err := client.PlaceOrder(context.Background(), PlaceOrderRequest{ClientOid: "c1", Symbol: "BTC-USDT", Side: "buy"})
	if !errors.Is(err, ErrLimitOrderRequiresPrice) {
		t.Fatalf("expected ErrLimitOrderRequiresPrice, got %v", err)
	}
}

func TestPlaceOrder_MarketDoesNotRequirePrice(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderId":"o1","clientOid":"c1"}}`))
	})

	_, err := client.PlaceOrder(context.Background(), PlaceOrderRequest{ClientOid: "c1", Symbol: "BTC-USDT", Side: "buy", Type: "market", Funds: "100"})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
}

func TestPlaceOrder_SendsAutoBorrowAndDecodesLoanFields(t *testing.T) {
	var gotBody map[string]interface{}
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		if r.URL.Path != "/api/v3/hf/margin/order" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderId":"o1","clientOid":"c1","borrowSize":"0.5","loanApplyId":"loan1"}}`))
	})

	ref, err := client.PlaceOrder(context.Background(), PlaceOrderRequest{
		ClientOid: "c1", Symbol: "BTC-USDT", Side: "buy", Price: "50000", Size: "0.1", IsIsolated: true, AutoBorrow: true,
	})
	if err != nil {
		t.Fatalf("PlaceOrder: %v", err)
	}
	if gotBody["autoBorrow"] != true || gotBody["isIsolated"] != true {
		t.Errorf("unexpected body: %v", gotBody)
	}
	if ref.BorrowSize != "0.5" || ref.LoanApplyID != "loan1" {
		t.Errorf("unexpected ref: %+v", ref)
	}
}

func TestCancelOrderByID(t *testing.T) {
	var gotPath, gotSymbol string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotSymbol = r.URL.Query().Get("symbol")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderId":"o1"}}`))
	})

	result, err := client.CancelOrderByID(context.Background(), "o1", "BTC-USDT")
	if err != nil {
		t.Fatalf("CancelOrderByID: %v", err)
	}
	if gotPath != "/api/v3/hf/margin/orders/o1" || gotSymbol != "BTC-USDT" || result.OrderID != "o1" {
		t.Errorf("unexpected: path=%s symbol=%s result=%+v", gotPath, gotSymbol, result)
	}
}

func TestCancelOrderByClientOid(t *testing.T) {
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
	if gotPath != "/api/v3/hf/margin/orders/client-order/c1" || result.ClientOid != "c1" {
		t.Errorf("unexpected: path=%s result=%+v", gotPath, result)
	}
}

func TestCancelAllOrders_DecodesScalarResult(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"symbol": r.URL.Query().Get("symbol"), "tradeType": r.URL.Query().Get("tradeType")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":"success"}`))
	})

	result, err := client.CancelAllOrders(context.Background(), "BTC-USDT", TradeTypeMargin)
	if err != nil {
		t.Fatalf("CancelAllOrders: %v", err)
	}
	if result != "success" || gotQuery["tradeType"] != TradeTypeMargin {
		t.Errorf("unexpected: result=%s query=%v", result, gotQuery)
	}
}

func TestGetOrderByID(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"id":"o1","symbol":"BTC-USDT","active":true}}`))
	})

	order, err := client.GetOrderByID(context.Background(), "o1", "BTC-USDT")
	if err != nil {
		t.Fatalf("GetOrderByID: %v", err)
	}
	if !order.Active {
		t.Errorf("unexpected: %+v", order)
	}
}

func TestGetOrderByClientOid(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"id":"o1","clientOid":"c1"}}`))
	})

	order, err := client.GetOrderByClientOid(context.Background(), "c1", "BTC-USDT")
	if err != nil {
		t.Fatalf("GetOrderByClientOid: %v", err)
	}
	if gotPath != "/api/v3/hf/margin/orders/client-order/c1" || order.ClientOid != "c1" {
		t.Errorf("unexpected: path=%s order=%+v", gotPath, order)
	}
}

func TestGetOpenOrders_RequiresTradeType(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"symbol": r.URL.Query().Get("symbol"), "tradeType": r.URL.Query().Get("tradeType")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"id":"o1"}]}`))
	})

	orders, err := client.GetOpenOrders(context.Background(), "BTC-USDT", TradeTypeMarginIsolated)
	if err != nil {
		t.Fatalf("GetOpenOrders: %v", err)
	}
	if gotQuery["tradeType"] != TradeTypeMarginIsolated || len(orders) != 1 {
		t.Errorf("unexpected: query=%v orders=%+v", gotQuery, orders)
	}
}

func TestGetClosedOrders_SendsFilters(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"lastId": r.URL.Query().Get("lastId"), "limit": r.URL.Query().Get("limit")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"lastId":100,"items":[{"id":"o1"}]}}`))
	})

	page, err := client.GetClosedOrders(context.Background(), "BTC-USDT", TradeTypeMargin, GetClosedOrdersOptions{LastID: 50, Limit: 10})
	if err != nil {
		t.Fatalf("GetClosedOrders: %v", err)
	}
	if gotQuery["lastId"] != "50" || gotQuery["limit"] != "10" || page.LastID != 100 {
		t.Errorf("unexpected: query=%v page=%+v", gotQuery, page)
	}
}

func TestGetTradeHistory(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"orderId": r.URL.Query().Get("orderId")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"items":[{"id":1,"tradeId":9}],"lastId":9}}`))
	})

	page, err := client.GetTradeHistory(context.Background(), "BTC-USDT", TradeTypeMargin, GetTradeHistoryOptions{OrderID: "o1"})
	if err != nil {
		t.Fatalf("GetTradeHistory: %v", err)
	}
	if gotQuery["orderId"] != "o1" || len(page.Items) != 1 || page.Items[0].TradeID != 9 {
		t.Errorf("unexpected: query=%v page=%+v", gotQuery, page)
	}
}
