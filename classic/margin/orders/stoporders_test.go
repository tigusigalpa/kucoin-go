package orders

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
)

func TestAddStopOrder_LimitRequiresPrice(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no network call should have been made")
	})

	_, err := client.AddStopOrder(context.Background(), StopOrderRequest{Symbol: "BTC-USDT", Side: "buy", StopPrice: "48000"})
	if !errors.Is(err, ErrLimitOrderRequiresPrice) {
		t.Fatalf("expected ErrLimitOrderRequiresPrice, got %v", err)
	}
}

func TestAddStopOrder_SendsIsIsolated(t *testing.T) {
	var gotPath string
	var gotBody map[string]interface{}
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderId":"o1"}}`))
	})

	ref, err := client.AddStopOrder(context.Background(), StopOrderRequest{
		Symbol: "BTC-USDT", Side: "buy", Type: "market", StopPrice: "48000", Funds: "100", IsIsolated: true,
	})
	if err != nil {
		t.Fatalf("AddStopOrder: %v", err)
	}
	if gotPath != "/api/v3/hf/margin/stop-order" || gotBody["isIsolated"] != true || ref.OrderID != "o1" {
		t.Errorf("unexpected: path=%s body=%v ref=%+v", gotPath, gotBody, ref)
	}
}

func TestCancelStopOrderByID(t *testing.T) {
	var gotPath, gotOrderID string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotOrderID = r.URL.Query().Get("orderId")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"cancelledOrderIds":["o1"]}}`))
	})

	result, err := client.CancelStopOrderByID(context.Background(), "o1")
	if err != nil {
		t.Fatalf("CancelStopOrderByID: %v", err)
	}
	if gotPath != "/api/v3/hf/margin/stop-order/cancel-by-id" || gotOrderID != "o1" || len(result.CancelledOrderIDs) != 1 {
		t.Errorf("unexpected: path=%s orderId=%s result=%+v", gotPath, gotOrderID, result)
	}
}

func TestCancelStopOrderByClientOid(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"cancelledOrderIds":["o1"]}}`))
	})

	result, err := client.CancelStopOrderByClientOid(context.Background(), "c1")
	if err != nil {
		t.Fatalf("CancelStopOrderByClientOid: %v", err)
	}
	if gotPath != "/api/v3/hf/margin/stop-order/cancel-by-clientOid" || len(result.CancelledOrderIDs) != 1 {
		t.Errorf("unexpected: path=%s result=%+v", gotPath, result)
	}
}

func TestCancelStopOrders_RequiresTradeType(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"tradeType": r.URL.Query().Get("tradeType"), "symbol": r.URL.Query().Get("symbol")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"cancelledOrderIds":["o1","o2"]}}`))
	})

	result, err := client.CancelStopOrders(context.Background(), "BTC-USDT", TradeTypeMargin, "")
	if err != nil {
		t.Fatalf("CancelStopOrders: %v", err)
	}
	if gotQuery["tradeType"] != TradeTypeMargin || gotQuery["symbol"] != "BTC-USDT" || len(result.CancelledOrderIDs) != 2 {
		t.Errorf("unexpected: query=%v result=%+v", gotQuery, result)
	}
}

func TestGetStopOrderByID(t *testing.T) {
	var gotPath, gotOrderID string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotOrderID = r.URL.Query().Get("orderId")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"id":"o1","status":"NEW","tradeType":"MARGIN_TRADE"}}`))
	})

	order, err := client.GetStopOrderByID(context.Background(), "o1")
	if err != nil {
		t.Fatalf("GetStopOrderByID: %v", err)
	}
	if gotPath != "/api/v3/hf/margin/stop-order/orderId" || gotOrderID != "o1" || order.TradeType != TradeTypeMargin {
		t.Errorf("unexpected: path=%s orderId=%s order=%+v", gotPath, gotOrderID, order)
	}
}

func TestGetStopOrderByClientOid_DecodesSingleObject(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"id":"o1","clientOid":"c1"}}`))
	})

	order, err := client.GetStopOrderByClientOid(context.Background(), "c1")
	if err != nil {
		t.Fatalf("GetStopOrderByClientOid: %v", err)
	}
	if gotPath != "/api/v3/hf/margin/stop-order/clientOid" || order.ClientOid != "c1" {
		t.Errorf("unexpected: path=%s order=%+v", gotPath, order)
	}
}

func TestGetStopOrderList_SendsFilters(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"tradeType": r.URL.Query().Get("tradeType"), "pageSize": r.URL.Query().Get("pageSize")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"currentPage":1,"pageSize":50,"totalNum":1,"totalPage":1,"items":[{"id":"o1"}]}}`))
	})

	page, err := client.GetStopOrderList(context.Background(), GetStopOrderListOptions{TradeType: TradeTypeMarginIsolated, PageSize: 50})
	if err != nil {
		t.Fatalf("GetStopOrderList: %v", err)
	}
	if gotQuery["tradeType"] != TradeTypeMarginIsolated || gotQuery["pageSize"] != "50" || len(page.Items) != 1 {
		t.Errorf("unexpected: query=%v page=%+v", gotQuery, page)
	}
}
