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

func TestAddStopOrder_MarketDoesNotRequirePrice(t *testing.T) {
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
		Symbol: "BTC-USDT", Side: "buy", Type: "market", StopPrice: "48000", Funds: "100",
	})
	if err != nil {
		t.Fatalf("AddStopOrder: %v", err)
	}
	if gotPath != "/api/v1/stop-order" || gotBody["stopPrice"] != "48000" || ref.OrderID != "o1" {
		t.Errorf("unexpected: path=%s body=%v ref=%+v", gotPath, gotBody, ref)
	}
}

func TestCancelStopOrderByID(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"cancelledOrderIds":["o1"]}}`))
	})

	result, err := client.CancelStopOrderByID(context.Background(), "o1")
	if err != nil {
		t.Fatalf("CancelStopOrderByID: %v", err)
	}
	if gotPath != "/api/v1/stop-order/o1" || len(result.CancelledOrderIDs) != 1 {
		t.Errorf("unexpected: path=%s result=%+v", gotPath, result)
	}
}

func TestCancelStopOrderByClientOid(t *testing.T) {
	var gotPath, gotClientOid string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotClientOid = r.URL.Query().Get("clientOid")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"cancelledOrderId":"o1","clientOid":"c1"}}`))
	})

	result, err := client.CancelStopOrderByClientOid(context.Background(), "c1", "BTC-USDT")
	if err != nil {
		t.Fatalf("CancelStopOrderByClientOid: %v", err)
	}
	if gotPath != "/api/v1/stop-order/cancelOrderByClientOid" || gotClientOid != "c1" || result.ClientOid != "c1" {
		t.Errorf("unexpected: path=%s clientOid=%s result=%+v", gotPath, gotClientOid, result)
	}
}

func TestCancelStopOrders_SendsFilters(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"symbol": r.URL.Query().Get("symbol"), "orderIds": r.URL.Query().Get("orderIds")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"cancelledOrderIds":["o1","o2"]}}`))
	})

	result, err := client.CancelStopOrders(context.Background(), "BTC-USDT", "", "o1,o2")
	if err != nil {
		t.Fatalf("CancelStopOrders: %v", err)
	}
	if gotQuery["symbol"] != "BTC-USDT" || gotQuery["orderIds"] != "o1,o2" || len(result.CancelledOrderIDs) != 2 {
		t.Errorf("unexpected: query=%v result=%+v", gotQuery, result)
	}
}

func TestGetStopOrderByID(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"id":"o1","status":"NEW","stopPrice":"48000"}}`))
	})

	order, err := client.GetStopOrderByID(context.Background(), "o1")
	if err != nil {
		t.Fatalf("GetStopOrderByID: %v", err)
	}
	if order.Status != "NEW" || order.StopPrice != "48000" {
		t.Errorf("unexpected: %+v", order)
	}
}

func TestGetStopOrderByClientOid_DecodesArray(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"id":"o1","clientOid":"c1"}]}`))
	})

	orders, err := client.GetStopOrderByClientOid(context.Background(), "c1", "")
	if err != nil {
		t.Fatalf("GetStopOrderByClientOid: %v", err)
	}
	if gotPath != "/api/v1/stop-order/queryOrderByClientOid" || len(orders) != 1 || orders[0].ClientOid != "c1" {
		t.Errorf("unexpected: path=%s orders=%+v", gotPath, orders)
	}
}

func TestGetStopOrderList_SendsFilters(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"stop": r.URL.Query().Get("stop"), "pageSize": r.URL.Query().Get("pageSize")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"currentPage":1,"pageSize":50,"totalNum":1,"totalPage":1,"items":[{"id":"o1"}]}}`))
	})

	page, err := client.GetStopOrderList(context.Background(), GetStopOrderListOptions{Stop: "stop", PageSize: 50})
	if err != nil {
		t.Fatalf("GetStopOrderList: %v", err)
	}
	if gotQuery["stop"] != "stop" || gotQuery["pageSize"] != "50" || len(page.Items) != 1 {
		t.Errorf("unexpected: query=%v page=%+v", gotQuery, page)
	}
}
