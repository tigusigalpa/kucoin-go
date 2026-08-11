package orders

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func TestAddOCOOrder(t *testing.T) {
	var gotPath string
	var gotBody map[string]interface{}
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderId":"o1"}}`))
	})

	ref, err := client.AddOCOOrder(context.Background(), OCOOrderRequest{
		Symbol: "BTC-USDT", Side: "sell", ClientOid: "c1", Price: "50000", Size: "0.001", StopPrice: "45000", LimitPrice: "44900", IsIsolated: true,
	})
	if err != nil {
		t.Fatalf("AddOCOOrder: %v", err)
	}
	if gotPath != "/api/v3/hf/margin/oco-order" || gotBody["isIsolated"] != true || ref.OrderID != "o1" {
		t.Errorf("unexpected: path=%s body=%v ref=%+v", gotPath, gotBody, ref)
	}
}

func TestCancelOCOOrderByID(t *testing.T) {
	var gotPath, gotOrderID string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotOrderID = r.URL.Query().Get("orderId")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"cancelledOrderIds":["o1","o2"]}}`))
	})

	result, err := client.CancelOCOOrderByID(context.Background(), "o1")
	if err != nil {
		t.Fatalf("CancelOCOOrderByID: %v", err)
	}
	if gotPath != "/api/v3/hf/margin/oco-order/cancel-by-id" || gotOrderID != "o1" || len(result.CancelledOrderIDs) != 2 {
		t.Errorf("unexpected: path=%s orderId=%s result=%+v", gotPath, gotOrderID, result)
	}
}

func TestCancelOCOOrderByClientOid(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"cancelledOrderIds":["o1","o2"]}}`))
	})

	result, err := client.CancelOCOOrderByClientOid(context.Background(), "c1")
	if err != nil {
		t.Fatalf("CancelOCOOrderByClientOid: %v", err)
	}
	if gotPath != "/api/v3/hf/margin/oco-order/cancel-by-clientOid" || len(result.CancelledOrderIDs) != 2 {
		t.Errorf("unexpected: path=%s result=%+v", gotPath, result)
	}
}

func TestCancelOCOOrders_SendsFilters(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"symbol": r.URL.Query().Get("symbol"), "tradeType": r.URL.Query().Get("tradeType")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"cancelledOrderIds":["o1","o2","o3","o4"]}}`))
	})

	result, err := client.CancelOCOOrders(context.Background(), "BTC-USDT", TradeTypeMarginIsolated, "")
	if err != nil {
		t.Fatalf("CancelOCOOrders: %v", err)
	}
	if gotQuery["symbol"] != "BTC-USDT" || gotQuery["tradeType"] != TradeTypeMarginIsolated || len(result.CancelledOrderIDs) != 4 {
		t.Errorf("unexpected: query=%v result=%+v", gotQuery, result)
	}
}

func TestGetOCOOrderByID(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderId":"o1","status":"NEW"}}`))
	})

	info, err := client.GetOCOOrderByID(context.Background(), "o1")
	if err != nil {
		t.Fatalf("GetOCOOrderByID: %v", err)
	}
	if gotPath != "/api/v3/hf/margin/oco-order/orderId" || info.Status != "NEW" {
		t.Errorf("unexpected: path=%s info=%+v", gotPath, info)
	}
}

func TestGetOCOOrderByClientOid(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderId":"o1","clientOid":"c1"}}`))
	})

	info, err := client.GetOCOOrderByClientOid(context.Background(), "c1")
	if err != nil {
		t.Fatalf("GetOCOOrderByClientOid: %v", err)
	}
	if gotPath != "/api/v3/hf/margin/oco-order/clientOid" || info.ClientOid != "c1" {
		t.Errorf("unexpected: path=%s info=%+v", gotPath, info)
	}
}

func TestGetOCOOrderDetails_DecodesLegs(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"orderId":"o1","status":"NEW","orders":[{"id":"leg1","side":"sell","price":"50000"},{"id":"leg2","side":"sell","stopPrice":"45000"}]}}`))
	})

	details, err := client.GetOCOOrderDetails(context.Background(), "o1")
	if err != nil {
		t.Fatalf("GetOCOOrderDetails: %v", err)
	}
	if gotPath != "/api/v3/hf/margin/oco-order/detail/orderId" || len(details.Orders) != 2 || details.Orders[0].ID != "leg1" {
		t.Errorf("unexpected: path=%s details=%+v", gotPath, details)
	}
}

func TestGetOCOOrderList_SendsFilters(t *testing.T) {
	var gotQuery map[string]string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = map[string]string{"symbol": r.URL.Query().Get("symbol"), "tradeType": r.URL.Query().Get("tradeType")}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"currentPage":1,"pageSize":50,"totalNum":1,"totalPage":1,"items":[{"orderId":"o1"}]}}`))
	})

	page, err := client.GetOCOOrderList(context.Background(), GetOCOOrderListOptions{Symbol: "BTC-USDT", TradeType: TradeTypeMargin})
	if err != nil {
		t.Fatalf("GetOCOOrderList: %v", err)
	}
	if gotQuery["symbol"] != "BTC-USDT" || gotQuery["tradeType"] != TradeTypeMargin || len(page.Items) != 1 {
		t.Errorf("unexpected: query=%v page=%+v", gotQuery, page)
	}
}
