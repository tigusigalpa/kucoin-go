package orders

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

func TestSetDCP(t *testing.T) {
	var gotPath string
	var gotBody map[string]interface{}
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"currentTime":1700000000,"triggerTime":1700000600}}`))
	})

	trigger, err := client.SetDCP(context.Background(), SetDCPRequest{Timeout: 600, Symbols: "BTC-USDT"})
	if err != nil {
		t.Fatalf("SetDCP: %v", err)
	}
	if gotPath != "/api/v1/hf/orders/dead-cancel-all" || gotBody["timeout"] != float64(600) || trigger.TriggerTime != 1700000600 {
		t.Errorf("unexpected: path=%s body=%v trigger=%+v", gotPath, gotBody, trigger)
	}
}

func TestGetDCP(t *testing.T) {
	var gotPath string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"timeout":600,"symbols":"BTC-USDT","currentTime":1700000000,"triggerTime":1700000600}}`))
	})

	config, err := client.GetDCP(context.Background())
	if err != nil {
		t.Fatalf("GetDCP: %v", err)
	}
	if gotPath != "/api/v1/hf/orders/dead-cancel-all/query" || config.Timeout != 600 || config.Symbols != "BTC-USDT" {
		t.Errorf("unexpected: path=%s config=%+v", gotPath, config)
	}
}

func TestDeactivateDCP_SendsTimeoutNegativeOne(t *testing.T) {
	var gotBody map[string]interface{}
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"currentTime":1700000000,"triggerTime":0}}`))
	})

	_, err := client.DeactivateDCP(context.Background())
	if err != nil {
		t.Fatalf("DeactivateDCP: %v", err)
	}
	if gotBody["timeout"] != float64(-1) {
		t.Errorf("unexpected body: %v", gotBody)
	}
}
