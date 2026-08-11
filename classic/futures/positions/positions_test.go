package positions

import (
	"context"
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

func TestGetPositionDetails_DecodesArrayResponse(t *testing.T) {
	var gotQuery string
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Get("symbol")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"id":"p1","symbol":"XBTUSDTM","maintMarginReq":"0.005","crossMode":false,"delevPercentage":"0.1","currentQty":1,"currentCost":"100","currentComm":"0","unrealisedCost":"100","realisedGrossCost":"0","realisedCost":"0","isOpen":true,"markPrice":"50000","markValue":"50000","posCost":"100","posInit":"100","posMargin":"100","posMaint":"50","realisedGrossPnl":"0","realisedPnl":"0","unrealisedPnl":"10","unrealisedPnlPcnt":"0.1","unrealisedRoePcnt":"0.1","avgEntryPrice":"49900","liquidationPrice":"45000","bankruptPrice":"44000","settleCurrency":"USDT","isInverse":false,"maintainMargin":"50","marginMode":"CROSS","positionSide":"BOTH","leverage":"10","cumulativeTradeFee":"0","cumulativeFundingFee":"0","cumulativeTax":"0"}]}`))
	})

	positions, err := client.GetPositionDetails(context.Background(), "XBTUSDTM")
	if err != nil {
		t.Fatalf("GetPositionDetails: %v", err)
	}
	if gotQuery != "XBTUSDTM" {
		t.Errorf("unexpected query: %s", gotQuery)
	}
	if len(positions) != 1 || positions[0].ID != "p1" || positions[0].MarkPrice != "50000" {
		t.Errorf("unexpected positions: %+v", positions)
	}
}

func TestGetPositionList_DecodesNumericFieldsAsJSONNumber(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[{"id":"p1","symbol":"XBTUSDTM","crossMode":false,"maintMarginReq":0.005,"delevPercentage":0.1,"currentQty":1,"currentCost":100.5,"currentComm":0,"unrealisedCost":100.5,"realisedGrossCost":0,"realisedCost":0,"isOpen":true,"markPrice":50000.1234,"markValue":50000,"posCost":100.5,"posInit":100.5,"posMargin":100.5,"posMaint":50,"realisedGrossPnl":0,"realisedPnl":0,"unrealisedPnl":10.5,"unrealisedPnlPcnt":0.1,"unrealisedRoePcnt":0.1,"avgEntryPrice":49900,"liquidationPrice":45000,"bankruptPrice":44000,"settleCurrency":"USDT","isInverse":false,"maintainMargin":50,"marginMode":"CROSS","positionSide":"BOTH","leverage":10,"dealComm":0,"fundingFee":0,"tax":0}]}`))
	})

	positions, err := client.GetPositionList(context.Background(), "USDT")
	if err != nil {
		t.Fatalf("GetPositionList: %v", err)
	}
	if len(positions) != 1 {
		t.Fatalf("expected 1 position, got %d", len(positions))
	}
	// json.Number preserves the exact literal text -- verify no precision
	// was lost converting through float64.
	if positions[0].MarkPrice.String() != "50000.1234" {
		t.Errorf("MarkPrice = %s, want 50000.1234", positions[0].MarkPrice.String())
	}
}

func TestGetMarginMode(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"symbol":"XBTUSDTM","marginMode":"ISOLATED"}}`))
	})

	mode, err := client.GetMarginMode(context.Background(), "XBTUSDTM")
	if err != nil {
		t.Fatalf("GetMarginMode: %v", err)
	}
	if mode.MarginMode != "ISOLATED" {
		t.Errorf("unexpected mode: %+v", mode)
	}
}

func TestGetPositionMode_DecodesIntegerEnum(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"positionMode":1}}`))
	})

	mode, err := client.GetPositionMode(context.Background())
	if err != nil {
		t.Fatalf("GetPositionMode: %v", err)
	}
	if mode.PositionMode != 1 {
		t.Errorf("unexpected mode: %+v", mode)
	}
}
