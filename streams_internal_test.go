package kucoin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tigusigalpa/kucoin-go/transport"
	utav2market "github.com/tigusigalpa/kucoin-go/uta/v2/market"
)

// The REST snapshot of the deprecated UTA increment book must be the complete
// book: a truncated one leaves every level beyond the cut-off missing from a book
// that is kept in step with the feed.
func TestUTABookSnapshotRequestsTheFullBook(t *testing.T) {
	var gotLimit, gotTradeType, gotSymbol string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		gotLimit, gotTradeType, gotSymbol = q.Get("limit"), q.Get("tradeType"), q.Get("symbol")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":"200000","data":{"tradeType":"SPOT","symbol":"BTC-USDT","sequence":42,"bids":[["100","1"]],"asks":[["101","2","3"]]}}`))
	}))
	t.Cleanup(srv.Close)
	market := utav2market.NewClient(transport.NewExecutor(transport.ExecutorConfig{
		BaseURL:     srv.URL,
		Credentials: transport.Credentials{APIKey: "key", APISecret: "secret", APIPassphrase: "pass"},
	}))

	snap, err := utaBookSnapshot(market)(context.Background(), "SPOT", "BTC-USDT")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if gotLimit != "FULL" || gotTradeType != "SPOT" || gotSymbol != "BTC-USDT" {
		t.Fatalf("request limit=%q tradeType=%q symbol=%q, want limit=FULL", gotLimit, gotTradeType, gotSymbol)
	}
	if snap.Sequence != 42 || len(snap.Bids) != 1 || snap.Bids[0].Price != "100" || len(snap.Asks) != 1 || snap.Asks[0].RPISize != "3" {
		t.Fatalf("snapshot: %+v", snap)
	}
}
