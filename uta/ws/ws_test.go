package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tigusigalpa/kucoin-go/transport"
)

func TestGetPrivateToken(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"token":"abc123"}}`))
	}))
	defer server.Close()

	executor := transport.NewExecutor(transport.ExecutorConfig{
		BaseURL:     server.URL,
		Credentials: transport.Credentials{APIKey: "k", APISecret: "s", APIPassphrase: "p"},
	})
	client := NewClient(executor)

	token, err := client.GetPrivateToken(context.Background())
	if err != nil {
		t.Fatalf("GetPrivateToken: %v", err)
	}
	if gotPath != "/api/v2/bullet-private" || token.Token != "abc123" {
		t.Errorf("unexpected: path=%s token=%+v", gotPath, token)
	}
}
