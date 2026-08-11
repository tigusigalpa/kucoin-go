package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tigusigalpa/kucoin-go/transport"
)

func TestGetPublicToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"token":"pub-token","instanceServers":[{"endpoint":"wss://ws-api-futures.kucoin.com/","encrypt":true,"protocol":"websocket","pingInterval":18000,"pingTimeout":10000}]}}`))
	}))
	defer server.Close()

	executor := transport.NewExecutor(transport.ExecutorConfig{BaseURL: server.URL})
	client := NewClient(executor)

	token, err := client.GetPublicToken(context.Background())
	if err != nil {
		t.Fatalf("GetPublicToken: %v", err)
	}
	if len(token.InstanceServers) != 1 || token.InstanceServers[0].Endpoint != "wss://ws-api-futures.kucoin.com/" {
		t.Errorf("unexpected token: %+v", token)
	}
}

func TestGetPrivateToken_RequiresCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("no network call should have been made")
	}))
	defer server.Close()

	executor := transport.NewExecutor(transport.ExecutorConfig{BaseURL: server.URL})
	client := NewClient(executor)

	if _, err := client.GetPrivateToken(context.Background()); err == nil {
		t.Fatal("expected error without credentials")
	}
}
