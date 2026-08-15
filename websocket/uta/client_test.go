package uta

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func fakeServer(t *testing.T, handle func(conn *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()

		if err := conn.WriteJSON(map[string]any{
			"sessionId":    "sess-1",
			"message":      "welcome",
			"pingInterval": 15000,
		}); err != nil {
			t.Errorf("write welcome: %v", err)
			return
		}
		handle(conn)
	}))
}

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func TestClient_ConnectSubscribePush(t *testing.T) {
	server := fakeServer(t, func(conn *websocket.Conn) {
		for {
			var msg map[string]any
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}
			switch msg["action"] {
			case "subscribe":
				_ = conn.WriteJSON(map[string]any{"id": msg["id"], "result": "true"})
				_ = conn.WriteJSON(map[string]any{
					"T": "ticker.SPOT",
					"P": 1234567890,
					"d": map[string]string{"price": "1.23"},
				})
			}
			if msg["type"] == "ping" {
				_ = conn.WriteJSON(map[string]any{"id": msg["id"], "type": "pong", "ts": 1})
			}
		}
	})
	defer server.Close()

	client := NewClient(wsURL(server.URL), "", WithAutoReconnect(false))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	pushes, err := client.Subscribe("ticker", "SPOT", "BTC-USDT")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case push := <-pushes:
		if push.T != "ticker.SPOT" {
			t.Errorf("unexpected push: %+v", push)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for push")
	}

	if err := client.Unsubscribe("ticker", "SPOT", "BTC-USDT"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
}

func TestClient_ConnectTimesOutWithoutWelcome(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		time.Sleep(2 * time.Second)
	}))
	defer server.Close()

	client := NewClient(wsURL(server.URL), "", WithAutoReconnect(false))
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	if err := client.Connect(ctx); err == nil {
		t.Fatal("expected Connect to fail without a welcome message")
	}
}

func TestClient_Close(t *testing.T) {
	server := fakeServer(t, func(conn *websocket.Conn) {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer server.Close()

	client := NewClient(wsURL(server.URL), "", WithAutoReconnect(false))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close should be a no-op, got: %v", err)
	}
}

func TestClient_ConnectEscapesTokenAndPreservesHostQuery(t *testing.T) {
	connected := make(chan *http.Request, 1)
	server := fakeServer(t, func(conn *websocket.Conn) {})
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connected <- r
		conn, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			defer conn.Close()
			_ = conn.WriteJSON(map[string]any{"sessionId": "sess-1", "message": "welcome"})
		}
	})
	defer server.Close()

	client := NewClient(wsURL(server.URL)+"?region=eu", "a+b&c", WithAutoReconnect(false))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	select {
	case request := <-connected:
		if request.URL.Query().Get("region") != "eu" || request.URL.Query().Get("token") != "a+b&c" {
			t.Errorf("unexpected query: %s", request.URL.RawQuery)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not receive connection")
	}
}

func TestClient_WriteJSONIsSafeForConcurrentCalls(t *testing.T) {
	server := fakeServer(t, func(conn *websocket.Conn) {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer server.Close()

	client := NewClient(wsURL(server.URL), "", WithAutoReconnect(false))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	const writers = 32
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- client.writeJSON(map[string]string{"type": "ping"})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("writeJSON: %v", err)
		}
	}
}
