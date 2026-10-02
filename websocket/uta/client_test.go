package uta

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tigusigalpa/kucoin-go/transport"
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

func TestClient_ConnectAuthenticatesWithCredentials(t *testing.T) {
	authenticated := make(chan map[string]any, 1)
	server := fakeServer(t, func(conn *websocket.Conn) {
		var request map[string]any
		if err := conn.ReadJSON(&request); err != nil {
			return
		}
		if request["op"] != "auth" {
			t.Errorf("op = %v, want auth", request["op"])
			return
		}
		if request["kc-api-key"] != "test-key" || request["kc-api-sign"] == "" || request["kc-api-passphrase"] == "" || request["kc-api-timestamp"] == "" {
			t.Errorf("incomplete auth request: %#v", request)
			return
		}
		authenticated <- request
		_ = conn.WriteJSON(map[string]any{"id": request["id"], "result": true})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer server.Close()

	client := NewClient(wsURL(server.URL), "", WithAutoReconnect(false), WithCredentials(transport.Credentials{
		APIKey:        "test-key",
		APISecret:     "test-secret",
		APIPassphrase: "test-passphrase",
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()
	select {
	case <-authenticated:
	case <-time.After(time.Second):
		t.Fatal("server did not receive auth request")
	}
}

func TestClient_ConnectReportsAuthenticationFailure(t *testing.T) {
	server := fakeServer(t, func(conn *websocket.Conn) {
		var request map[string]any
		if err := conn.ReadJSON(&request); err == nil && request["op"] == "auth" {
			_ = conn.WriteJSON(map[string]any{"id": request["id"], "result": false})
		}
	})
	defer server.Close()

	client := NewClient(wsURL(server.URL), "", WithAutoReconnect(false), WithCredentials(transport.Credentials{
		APIKey:        "test-key",
		APISecret:     "test-secret",
		APIPassphrase: "test-passphrase",
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); !errors.Is(err, ErrAuthenticationFailed) {
		t.Fatalf("Connect error = %v, want ErrAuthenticationFailed", err)
	}
}

func TestClient_ConnectRejectsIncompleteCredentials(t *testing.T) {
	server := fakeServer(t, func(conn *websocket.Conn) {
		_, _, _ = conn.ReadMessage()
	})
	defer server.Close()

	client := NewClient(wsURL(server.URL), "", WithAutoReconnect(false), WithCredentials(transport.Credentials{APIKey: "test-key"}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); !errors.Is(err, ErrIncompleteCredentials) {
		t.Fatalf("Connect error = %v, want ErrIncompleteCredentials", err)
	}
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
			if msg["op"] == "ping" {
				_ = conn.WriteJSON(map[string]any{"id": msg["id"], "op": "pong", "ts": 1})
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
	client.mu.RLock()
	conn := client.conn
	done := client.done
	client.mu.RUnlock()
	if conn != nil {
		t.Fatal("connection remained active after welcome timeout")
	}
	select {
	case <-done:
	default:
		t.Fatal("connection worker was not stopped after welcome timeout")
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

func TestClient_CloseClosesSubscriptionChannels(t *testing.T) {
	server := fakeServer(t, func(conn *websocket.Conn) {
		for {
			var message map[string]any
			if err := conn.ReadJSON(&message); err != nil {
				return
			}
			if message["action"] == "subscribe" {
				_ = conn.WriteJSON(map[string]any{"id": message["id"], "result": "true"})
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
	pushes, err := client.Subscribe("ticker", "SPOT", "BTC-USDT")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case _, ok := <-pushes:
		if ok {
			t.Fatal("subscription channel is still open after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription channel was not closed")
	}
}

func TestClient_SubscribeTickerDeliversTypedPush(t *testing.T) {
	server := fakeServer(t, func(conn *websocket.Conn) {
		for {
			var message map[string]any
			if err := conn.ReadJSON(&message); err != nil {
				return
			}
			if message["action"] == "subscribe" {
				_ = conn.WriteJSON(map[string]any{"id": message["id"], "result": "true"})
				_ = conn.WriteJSON(map[string]any{
					"T": "ticker.SPOT",
					"P": 1768206966101166007,
					"d": map[string]any{
						"s": "BTC-USDT", "a": "90968.2", "A": "0.97675941",
						"b": "90968.1", "B": "0.02052839", "l": "90968.2",
						"q": "0.00109929", "S": "BUY", "E": 25958853459,
						"M": 1768206966096000000,
					},
				})
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

	tickers, err := client.SubscribeTicker("SPOT", "BTC-USDT")
	if err != nil {
		t.Fatalf("SubscribeTicker: %v", err)
	}
	select {
	case ticker := <-tickers:
		if ticker.TradeType != "SPOT" || ticker.Symbol != "BTC-USDT" || ticker.LastPrice != "90968.2" || ticker.GatewayTimestamp != 1768206966101166007 {
			t.Fatalf("unexpected ticker: %+v", ticker)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for typed ticker")
	}
}

func TestClient_ReconnectsAndResubscribes(t *testing.T) {
	var connections int
	var mu sync.Mutex
	resubscribed := make(chan struct{})
	var once sync.Once
	server := fakeServer(t, func(conn *websocket.Conn) {
		mu.Lock()
		connections++
		connection := connections
		mu.Unlock()
		for {
			var message map[string]any
			if err := conn.ReadJSON(&message); err != nil {
				return
			}
			if message["action"] != "subscribe" {
				continue
			}
			if connection == 1 {
				_ = conn.WriteJSON(map[string]any{"id": message["id"], "result": "true"})
				_ = conn.Close()
				return
			}
			once.Do(func() { close(resubscribed) })
		}
	})
	defer server.Close()

	client := NewClient(wsURL(server.URL), "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()
	if _, err := client.Subscribe("ticker", "SPOT", "BTC-USDT"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	select {
	case <-resubscribed:
	case <-time.After(4 * time.Second):
		t.Fatal("client did not resubscribe after an unexpected disconnect")
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

func TestClient_HandlesControlMessagesAndResubscribeWithoutConnection(t *testing.T) {
	client := NewClient("ws://example.test", "token", WithLogger(noopLogger{}))
	noopLogger{}.Debug("debug")
	noopLogger{}.Info("info")
	noopLogger{}.Warn("warn")
	noopLogger{}.Error("error")

	client.closed = true
	if !client.isClosed() {
		t.Fatal("isClosed = false, want true")
	}
	client.reconnectLoop()
	client.closed = false

	client.welcomed = make(chan struct{})
	client.handleMessage([]byte(`{"message":"welcome"}`))
	select {
	case <-client.welcomed:
	default:
		t.Fatal("welcome did not unblock Connect")
	}

	ack := make(chan bool, 1)
	client.ackWaiters["ack-1"] = ack
	client.handleMessage([]byte(`{"id":"ack-1","result":"true"}`))
	select {
	case ok := <-ack:
		if !ok {
			t.Fatal("ack result = false, want true")
		}
	default:
		t.Fatal("ack waiter was not notified")
	}
	if _, ok := client.ackWaiters["ack-1"]; ok {
		t.Fatal("ack waiter was not removed")
	}

	sub := &subscription{channel: "ticker", tradeType: "SPOT", ch: make(chan Push, 1)}
	client.subscriptions["ticker:SPOT:"] = sub
	client.handleMessage([]byte(`{"T":"ticker.SPOT","d":{"price":"1"}}`))
	select {
	case push := <-sub.ch:
		if push.T != "ticker.SPOT" {
			t.Fatalf("push type = %q, want ticker.SPOT", push.T)
		}
	default:
		t.Fatal("push was not dispatched")
	}

	client.handleMessage([]byte(`not json`))
	client.handleMessage([]byte(`{"type":"pong"}`))
	client.subscriptions["ticker:FUTURES:"] = &subscription{channel: "ticker", tradeType: "FUTURES", ch: make(chan Push, 1)}
	sub.ch <- Push{}
	client.handleMessage([]byte(`{"T":"ticker.SPOT","d":{}}`))
	client.resubscribeAll()
}

func TestClient_SubscribeReportsNegativeAcknowledgement(t *testing.T) {
	server := fakeServer(t, func(conn *websocket.Conn) {
		var message map[string]any
		if err := conn.ReadJSON(&message); err == nil && message["action"] == "subscribe" {
			_ = conn.WriteJSON(map[string]any{"id": message["id"], "result": false})
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
	if _, err := client.Subscribe("ticker", "SPOT", "BTC-USDT"); !errors.Is(err, ErrSubscriptionFailed) {
		t.Fatalf("Subscribe error = %v, want ErrSubscriptionFailed", err)
	}
}

func TestClient_IgnoresFramesFromSupersededConnection(t *testing.T) {
	client := NewClient("ws://example.test", "")
	active := &websocket.Conn{}
	stale := &websocket.Conn{}
	client.conn = active
	client.done = make(chan struct{})
	client.welcomed = make(chan struct{})

	client.handleMessageForConnection(stale, make(chan struct{}), []byte(`{"message":"welcome"}`))
	select {
	case <-client.welcomed:
		t.Fatal("a stale connection completed the active welcome handshake")
	default:
	}
}
