package classic

import (
	"context"
	"encoding/json"
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

// fakeServer simulates just enough of KuCoin's Classic WS wire protocol to
// exercise Client's welcome/subscribe/ack/push/ping-pong/unsubscribe flow.
func fakeServer(t *testing.T, handle func(conn *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()

		if err := conn.WriteJSON(map[string]string{"id": "welcome-id", "type": "welcome"}); err != nil {
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
			switch msg["type"] {
			case "subscribe":
				_ = conn.WriteJSON(map[string]string{"id": msg["id"].(string), "type": "ack"})
				_ = conn.WriteJSON(map[string]any{
					"type":    "message",
					"topic":   msg["topic"],
					"subject": "trade.ticker",
					"data":    json.RawMessage(`{"price":"1.23"}`),
				})
			case "ping":
				_ = conn.WriteJSON(map[string]string{"id": msg["id"].(string), "type": "pong"})
			case "unsubscribe":
				// no response needed
			}
		}
	})
	defer server.Close()

	client := NewClient(wsURL(server.URL), "test-token", WithAutoReconnect(false))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	msgs, err := client.Subscribe(ctx, "/market/ticker:BTC-USDT", false)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	select {
	case msg := <-msgs:
		if msg.Topic != "/market/ticker:BTC-USDT" || string(msg.Data) != `{"price":"1.23"}` {
			t.Errorf("unexpected message: %+v", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for push")
	}

	if err := client.Unsubscribe("/market/ticker:BTC-USDT"); err != nil {
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

	client := NewClient(wsURL(server.URL), "test-token", WithAutoReconnect(false))
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

	client := NewClient(wsURL(server.URL), "test-token", WithAutoReconnect(false))
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
			if message["type"] == "subscribe" {
				_ = conn.WriteJSON(map[string]string{"id": message["id"].(string), "type": "ack"})
			}
		}
	})
	defer server.Close()

	client := NewClient(wsURL(server.URL), "test-token", WithAutoReconnect(false))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	msgs, err := client.Subscribe(ctx, "/market/ticker:BTC-USDT", false)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case _, ok := <-msgs:
		if ok {
			t.Fatal("subscription channel is still open after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription channel was not closed")
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
			if message["type"] != "subscribe" {
				continue
			}
			if connection == 1 {
				_ = conn.WriteJSON(map[string]string{"id": message["id"].(string), "type": "ack"})
				_ = conn.Close()
				return
			}
			once.Do(func() { close(resubscribed) })
		}
	})
	defer server.Close()

	client := NewClient(wsURL(server.URL), "test-token")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()
	if _, err := client.Subscribe(ctx, "/market/ticker:BTC-USDT", false); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	select {
	case <-resubscribed:
	case <-time.After(4 * time.Second):
		t.Fatal("client did not resubscribe after an unexpected disconnect")
	}
}

func TestClient_ConnectEscapesTokenAndPreservesEndpointQuery(t *testing.T) {
	connected := make(chan *http.Request, 1)
	server := fakeServer(t, func(conn *websocket.Conn) {})
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connected <- r
		conn, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			defer conn.Close()
			_ = conn.WriteJSON(map[string]string{"id": "welcome-id", "type": "welcome"})
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
		if request.URL.Query().Get("region") != "eu" || request.URL.Query().Get("token") != "a+b&c" || request.URL.Query().Get("connectId") == "" {
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

	client := NewClient(wsURL(server.URL), "test-token", WithAutoReconnect(false))
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
	client := NewClient("ws://example.test", "token", WithLogger(noopLogger{}), WithPingInterval(time.Second), WithPingTimeout(time.Second))
	if client.pingInterval != time.Second || client.pingTimeout != time.Second {
		t.Fatal("ping options were not applied")
	}
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
	client.handleMessage([]byte(`{"type":"welcome"}`))
	select {
	case <-client.welcomed:
	default:
		t.Fatal("welcome did not unblock Connect")
	}

	ack := make(chan struct{})
	client.ackWaiters["ack-1"] = ack
	client.handleMessage([]byte(`{"id":"ack-1","type":"ack"}`))
	select {
	case <-ack:
	default:
		t.Fatal("ack waiter was not notified")
	}
	if _, ok := client.ackWaiters["ack-1"]; ok {
		t.Fatal("ack waiter was not removed")
	}

	sub := &subscription{topic: "/market/ticker:BTC-USDT", ch: make(chan Message, 1)}
	client.subscriptions[sub.topic] = sub
	client.handleMessage([]byte(`{"type":"message","topic":"/market/ticker:BTC-USDT","data":{"price":"1"}}`))
	select {
	case message := <-sub.ch:
		if message.Topic != sub.topic {
			t.Fatalf("topic = %q, want %q", message.Topic, sub.topic)
		}
	default:
		t.Fatal("push was not dispatched")
	}

	client.handleMessage([]byte(`not json`))
	client.handleMessage([]byte(`{"type":"pong"}`))
	client.handleMessage([]byte(`{"type":"error"}`))
	client.handleMessage([]byte(`{"type":"message","topic":"/market/unknown","data":{}}`))
	sub.ch <- Message{}
	client.handleMessage([]byte(`{"type":"message","topic":"/market/ticker:BTC-USDT","data":{}}`))
	client.resubscribeAll()
}

func TestClient_IgnoresFramesFromSupersededConnection(t *testing.T) {
	client := NewClient("ws://example.test", "token")
	active := &websocket.Conn{}
	stale := &websocket.Conn{}
	client.conn = active
	client.done = make(chan struct{})
	client.welcomed = make(chan struct{})

	client.handleMessageForConnection(stale, make(chan struct{}), []byte(`{"type":"welcome"}`))
	select {
	case <-client.welcomed:
		t.Fatal("a stale connection completed the active welcome handshake")
	default:
	}
}
