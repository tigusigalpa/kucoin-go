// Package wstest provides the test support shared by the WebSocket packages of
// this module: a scriptable WebSocket server that stands in for KuCoin, and a
// goroutine-leak checker. It is internal and only meant for tests.
package wstest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// Server is a scriptable WebSocket server.
type Server struct {
	t   testing.TB
	srv *httptest.Server

	mu       sync.Mutex
	handler  func(*Conn)
	conns    []*Conn
	rejects  int
	rejectSt int
	accepted atomic.Int32
}

// NewServer starts a server whose handler runs once per accepted connection
// (after the WebSocket upgrade) and closes the connection when it returns. The
// server is closed with t.Cleanup.
func NewServer(t testing.TB, handler func(c *Conn)) *Server {
	t.Helper()
	s := &Server{t: t, handler: handler}
	s.srv = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.Close)
	return s
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.rejects > 0 {
		s.rejects--
		status := s.rejectSt
		s.mu.Unlock()
		http.Error(w, http.StatusText(status), status)
		return
	}
	handler := s.handler
	s.mu.Unlock()

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &Conn{ws: ws, Index: int(s.accepted.Add(1)), Request: r, done: make(chan struct{})}
	s.mu.Lock()
	s.conns = append(s.conns, c)
	s.mu.Unlock()
	defer func() {
		_ = ws.Close()
		close(c.done)
	}()
	if handler != nil {
		handler(c)
	}
}

// URL returns the server's ws:// URL.
func (s *Server) URL() string { return "ws" + strings.TrimPrefix(s.srv.URL, "http") }

// HTTPURL returns the server's http:// URL.
func (s *Server) HTTPURL() string { return s.srv.URL }

// Connections returns how many WebSocket connections were accepted so far.
func (s *Server) Connections() int { return int(s.accepted.Load()) }

// SetHandler replaces the handler used for connections accepted from now on.
func (s *Server) SetHandler(h func(*Conn)) {
	s.mu.Lock()
	s.handler = h
	s.mu.Unlock()
}

// RejectUpgrades makes the next n connection attempts fail with the given HTTP
// status instead of upgrading.
func (s *Server) RejectUpgrades(n, status int) {
	s.mu.Lock()
	s.rejects, s.rejectSt = n, status
	s.mu.Unlock()
}

// Conn returns the n-th (1-based) accepted connection, or nil.
func (s *Server) Conn(n int) *Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n < 1 || n > len(s.conns) {
		return nil
	}
	return s.conns[n-1]
}

// DropAll abruptly closes every open connection without a close handshake, the
// way a crashed peer or a load balancer would.
func (s *Server) DropAll() {
	s.mu.Lock()
	conns := append([]*Conn(nil), s.conns...)
	s.mu.Unlock()
	for _, c := range conns {
		c.Drop()
	}
}

// Close shuts the server down and waits for its handlers.
func (s *Server) Close() {
	s.DropAll()
	s.srv.Close()
}

// Conn is one server-side WebSocket connection.
type Conn struct {
	ws *websocket.Conn
	// Index is the 1-based order in which the server accepted the connection.
	Index int
	// Request is the upgrade request (its URL carries the query parameters).
	Request *http.Request

	wmu  sync.Mutex
	done chan struct{}
}

// Done is closed when the connection's handler has returned.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Send writes v as a JSON text frame. It is safe for concurrent use.
func (c *Conn) Send(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.SendText(string(b))
}

// SendText writes a text frame. It is safe for concurrent use.
func (c *Conn) SendText(s string) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_ = c.ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return c.ws.WriteMessage(websocket.TextMessage, []byte(s))
}

// ReadText reads the next text frame.
func (c *Conn) ReadText() (string, error) {
	for {
		mt, b, err := c.ws.ReadMessage()
		if err != nil {
			return "", err
		}
		if mt == websocket.TextMessage {
			return string(b), nil
		}
	}
}

// ReadJSON reads the next frame into a generic map.
func (c *Conn) ReadJSON() (map[string]any, error) {
	text, err := c.ReadText()
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(text), &m); err != nil {
		return nil, err
	}
	return m, nil
}

// Drop closes the underlying TCP connection without a WebSocket close frame.
func (c *Conn) Drop() {
	_ = c.ws.UnderlyingConn().Close()
}

// CloseNormal sends a normal-closure close frame and closes the connection.
func (c *Conn) CloseNormal() {
	c.wmu.Lock()
	_ = c.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"), time.Now().Add(time.Second))
	c.wmu.Unlock()
	_ = c.ws.Close()
}

// Str reads a string field of a decoded frame ("" when absent or not a string).
func Str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

// CheckLeaks fails the test if goroutines started by this module are still
// running after the test and its cleanups finished. Call it first in a test so
// that its cleanup runs last, after servers and clients were closed.
func CheckLeaks(t testing.TB) {
	t.Helper()
	before := moduleGoroutines()
	t.Cleanup(func() {
		deadline := time.Now().Add(5 * time.Second)
		for {
			var leaked []string
			for id, stack := range moduleGoroutines() {
				if _, existed := before[id]; !existed {
					leaked = append(leaked, stack)
				}
			}
			if len(leaked) == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("%d goroutine(s) leaked:\n\n%s", len(leaked), strings.Join(leaked, "\n\n"))
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
}

// moduleGoroutines returns the stacks of the goroutines whose stack mentions this
// module, keyed by goroutine ID. Goroutines of the testing framework itself are
// excluded.
func moduleGoroutines() map[string]string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	out := make(map[string]string)
	for _, g := range strings.Split(string(buf), "\n\n") {
		if !strings.Contains(g, "github.com/tigusigalpa/kucoin-go/") {
			continue
		}
		if strings.Contains(g, "testing.tRunner") || strings.Contains(g, "testing.(*T).Run") || strings.Contains(g, "testing.runTests") || strings.Contains(g, "wstest.moduleGoroutines") {
			continue
		}
		header, _, _ := strings.Cut(g, "\n")
		id := strings.Fields(header)
		if len(id) < 2 {
			continue
		}
		out[id[1]] = g
	}
	return out
}
