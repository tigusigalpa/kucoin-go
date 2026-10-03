package wstest

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	classicws "github.com/tigusigalpa/kucoin-go/classic/ws"
)

// ClassicFake is a scriptable server for KuCoin's Classic WebSocket dialect
// ({"type":"subscribe"|"ack"|"error"|"message", "topic": ...}).
type ClassicFake struct {
	Server *Server

	mu      sync.Mutex
	frames  []map[string]any
	scripts map[string][]string
	rejects map[string]int
	noAck   map[string]bool
}

// NewClassicFake starts the server (closed with t.Cleanup).
func NewClassicFake(t testing.TB) *ClassicFake {
	t.Helper()
	f := &ClassicFake{scripts: map[string][]string{}, rejects: map[string]int{}, noAck: map[string]bool{}}
	f.Server = NewServer(t, f.serve)
	return f
}

// URL is the WebSocket endpoint.
func (f *ClassicFake) URL() string { return f.Server.URL() }

// Token returns a token response pointing at the server, with a very long ping
// interval so heartbeats stay out of the way.
func (f *ClassicFake) Token() *classicws.Token {
	return &classicws.Token{
		Token: "test-token",
		InstanceServers: []classicws.InstanceServer{
			{Endpoint: f.URL(), Encrypt: true, Protocol: "websocket", PingInterval: 3_600_000, PingTimeout: 10_000},
		},
	}
}

// OnSubscribe makes the server push frames, in order, right after acknowledging
// a subscription to topic (the topic exactly as the client sent it).
func (f *ClassicFake) OnSubscribe(topic string, frames ...string) {
	f.mu.Lock()
	f.scripts[topic] = frames
	f.mu.Unlock()
}

// Reject makes subscriptions to topic fail with an error frame carrying code.
func (f *ClassicFake) Reject(topic string, code int) {
	f.mu.Lock()
	f.rejects[topic] = code
	f.mu.Unlock()
}

// NeverAck makes the server ignore subscriptions to topic.
func (f *ClassicFake) NeverAck(topic string) {
	f.mu.Lock()
	f.noAck[topic] = true
	f.mu.Unlock()
}

// Push sends a frame on the most recent connection.
func (f *ClassicFake) Push(frame string) error {
	c := f.Server.Conn(f.Server.Connections())
	if c == nil {
		return fmt.Errorf("wstest: no connection yet")
	}
	return c.SendText(frame)
}

// Message builds a Classic push frame.
func Message(topic, subject, data string) string {
	return fmt.Sprintf(`{"type":"message","topic":%q,"subject":%q,"data":%s}`, topic, subject, data)
}

// Count returns how many frames of the given type (and topic, when non-empty) the
// server received.
func (f *ClassicFake) Count(typ, topic string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, m := range f.frames {
		if Str(m, "type") == typ && (topic == "" || Str(m, "topic") == topic) {
			n++
		}
	}
	return n
}

// Last returns the most recent received frame of a type, or nil.
func (f *ClassicFake) Last(typ string) map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.frames) - 1; i >= 0; i-- {
		if Str(f.frames[i], "type") == typ {
			return f.frames[i]
		}
	}
	return nil
}

func (f *ClassicFake) serve(c *Conn) {
	_ = c.Send(map[string]any{"id": c.Request.URL.Query().Get("connectId"), "type": "welcome"})
	for {
		m, err := c.ReadJSON()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.frames = append(f.frames, m)
		topic := Str(m, "topic")
		code, rejected := f.rejects[topic]
		script := f.scripts[topic]
		silent := f.noAck[topic]
		f.mu.Unlock()
		id := m["id"]
		switch Str(m, "type") {
		case "ping":
			_ = c.Send(map[string]any{"id": id, "type": "pong", "timestamp": time.Now().UnixMicro()})
		case "subscribe":
			switch {
			case rejected:
				_ = c.Send(map[string]any{"id": id, "type": "error", "code": code, "data": "rejected by the fake"})
			case silent:
			default:
				_ = c.Send(map[string]any{"id": id, "type": "ack"})
				for _, frame := range script {
					_ = c.SendText(frame)
				}
			}
		case "unsubscribe":
			_ = c.Send(map[string]any{"id": id, "type": "ack"})
		}
	}
}

// UTAFake is a scriptable server for KuCoin's UTA v2 WebSocket dialect
// ({"action":"subscribe", "channel": ...} / {"result": true}).
type UTAFake struct {
	Server *Server

	mu      sync.Mutex
	frames  []map[string]any
	scripts map[string][]string
	rejects map[string]string
	noAck   map[string]bool
	onAuth  func(m map[string]any) map[string]any
}

// NewUTAFake starts the server (closed with t.Cleanup).
func NewUTAFake(t testing.TB) *UTAFake {
	t.Helper()
	f := &UTAFake{scripts: map[string][]string{}, rejects: map[string]string{}, noAck: map[string]bool{}}
	f.Server = NewServer(t, f.serve)
	return f
}

// URL is the WebSocket endpoint.
func (f *UTAFake) URL() string { return f.Server.URL() }

// OnSubscribe makes the server push frames after acknowledging a subscription to
// channel. The key is the channel name, optionally narrowed to
// "channel|symbol" or "channel|symbol|depth" or "channel|symbol|interval"; the
// most specific match wins.
func (f *UTAFake) OnSubscribe(key string, frames ...string) {
	f.mu.Lock()
	f.scripts[key] = frames
	f.mu.Unlock()
}

// Reject makes subscriptions matching key fail with the reason.
func (f *UTAFake) Reject(key, reason string) {
	f.mu.Lock()
	f.rejects[key] = reason
	f.mu.Unlock()
}

// NeverAck makes the server ignore subscriptions matching key.
func (f *UTAFake) NeverAck(key string) {
	f.mu.Lock()
	f.noAck[key] = true
	f.mu.Unlock()
}

// OnAuth overrides the reply to the authentication frame (nil keeps the default
// success reply).
func (f *UTAFake) OnAuth(fn func(m map[string]any) map[string]any) {
	f.mu.Lock()
	f.onAuth = fn
	f.mu.Unlock()
}

// Push sends a frame on the most recent connection.
func (f *UTAFake) Push(frame string) error {
	c := f.Server.Conn(f.Server.Connections())
	if c == nil {
		return fmt.Errorf("wstest: no connection yet")
	}
	return c.SendText(frame)
}

// Count returns how many subscribe/unsubscribe frames (action) the server received
// for a channel (all channels when empty).
func (f *UTAFake) Count(action, channel string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, m := range f.frames {
		if strings.EqualFold(Str(m, "action"), action) && (channel == "" || Str(m, "channel") == channel) {
			n++
		}
	}
	return n
}

// Frames returns a copy of every received frame.
func (f *UTAFake) Frames() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.frames...)
}

// LastSubscribe returns the most recent subscribe frame for a channel, or nil.
func (f *UTAFake) LastSubscribe(channel string) map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.frames) - 1; i >= 0; i-- {
		if strings.EqualFold(Str(f.frames[i], "action"), "subscribe") && Str(f.frames[i], "channel") == channel {
			return f.frames[i]
		}
	}
	return nil
}

func keysFor(m map[string]any) []string {
	channel := Str(m, "channel")
	symbols := []string{}
	if s := Str(m, "symbol"); s != "" {
		symbols = append(symbols, s)
	}
	if list, ok := m["symbols"].([]any); ok {
		for _, s := range list {
			if str, ok := s.(string); ok {
				symbols = append(symbols, str)
			}
		}
	}
	var keys []string
	extra := []string{Str(m, "depth"), Str(m, "interval")}
	for _, sym := range symbols {
		for _, e := range extra {
			if e != "" {
				keys = append(keys, channel+"|"+sym+"|"+e)
			}
		}
		keys = append(keys, channel+"|"+sym)
	}
	return append(keys, channel)
}

func (f *UTAFake) serve(c *Conn) {
	_ = c.Send(map[string]any{"sessionId": fmt.Sprintf("sess-%d", c.Index), "message": "welcome", "pingInterval": 3_600_000})
	for {
		m, err := c.ReadJSON()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.frames = append(f.frames, m)
		onAuth := f.onAuth
		var script []string
		var reason string
		var rejected, silent bool
		for _, key := range keysFor(m) {
			if s, ok := f.scripts[key]; ok && script == nil {
				script = s
			}
			if r, ok := f.rejects[key]; ok && !rejected {
				reason, rejected = r, true
			}
			if f.noAck[key] {
				silent = true
			}
		}
		f.mu.Unlock()
		id := m["id"]
		switch {
		case m["op"] == "ping":
			_ = c.Send(map[string]any{"id": id, "op": "pong", "timestamp": time.Now().UnixMilli()})
		case m["op"] == "auth":
			reply := map[string]any{"id": id, "result": true}
			if onAuth != nil {
				reply = onAuth(m)
			}
			if reply != nil {
				_ = c.Send(reply)
			}
		case strings.EqualFold(Str(m, "action"), "subscribe"):
			switch {
			case rejected:
				_ = c.Send(map[string]any{"id": id, "result": false, "reason": reason})
			case silent:
			default:
				_ = c.Send(map[string]any{"id": id, "result": true})
				for _, frame := range script {
					_ = c.SendText(frame)
				}
			}
		case strings.EqualFold(Str(m, "action"), "unsubscribe"):
			_ = c.Send(map[string]any{"id": id, "result": true})
		}
	}
}
