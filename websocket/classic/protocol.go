package classic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	classicws "github.com/tigusigalpa/kucoin-go/classic/ws"
	"github.com/tigusigalpa/kucoin-go/internal/wsengine"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/types"
)

// wire is the superset of every Classic frame shape.
type wire struct {
	ID          types.ID        `json:"id"`
	Type        string          `json:"type"`
	Topic       string          `json:"topic"`
	Subject     string          `json:"subject"`
	Sn          types.Int64     `json:"sn"`
	UserID      types.ID        `json:"userId"`
	ChannelType string          `json:"channelType"`
	Code        types.Int64     `json:"code"`
	Data        json.RawMessage `json:"data"`
}

// protocol is the Classic dialect for the connection engine: token-based
// endpoints, {"type": ...} control frames and topic-routed pushes.
type protocol struct {
	source TokenSource
	// static endpoint and token, used when source is nil.
	endpoint string
	token    string
	servers  atomic.Uint64 // round-robin over the instance servers of a token
}

func (p *protocol) Name() string { return "classic" }

// MessageLimit implements wsengine.MessageLimiter: KuCoin allows a Classic
// connection 100 client messages per 10 seconds.
//
// Docs: https://www.kucoin.com/docs-new/rate-limit-rule-classic
func (p *protocol) MessageLimit() (int, time.Duration) { return 100, 10 * time.Second }

var _ wsengine.MessageLimiter = (*protocol)(nil)

// Endpoint builds the URL of one connection attempt. With a TokenSource it
// asks for a fresh token every time — KuCoin tokens expire after 24 hours and
// the endpoint list may change — and rotates through the instance servers the
// token response lists, so repeated failures try the next one.
func (p *protocol) Endpoint(ctx context.Context) (wsengine.Endpoint, error) {
	endpoint, token := p.endpoint, p.token
	var pingInterval, pingTimeout time.Duration
	if p.source != nil {
		tok, err := p.source.GetToken(ctx)
		if err != nil {
			return wsengine.Endpoint{}, err
		}
		if tok == nil || tok.Token == "" || len(tok.InstanceServers) == 0 {
			return wsengine.Endpoint{}, errors.New("kucoin: classic ws: token response has no token or instance server")
		}
		server := tok.InstanceServers[int(p.servers.Add(1)-1)%len(tok.InstanceServers)]
		endpoint, token = server.Endpoint, tok.Token
		pingInterval = time.Duration(server.PingInterval) * time.Millisecond
		pingTimeout = time.Duration(server.PingTimeout) * time.Millisecond
	}
	u, err := url.Parse(endpoint)
	if err == nil && u.Host == "" {
		err = errors.New("no host")
	}
	if err != nil {
		return wsengine.Endpoint{}, stream.Permanent(fmt.Errorf("kucoin: classic ws: invalid endpoint %q: %w", endpoint, err))
	}
	q := u.Query()
	q.Set("token", token)
	q.Set("connectId", newConnectID())
	u.RawQuery = q.Encode()
	return wsengine.Endpoint{URL: u.String(), PingInterval: pingInterval, PingTimeout: pingTimeout}, nil
}

func newConnectID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Classify interprets one inbound frame.
func (p *protocol) Classify(raw []byte) wsengine.Inbound {
	var w wire
	if err := json.Unmarshal(raw, &w); err != nil {
		return wsengine.Inbound{}
	}
	switch w.Type {
	case "welcome":
		return wsengine.Inbound{Kind: wsengine.KindWelcome, ID: string(w.ID)}
	case "pong":
		return wsengine.Inbound{Kind: wsengine.KindPong, ID: string(w.ID)}
	case "ack":
		return wsengine.Inbound{Kind: wsengine.KindAck, ID: string(w.ID)}
	case "error":
		return wsengine.Inbound{Kind: wsengine.KindError, ID: string(w.ID), Err: &stream.ServerError{
			Code:    int(w.Code),
			Message: dataText(w.Data),
			ID:      string(w.ID),
		}}
	}
	if w.Topic == "" {
		return wsengine.Inbound{}
	}
	return wsengine.Inbound{
		Kind:  wsengine.KindPush,
		Route: w.Topic,
		Msg: &Message{
			ID:          w.ID,
			Type:        w.Type,
			Topic:       w.Topic,
			Subject:     w.Subject,
			Sn:          w.Sn,
			UserID:      string(w.UserID),
			ChannelType: w.ChannelType,
			Data:        w.Data,
		},
	}
}

// dataText extracts the human-readable text of an error frame, whose data is
// a JSON string.
func dataText(data json.RawMessage) string {
	var s string
	if json.Unmarshal(data, &s) == nil {
		return s
	}
	return strings.TrimSpace(string(data))
}

func (p *protocol) Ping(id string) []byte {
	return []byte(`{"id":"` + id + `","type":"ping"}`)
}

// Welcomed is a no-op: Classic authenticates through the token in the URL.
func (p *protocol) Welcomed(context.Context, wsengine.Requester) error { return nil }

// routesFor returns the routing keys a subscription to topic receives. KuCoin
// accepts several symbols in one subscription ("/market/ticker:BTC-USDT,ETH-USDT")
// but pushes each update with the topic of a single symbol, so the topic is
// expanded to one key per symbol.
func routesFor(topic string) []string {
	i := strings.IndexByte(topic, ':')
	if i < 0 {
		return []string{topic}
	}
	prefix, list := topic[:i+1], topic[i+1:]
	parts := strings.Split(list, ",")
	routes := make([]string, 0, len(parts)+1)
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			routes = append(routes, prefix+part)
		}
	}
	if len(parts) > 1 {
		routes = append(routes, topic) // defensive: the combined form, should KuCoin ever echo it
	}
	if len(routes) == 0 {
		return []string{topic}
	}
	return routes
}

type frameOut struct {
	ID             string `json:"id"`
	Type           string `json:"type"`
	Topic          string `json:"topic"`
	PrivateChannel bool   `json:"privateChannel"`
	Response       bool   `json:"response"`
}

func subscribeFrame(topic string, private bool) func(id string) []byte {
	return func(id string) []byte {
		b, _ := json.Marshal(frameOut{ID: id, Type: "subscribe", Topic: topic, PrivateChannel: private, Response: true})
		return b
	}
}

func unsubscribeFrame(topic string, private bool) func(id string) []byte {
	return func(id string) []byte {
		b, _ := json.Marshal(frameOut{ID: id, Type: "unsubscribe", Topic: topic, PrivateChannel: private, Response: true})
		return b
	}
}

// Token re-exports the REST token response type for callers of this package.
type Token = classicws.Token
