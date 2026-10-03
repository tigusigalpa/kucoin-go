package uta

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/tigusigalpa/kucoin-go/auth"
	"github.com/tigusigalpa/kucoin-go/internal/wsengine"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
	"github.com/tigusigalpa/kucoin-go/types"
)

// authPath is the fixed string KuCoin expects inside the signature of the
// post-welcome authentication frame.
const authPath = "/api/websocket/users/verify"

// wire is the superset of every UTA frame shape. The field tags T/t and P are
// deliberately all declared: encoding/json matches keys case-insensitively as a
// fallback, so declaring both spellings makes each key bind to its own field.
type wire struct {
	ID           types.ID        `json:"id"`
	Op           string          `json:"op"`
	Type         string          `json:"type"` // legacy pong spelling
	Message      string          `json:"message"`
	Reason       string          `json:"reason"`
	Msg          string          `json:"msg"`
	Code         json.RawMessage `json:"code"`
	Result       json.RawMessage `json:"result"`
	SessionID    string          `json:"sessionId"`
	PingInterval types.Int64     `json:"pingInterval"`
	PingTimeout  types.Int64     `json:"pingTimeout"`
	T            string          `json:"T"`
	P            types.Int64     `json:"P"`
	Kind         string          `json:"t"`
	Dp           string          `json:"dp"`
	D            json.RawMessage `json:"d"`
}

// protocol is the UTA v2 dialect for the connection engine: a fixed host, an
// optional legacy token, op/action control frames and pushes routed by their T
// type plus the symbol inside the payload.
type protocol struct {
	host  string
	token string
	creds *transport.Credentials
	clock transport.Clock
}

func (p *protocol) Name() string { return "uta" }

// MessageLimit implements wsengine.MessageLimiter: KuCoin allows a public UTA
// connection 300 client messages per 10 seconds, subscriptions, unsubscriptions
// and pings included, and a private one 100.
//
// Docs: https://www.kucoin.com/docs-new/rate-limit-rule-uta
func (p *protocol) MessageLimit() (int, time.Duration) {
	if p.creds != nil || p.token != "" {
		return 100, 10 * time.Second
	}
	return 300, 10 * time.Second
}

var _ wsengine.MessageLimiter = (*protocol)(nil)

func (p *protocol) Endpoint(context.Context) (wsengine.Endpoint, error) {
	u, err := url.Parse(p.host)
	if err == nil && u.Host == "" {
		err = errors.New("no host")
	}
	if err != nil {
		return wsengine.Endpoint{}, stream.Permanent(fmt.Errorf("kucoin: uta ws: invalid host %q: %w", p.host, err))
	}
	if p.token != "" {
		q := u.Query()
		q.Set("token", p.token)
		u.RawQuery = q.Encode()
	}
	return wsengine.Endpoint{URL: u.String()}, nil
}

func (p *protocol) Ping(id string) []byte {
	return []byte(fmt.Sprintf(`{"id":%q,"op":"ping","timestamp":%q}`, id, strconv.FormatInt(p.now().UnixMilli(), 10)))
}

func (p *protocol) now() time.Time {
	if p.clock != nil {
		return p.clock.Now()
	}
	return time.Now()
}

// Classify interprets one inbound frame.
func (p *protocol) Classify(raw []byte) wsengine.Inbound {
	var w wire
	if err := json.Unmarshal(raw, &w); err != nil {
		return wsengine.Inbound{}
	}
	switch {
	case w.Message == "welcome" && w.T == "":
		return wsengine.Inbound{
			Kind:         wsengine.KindWelcome,
			PingInterval: time.Duration(w.PingInterval) * time.Millisecond,
			PingTimeout:  time.Duration(w.PingTimeout) * time.Millisecond,
		}
	case w.Op == "pong" || w.Type == "pong":
		return wsengine.Inbound{Kind: wsengine.KindPong, ID: string(w.ID)}
	case len(w.Result) > 0 && w.T == "":
		if resultOK(w.Result) {
			return wsengine.Inbound{Kind: wsengine.KindAck, ID: string(w.ID)}
		}
		return wsengine.Inbound{Kind: wsengine.KindNack, ID: string(w.ID), Err: &stream.ServerError{
			ID:      string(w.ID),
			Message: firstNonEmpty(w.Message, w.Reason, w.Msg),
		}}
	case len(w.Code) > 0 && w.T == "":
		return wsengine.Inbound{Kind: wsengine.KindError, ID: string(w.ID), Err: &stream.ServerError{
			ID:      string(w.ID),
			Code:    codeNumber(w.Code),
			Message: firstNonEmpty(w.Msg, w.Message, w.Reason),
		}}
	case w.T != "":
		route, alt := pushRoutes(w.T, w.Dp, w.D)
		return wsengine.Inbound{
			Kind:     wsengine.KindPush,
			Route:    route,
			RouteAlt: alt,
			Msg:      &Push{T: w.T, P: int64(w.P), Kind: w.Kind, Depth: w.Dp, Data: w.D},
		}
	}
	return wsengine.Inbound{}
}

// resultOK reads a reply's result, which the documentation shows both as a JSON
// boolean and as the string "true".
func resultOK(raw json.RawMessage) bool {
	var b bool
	if json.Unmarshal(raw, &b) == nil {
		return b
	}
	var s string
	return json.Unmarshal(raw, &s) == nil && strings.EqualFold(s, "true")
}

// codeNumber reads a gateway error code that may be a string or a number.
func codeNumber(raw json.RawMessage) int {
	var n types.Int64
	if json.Unmarshal(raw, &n) == nil {
		return int(n)
	}
	return 0
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

// symbolProbe extracts the routing fields of a push payload. Both spellings of
// each key are declared on purpose (see wire).
type symbolProbe struct {
	S        string          `json:"s"`
	Side     json.RawMessage `json:"S"`
	Interval string          `json:"i"`
	UpperI   json.RawMessage `json:"I"`
}

// pushRoutes returns the routing key of a push and its wildcard twin. The key
// is the lower-cased push type (KuCoin writes both "ticker.FUTURES" and
// "obu.spot"), the depth for order books, the symbol, and the interval for
// klines.
func pushRoutes(t, depth string, d json.RawMessage) (route, alt string) {
	symbol, interval := "", ""
	if len(d) > 0 && d[0] == '{' {
		var probe symbolProbe
		_ = json.Unmarshal(d, &probe)
		symbol = probe.S
		if strings.HasPrefix(strings.ToLower(t), "kline") {
			interval = probe.Interval
		}
	}
	return routeKey(t, depth, symbol, interval), routeKey(t, depth, "*", interval)
}

func routeKey(pushType, depth, symbol, interval string) string {
	return strings.ToLower(pushType) + "|" + depth + "|" + symbol + "|" + interval
}

// Welcomed authenticates a private connection. A missing key set or a
// rejection by KuCoin is permanent: retrying the same credentials cannot help.
func (p *protocol) Welcomed(ctx context.Context, r wsengine.Requester) error {
	if p.creds == nil {
		return nil
	}
	c := p.creds
	if c.APIKey == "" || c.APISecret == "" || c.APIPassphrase == "" {
		return stream.Permanent(ErrIncompleteCredentials)
	}
	timestamp := auth.TimestampMillis(p.now())
	signer := auth.NewSigner(c.APISecret)
	id := newID()
	frame, _ := json.Marshal(map[string]string{
		"id":                id,
		"op":                "auth",
		"kc-api-key":        c.APIKey,
		"kc-api-sign":       signer.Sign(timestamp, "POST", authPath, ""),
		"kc-api-timestamp":  timestamp,
		"kc-api-passphrase": signer.SignPassphrase(c.APIPassphrase),
	})
	err := r.Request(ctx, id, frame)
	if err == nil {
		return nil
	}
	var se *stream.ServerError
	if errors.As(err, &se) {
		return stream.Permanent(fmt.Errorf("%w: %w", ErrAuthenticationFailed, err))
	}
	return err
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
