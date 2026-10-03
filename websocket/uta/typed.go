package uta

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tigusigalpa/kucoin-go/stream"
)

// Ticker is a typed UTA WebSocket ticker update. Numeric exchange values are
// retained as strings to avoid precision loss. GatewayTimestamp is the P
// envelope field; MatchingEngineTime is the M payload field.
//
// It is kept for source compatibility. The uta/v2/streaming package offers a
// richer ticker type with exact-decimal fields.
//
// Docs: https://www.kucoin.com/docs-new/3470355w0
type Ticker struct {
	TradeType          string
	GatewayTimestamp   int64
	Symbol             string `json:"s"`
	BestAskPrice       string `json:"a"`
	BestAskSize        string `json:"A"`
	BestBidPrice       string `json:"b"`
	BestBidSize        string `json:"B"`
	LastPrice          string `json:"l"`
	LastSize           string `json:"q"`
	Side               string `json:"S"`
	Sequence           int64  `json:"E"`
	MatchingEngineTime int64  `json:"M"`
}

// DecodeTicker decodes a ticker.SPOT or ticker.FUTURES Push. It is useful when
// an application deliberately uses Subscribe's raw channel for generic
// dispatch; ordinary users should prefer SubscribeTicker.
func DecodeTicker(push Push) (Ticker, error) {
	const prefix = "ticker."
	if !strings.HasPrefix(strings.ToLower(push.T), prefix) {
		return Ticker{}, fmt.Errorf("kucoin: uta ws: expected %s push, got %q", prefix, push.T)
	}
	var ticker Ticker
	if err := json.Unmarshal(push.Data, &ticker); err != nil {
		return Ticker{}, fmt.Errorf("kucoin: uta ws: decode ticker payload: %w", err)
	}
	ticker.TradeType = push.T[len(prefix):]
	ticker.GatewayTimestamp = push.P
	return ticker, nil
}

// SubscribeTicker subscribes to the documented ticker channel and delivers typed
// updates; callers never need to decode JSON manually. Only updates for symbol
// are delivered. The subscription remains active until
// Unsubscribe("ticker", tradeType, symbol) or Close.
//
// Subscribing the same ticker twice returns the existing channel, also when
// several goroutines do it at the same time. For exact-decimal fields and
// multi-symbol subscriptions use uta/v2/streaming.
func (c *Client) SubscribeTicker(tradeType, symbol string) (<-chan Ticker, error) {
	spec := SubscribeSpec{Channel: "ticker", TradeType: tradeType}
	if symbol != "" {
		spec.Symbols = []string{symbol}
	}
	sub, err := c.ticker.Get(context.Background(), spec.Name(), func() (*stream.Subscription[Ticker], error) {
		return SubscribeTyped(context.Background(), c, spec, func(p *Push) (Ticker, bool, error) {
			t, err := DecodeTicker(*p)
			return t, err == nil, err
		})
	})
	if err != nil {
		return nil, err
	}
	return sub.C(), nil
}
