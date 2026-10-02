package uta

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Ticker is a typed UTA WebSocket ticker update. Numeric exchange values are
// retained as strings to avoid precision loss. GatewayTimestamp is the P
// envelope field; MatchingEngineTime is the M payload field.
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

// DecodeTicker decodes a ticker.SPOT or ticker.FUTURES Push. It is useful
// when an application deliberately uses Subscribe's raw channel for generic
// dispatch; ordinary users should prefer SubscribeTicker.
func DecodeTicker(push Push) (Ticker, error) {
	const prefix = "ticker."
	if !strings.HasPrefix(push.T, prefix) {
		return Ticker{}, fmt.Errorf("kucoin: uta ws: expected %s push, got %q", prefix, push.T)
	}
	var ticker Ticker
	if err := json.Unmarshal(push.Data, &ticker); err != nil {
		return Ticker{}, fmt.Errorf("kucoin: uta ws: decode ticker payload: %w", err)
	}
	ticker.TradeType = strings.TrimPrefix(push.T, prefix)
	ticker.GatewayTimestamp = push.P
	return ticker, nil
}

// SubscribeTicker subscribes to the documented ticker channel and delivers
// typed updates; callers never need to decode JSON manually. The subscription
// remains active until Unsubscribe("ticker", tradeType, symbol) or Close.
//
// A Client has one logical subscription per channel/tradeType/symbol tuple.
// Do not mix SubscribeTicker and Subscribe for the same tuple unless both raw
// and typed streams are intentionally consumed.
func (c *Client) SubscribeTicker(tradeType, symbol string) (<-chan Ticker, error) {
	key := "ticker:" + tradeType + ":" + symbol
	c.mu.Lock()
	sub, exists := c.subscriptions[key]
	if exists {
		if sub.tickerCh == nil {
			sub.tickerCh = make(chan Ticker, subBufferSize)
		}
		tickers := sub.tickerCh
		c.mu.Unlock()
		return tickers, nil
	}
	sub = &subscription{
		channel:   "ticker",
		tradeType: tradeType,
		symbol:    symbol,
		ch:        make(chan Push, subBufferSize),
	}
	c.subscriptions[key] = sub
	sub.tickerCh = make(chan Ticker, subBufferSize)
	tickers := sub.tickerCh
	id := randomID()
	ack := make(chan bool, 1)
	c.ackWaiters[id] = ack
	done := c.done
	c.mu.Unlock()
	cleanup := func() {
		c.mu.Lock()
		if c.ackWaiters[id] == ack {
			delete(c.ackWaiters, id)
		}
		if c.subscriptions[key] == sub {
			delete(c.subscriptions, key)
			close(sub.ch)
			close(sub.tickerCh)
		}
		c.mu.Unlock()
	}

	req := map[string]any{
		"id":        id,
		"action":    "subscribe",
		"channel":   "ticker",
		"tradeType": tradeType,
	}
	if symbol != "" {
		req["symbol"] = symbol
	}
	if err := c.writeJSON(req); err != nil {
		cleanup()
		return nil, err
	}

	select {
	case ok := <-ack:
		if !ok {
			cleanup()
			return nil, ErrSubscriptionFailed
		}
		return tickers, nil
	case <-time.After(welcomeWaitTimeout):
		cleanup()
		return nil, fmt.Errorf("kucoin: uta ws: subscribe to ticker timed out waiting for acknowledgement")
	case <-done:
		cleanup()
		return nil, context.Canceled
	}
}
