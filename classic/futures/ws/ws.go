// Package ws implements KuCoin Classic Futures' WebSocket
// token-acquisition endpoints. These are REST calls — the actual socket
// connection is built with websocket/classic.NewClient using the token
// returned here.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/get-public-token-futures
package ws

import (
	"context"
	"net/http"

	classicws "github.com/tigusigalpa/kucoin-go/classic/ws"
	"github.com/tigusigalpa/kucoin-go/transport"
)

// Client fetches Classic Futures WebSocket connection tokens.
type Client struct {
	executor *transport.Executor
}

// NewClient wires a ws.Client to the shared Classic Futures Executor. Not
// normally called directly; use kucoin.NewClient.
func NewClient(executor *transport.Executor) *Client {
	return &Client{executor: executor}
}

// GetPublicToken returns a token for opening a public Futures WebSocket
// connection (ticker, orderbook, etc.). No credentials required.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/get-public-token-futures
func (c *Client) GetPublicToken(ctx context.Context) (*classicws.Token, error) {
	var result classicws.Token
	if _, err := c.executor.DoPublic(ctx, http.MethodPost, "/api/v1/bullet-public", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetPrivateToken returns a token for opening a private Futures
// WebSocket connection (order channels).
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/get-private-token-futures
func (c *Client) GetPrivateToken(ctx context.Context) (*classicws.Token, error) {
	var result classicws.Token
	if _, err := c.executor.Do(ctx, http.MethodPost, "/api/v1/bullet-private", nil, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
