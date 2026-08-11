// Package ws implements KuCoin UTA's WebSocket token-acquisition
// endpoint. This is a REST call — the actual socket connection is built
// with websocket/uta.NewClient using the token returned here.
//
// The UTA WebSocket API is explicitly documented by KuCoin as
// pre-release/beta ("DO NOT use this API in production environments or
// live trading under any circumstances"), unlike Classic's WebSocket API.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/get-private-token-uta
package ws

import (
	"context"
	"net/http"

	"github.com/tigusigalpa/kucoin-go/transport"
)

// Token carries the bullet token needed to open a private UTA WebSocket
// connection. Unlike Classic's token response, this has no
// instanceServers list — UTA's WebSocket hosts are fixed, documented
// constants (see the websocket/uta package).
type Token struct {
	Token string `json:"token"`
}

// Client fetches UTA WebSocket connection tokens.
type Client struct {
	executor *transport.Executor
}

// NewClient wires a ws.Client to the shared UTA Executor. Not normally
// called directly; use kucoin.NewClient.
func NewClient(executor *transport.Executor) *Client {
	return &Client{executor: executor}
}

// GetPrivateToken returns a token for opening a private UTA WebSocket
// connection (order/balance/position channels).
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/get-private-token-uta
func (c *Client) GetPrivateToken(ctx context.Context) (*Token, error) {
	var result Token
	if _, err := c.executor.Do(ctx, http.MethodPost, "/api/v2/bullet-private", nil, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
