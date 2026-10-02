// Package ws preserves the legacy UTA WebSocket token-acquisition endpoint.
// This REST endpoint is listed under KuCoin's abandoned UTA API section.
// Current UTA WebSocket v2 private channels authenticate after welcome with
// websocket/uta.WithCredentials rather than a bullet token.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/get-private-token-uta
package ws

import (
	"context"
	"net/http"

	"github.com/tigusigalpa/kucoin-go/transport"
)

// Token carries the legacy UTA bullet token. New UTA v2 integrations should
// use websocket/uta.WithCredentials instead.
type Token struct {
	Token string `json:"token"`
}

// Client fetches legacy UTA WebSocket connection tokens.
type Client struct {
	executor *transport.Executor
}

// NewClient wires a ws.Client to the shared UTA Executor. Not normally
// called directly; use kucoin.NewClient.
func NewClient(executor *transport.Executor) *Client {
	return &Client{executor: executor}
}

// GetPrivateToken returns a legacy private UTA WebSocket token. Prefer the
// current signed UTA v2 WebSocket authentication flow for new integrations.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/get-private-token-uta
func (c *Client) GetPrivateToken(ctx context.Context) (*Token, error) {
	var result Token
	if _, err := c.executor.Do(ctx, http.MethodPost, "/api/v2/bullet-private", nil, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
