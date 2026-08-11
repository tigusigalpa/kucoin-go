// Package ws holds the token response shape shared by Classic Spot and
// Classic Futures' WebSocket token-acquisition endpoints.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/get-public-token-spot-margin
package ws

// InstanceServer is a single connectable WebSocket endpoint returned by a
// Classic bullet-token call.
type InstanceServer struct {
	Endpoint     string `json:"endpoint"`
	Encrypt      bool   `json:"encrypt"`
	Protocol     string `json:"protocol"`
	PingInterval int    `json:"pingInterval"` // milliseconds
	PingTimeout  int    `json:"pingTimeout"`  // milliseconds
}

// Token carries everything needed to open a Classic WebSocket connection:
// append "?token=" + Token (and, optionally, "&connectId=" + a
// caller-generated ID) to InstanceServers[0].Endpoint.
type Token struct {
	Token           string           `json:"token"`
	InstanceServers []InstanceServer `json:"instanceServers"`
}
