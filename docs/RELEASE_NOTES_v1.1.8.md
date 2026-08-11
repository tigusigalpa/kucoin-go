# kucoin-go v1.1.8

## Fixed

- Serialized WebSocket writes in both the Classic and UTA clients. This
  prevents concurrent writes from causing `gorilla/websocket` panics when
  subscriptions, unsubscriptions, and heartbeat pings occur at the same
  time.
- Switched WebSocket connection URL construction to `net/url`. Tokens are
  now safely escaped, and query parameters already present in an endpoint
  are preserved for both Classic and UTA connections.
- Updated the REST executor to accept successful empty responses, including
  `204 No Content`, without attempting to decode a KuCoin response envelope.

## Validation

- Added regression coverage for escaped WebSocket tokens, preserved endpoint
  query parameters, and successful `204 No Content` REST responses.
- Verified with `go test ./...` and `go vet ./...`.
