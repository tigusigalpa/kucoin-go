# kucoin-go v1.1.9

## Added

- Added a GitHub Actions CI workflow for pull requests and pushes to `main`.
  It runs on Go 1.22.x and the current stable Go release.
- CI now verifies formatting, module dependency consistency, static analysis,
  race-detector test coverage, and generated endpoint documentation.
- Added REST transport tests for query-parameter encoding and retrying GET
  requests after a rate-limit (`429`) response.
- Added concurrent-write regression tests for both Classic and UTA WebSocket
  clients.

## Changed

- Declared `github.com/gorilla/websocket` as a direct module dependency.

## Validation

- Verified locally with `go test ./...`, `go vet ./...`, module tidy checks,
  and endpoint documentation generation.
