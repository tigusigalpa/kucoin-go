# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this
project follows [SemVer](https://semver.org/).

## [Unreleased]

### Added

- Shared core: `Credentials`, `ClientConfig`/`Option`s, injectable `Clock`
  and `Logger`, conservative GET-only `RetryPolicy` with exponential
  backoff + jitter.
- `auth.Signer`: KC-API-SIGN and KC-API-PASSPHRASE (HMAC-SHA256/Base64),
  with known-answer fixture tests.
- `transport.Executor`: signed/public REST execution, KuCoin response
  envelope decoding, `ResponseMeta` (HTTP status, business code/message,
  request ID, rate-limit headers, `x-in-time`/`x-out-time`), typed error
  hierarchy (`KucoinError` + HTTP-status sentinels).
- `uta/market`: all 10 Phase-1 UTA public market-data endpoints
  (instrument, ticker, orderbook, kline, trade, currencies, currency,
  service-status, announcement, trade-statistics).
- `internal/endpoints.yaml` + `internal/gendocs`: manifest-driven
  `docs/ENDPOINTS.md` generation.

### Fixed

- `UTA.Market.GetOrderBook` now signs the request. A live smoke test
  during development confirmed this endpoint requires credentials (HTTP
  400 `400001` when unauthenticated), unlike every sibling UTA
  market-data endpoint — see `GetOrderBook`'s docblock.

### Known limitations (this checkpoint)

- Only UTA Market is implemented. UTA Account/Orders/Positions/Leverage,
  Classic Spot/Margin/Futures, and all WebSocket channels are not yet
  implemented — see [docs/ENDPOINTS.md](docs/ENDPOINTS.md).
- Business-level KuCoin error codes (per-domain, e.g. Spot/Margin/Futures)
  are not yet mapped to sentinels — only HTTP-status-derived sentinels
  exist so far. The raw code/message is always available via
  `*transport.KucoinError`.
