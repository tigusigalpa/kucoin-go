# Architecture

## Layering

```
kucoin-go/                  root package: ClientConfig, Option, Client (UTA/Classic roots)
├── auth/                   HMAC-SHA256 signer — no dependency on anything else in this module
├── transport/              Executor (signed/public HTTP), ResponseMeta, error hierarchy
│                           owns Credentials/Clock/Logger/RetryPolicy — root package re-exports
│                           them as type aliases so callers only import "kucoin"
├── uta/market/              retained UTA v1 market compatibility service
├── uta/account/             retained UTA v1 private-account compatibility service
├── uta/orders/              retained UTA v1 order-management compatibility service
├── uta/positions/           retained UTA v1 position-management compatibility service
├── uta/leverage/            retained UTA v1 leverage compatibility service — split out because all
│                            three require the "Unified" permission, unlike most
│                            read endpoints elsewhere, which need only "General"
├── uta/v2/market/           current UTA v2 Market Data (all 23 documented methods)
├── websocket/classic/       Classic protocol lifecycle and raw message routing
├── websocket/uta/           UTA v2 lifecycle, post-welcome auth and typed ticker routing
├── internal/endpoints.yaml  manifest: one row per implemented method
└── internal/gendocs/        generates docs/ENDPOINTS.md from the manifest
```

Every REST `uta/*` service package follows the identical shape: a `Client` struct
wrapping `*transport.Executor`, a `NewClient(executor)` constructor, and one
method per endpoint. None of them import each other — they're peers under
`uta/`, wired together only in the root `config.go`'s `UTAServices` struct.
This keeps each service independently testable and means adding a new
domain (e.g. a future `uta/v2/transfers`) never risks an import cycle. Current
UTA v2 services are nested at `Client.UTA.V2`; retained v1 services remain at
`Client.UTA.*` solely for source compatibility. Their types are deliberately
not merged because KuCoin has moved those endpoints to its abandoned section.

## Local, no-network validation

A few operations validate obviously-malformed input before making any
HTTP call, returning a local sentinel error instead: `orders.CancelOrder`
and `orders.GetOrderDetails` require `OrderID` or `ClientOid`
(`orders.ErrOrderIDOrClientOidRequired`), and
`orders.BatchCancelOrderByID` additionally rejects more than 20 items
(`orders.ErrTooManyBatchCancelItems`) and validates every item in the
batch up front. This is deliberately shallow — it does not duplicate
KuCoin's business validation (min order size, symbol existence, etc.) —
just enough to catch requests that would obviously fail signing/sending.

## Why transport owns the shared config types

`Credentials`, `Clock`, `Logger`, and `RetryPolicy` are defined in package
`transport`, not the root package, even though users write
`kucoin.Credentials{...}`. The root package re-exports them as type
aliases (`type Credentials = transport.Credentials`). This avoids an
import cycle: `transport.Executor` needs these types, and service packages
like `uta/market` depend on `transport.Executor`, not on the root package
— so the root package can freely import both `transport` and every
service package without anything importing back up to root. Domain
services (`uta/market`, and future `uta/account`, `classic/spot`, etc.)
only ever depend on `transport`, never on each other or on root.

## Request lifecycle (transport.Executor)

1. Caller invokes a typed service method (e.g. `market.Client.GetTicker`).
2. The service builds a `map[string]string` query (GET) or a typed request
   struct (POST body) and calls `Executor.DoPublic` or `Executor.Do`.
   Endpoints requiring repeated query keys use `Executor.DoPublicValues` so
   the exact `url.Values` representation survives onto the wire.
3. `Executor.Do` returns `ErrCredentialsRequired` **locally, with no
   network call**, if no credentials are configured.
4. The query string is built once via `url.Values.Encode()` (which sorts
   keys) and reused byte-for-byte both in the signature and on the wire —
   this is required because KuCoin's signature covers the literal
   query string it receives.
5. For signed requests: `KC-API-TIMESTAMP` (millisecond, from the
   injected `Clock`), `KC-API-SIGN` (`auth.Signer.Sign`), and
   `KC-API-PASSPHRASE` (`auth.Signer.SignPassphrase`) are computed and
   attached, along with `KC-API-KEY` / `KC-API-KEY-VERSION`.
6. GET requests retry per `RetryPolicy` (exponential backoff + full
   jitter, bounded by `MaxElapsed`) on transient transport errors or
   HTTP 429/5xx. POST/DELETE requests are **never** retried automatically,
   regardless of policy — see `Executor.do`'s `retryable` check.
7. The response is decoded into KuCoin's `{code, msg, data}` envelope.
   `code == "200000"` is success; anything else becomes a `*KucoinError`
   carrying the raw code/message. A non-2xx HTTP status is additionally
   wrapped with an HTTP-status sentinel (`ErrUnauthorized`,
   `ErrRateLimited`, etc.) via `%w: %w` so both `errors.Is` (sentinel) and
   `errors.As` (`*KucoinError` detail) work on the same error value.
8. `ResponseMeta` (HTTP status, business code/message, request ID,
   `gw-ratelimit-*`, `x-in-time`/`x-out-time`) is always returned
   alongside the decoded result, even on error.

## Manifest-driven documentation

`internal/endpoints.yaml` is the single source of truth for endpoint
coverage. `internal/gendocs` renders it to `docs/ENDPOINTS.md`. CI
(`.github/workflows/ci.yml`, job `docs`) regenerates the file and
fails the build if it differs from what's committed — so the coverage
table can never silently drift from the manifest, and the manifest can
never silently drift from what's actually implemented (every method in
`uta/market` has exactly one corresponding row, enforced by code review
per [CONTRIBUTING.md](../CONTRIBUTING.md) for now; a stricter automated
check — reflecting over exported methods — is a good follow-up once more
domains exist).

## What's deliberately not abstracted away

- **UTA vs Classic** are separate service roots (`Client.UTA`,
  `Client.Classic`), not a merged type — see the root README's "UTA
  versus Classic" section.
- **No generic `Request(method, path, data)` escape hatch** is exposed
  publicly yet; every implemented endpoint has a typed method. If a raw
  escape hatch is added later, it will be under an `Advanced` name and
  documented as outside typed-compatibility guarantees.

## WebSocket lifecycle

Classic and UTA sockets have intentionally separate clients because their
authentication and envelopes differ. Each client serializes writes, gives a
reader and pinger a specific connection generation, ignores late frames from
a previous generation, stops backoff waits on `Close`, and re-sends retained
subscriptions after reconnect. This keeps a stale connection from completing
a fresh handshake or injecting a push into a current stream.

The UTA v2 client signs a post-welcome private-channel authentication frame
when constructed with `websocket/uta.WithCredentials`. Generic UTA and
Classic subscriptions remain available for all documented channel names, but
only UTA ticker has a typed stream today. The raw routes are deliberately
marked partial in [API_COVERAGE.md](API_COVERAGE.md), rather than presenting
untyped JSON as a complete SDK channel surface.
