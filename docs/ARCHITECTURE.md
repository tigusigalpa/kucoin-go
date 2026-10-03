# Architecture

## Layering

```
kucoin-go/                    root package: ClientConfig, Option, Client (UTA / Classic roots)
├── auth/                     HMAC-SHA256 signer — depends on nothing else in this module
├── transport/                Executor (signed / public / optionally signed HTTP), ResponseMeta,
│                             error hierarchy; owns Credentials/Clock/Logger/RetryPolicy,
│                             which the root package re-exports as type aliases
├── types/                    exact-decimal wire types: Decimal, Int64, ID
├── stream/                   the vocabulary shared by every WebSocket client: connection
│                             State and Event, typed errors, Subscription[T], Handler,
│                             Config and its options
├── orderbook/                exact-decimal Book, the Syncer that keeps it in step with a
│                             sequenced stream, and the shared Level/Snapshot/Delta types
├── internal/wsengine/        the one connection engine behind every WebSocket client
├── internal/wstest/          scriptable fake KuCoin servers and a goroutine-leak detector (tests)
├── websocket/classic/        Classic dialect adapter + low-level client (raw topics, SubscribeTyped)
├── websocket/uta/            UTA v2 dialect adapter + low-level client (SubscribeSpec, auth)
│
├── classic/futures/market/   REST: the complete public Futures market-data group (17 methods)
├── classic/futures/streaming typed Futures channels + managed order book
├── classic/futures/{orders,positions,ws}/   REST seed services and the token endpoints
├── classic/spot/{market,orders,ws}/         REST services and token endpoints
├── classic/spot/streaming    typed Spot channels + managed order book
├── classic/margin/{market,orders,debit}/    REST services
├── classic/margin/streaming  typed Margin channels
├── uta/{market,account,orders,positions,leverage,ws}/   retained UTA v1 compatibility services
├── uta/v2/market/            current UTA REST v2 market data (23 methods)
├── uta/v2/streaming          typed UTA v2 WebSocket channels + managed order book
│
├── internal/endpoints.yaml   manifest: one row per implemented REST method
├── internal/channels.yaml    manifest: one row per typed WebSocket subscription
└── internal/gendocs/         generates docs/ENDPOINTS.md and docs/CHANNELS.md from the manifests
```

Every REST service package follows the identical shape: a `Client` struct wrapping
`*transport.Executor`, a `NewClient(executor)` constructor, and one method per
endpoint. None of them import each other — they are peers wired together only in the
root `config.go`. This keeps each service independently testable and means adding a
domain never risks an import cycle. Current UTA v2 services are nested at
`Client.UTA.V2`; retained v1 services remain at `Client.UTA.*` solely for source
compatibility.

The streaming packages follow one shape as well: a `Service` (`Dial…`) producing a
`Session` with one `Subscribe…` method per channel. They depend on `stream`,
`orderbook`, `types` and one dialect adapter, never on each other. The root package
builds each `Service` from the REST pieces it already has: the token calls of the
`…/ws` services and, for managed order books, a snapshot adapter over the market REST
client.

## Local, no-network validation

A few operations validate obviously malformed input before making any HTTP call or
sending any WebSocket frame, returning a local sentinel error instead: cancelling an
order without an ID (`orders.ErrOrderIDOrClientOidRequired`), a batch over KuCoin's
20-item limit (`orders.ErrTooManyBatchCancelItems`), a market-data call without a
symbol (`market.ErrSymbolRequired`), an order-book depth KuCoin does not offer, a
subscription with no symbols or with more than 100, a private channel requested on a
public session, a private session without credentials. This is deliberately shallow —
it does not duplicate KuCoin's business validation — just enough to turn requests that
would obviously fail into immediate, typed errors.

## Why transport owns the shared config types

`Credentials`, `Clock`, `Logger` and `RetryPolicy` are defined in package `transport`,
not the root package, even though users write `kucoin.Credentials{...}`. The root
package re-exports them as type aliases. This avoids an import cycle: `transport.Executor`
needs these types, and service packages depend on `transport.Executor`, not on the root
package — so the root can import both `transport` and every service package without
anything importing back up. `stream.Logger` has the same method set as `transport.Logger`,
so one logger serves both layers.

## Request lifecycle (transport.Executor)

1. A service method builds a query map (GET) or a typed request struct (POST body) and
   calls `Executor.DoPublic`, `Executor.Do` or `Executor.DoOptional`. Endpoints that need
   repeated query keys use `Executor.DoPublicValues`.
2. `Executor.Do` returns `ErrCredentialsRequired` **locally, with no network call**, when
   credentials are missing. `Executor.DoOptional` signs when complete credentials are
   configured and otherwise sends the request unsigned, leaving KuCoin to decide; it exists
   for the endpoint KuCoin documents as public but serves only to signed callers.
3. The query string is built once via `url.Values.Encode()` (sorted keys) and reused
   byte-for-byte in the signature and on the wire, because KuCoin signs the literal query.
4. Signed requests carry `KC-API-TIMESTAMP` (millisecond, from the injected `Clock`),
   `KC-API-SIGN`, `KC-API-PASSPHRASE` (signed), `KC-API-KEY` and `KC-API-KEY-VERSION`.
5. GET requests retry per `RetryPolicy` (exponential backoff with full jitter, bounded by
   `MaxElapsed`; the wait is a stoppable timer, so a cancelled context ends it at once) on
   transient transport errors and HTTP 429/5xx. POST/DELETE are **never** retried.
6. The response is decoded from KuCoin's `{code, msg, data}` envelope. `code == "200000"`
   is success; anything else becomes a `*KucoinError`, additionally wrapped with an
   HTTP-status sentinel (`ErrUnauthorized`, `ErrRateLimited`, ...) when the status was not
   2xx, so `errors.Is` and `errors.As` both work on one value.
7. `ResponseMeta` (HTTP status, business code and message, request ID, rate-limit headers,
   `x-in-time`/`x-out-time`) is returned alongside the result, even on error.

## Manifest-driven documentation, checked against the code

`internal/endpoints.yaml` and `internal/channels.yaml` are the single sources of truth for
REST and WebSocket coverage. `internal/gendocs` renders them to `docs/ENDPOINTS.md` and
`docs/CHANNELS.md`; CI regenerates both and fails on a diff, and a unit test performs the
same comparison under `go test`.

The manifests cannot claim more than the code implements. `manifest_test.go` in the root
package uses reflection on a real `kucoin.Client`: every row must resolve to an exported
method, every referenced test must exist, every REST method of a tracked service must
have a row, and every `Subscribe…` method of a streaming session must have a channel row
(and the other way round).

## WebSocket architecture

### One engine, two dialects

Classic (`{"type":"subscribe","topic":...}`, REST-issued bullet tokens) and UTA v2
(`{"action":"subscribe","channel":...}`, post-welcome HMAC authentication) differ in their
envelopes and handshakes, not in how a connection must be managed. All of that management
lives once, in `internal/wsengine`, behind a small `Protocol` interface: obtain an endpoint
(a fresh token per attempt), classify an inbound frame (welcome, pong, ack, nack, error,
push), build ping and subscribe frames, and run a post-welcome step (UTA's authentication).
`websocket/classic` and `websocket/uta` are those adapters plus a thin public client, and
the typed streaming packages sit on top of them.

### Goroutines and ownership

| Goroutine | Count | Owns |
|---|---|---|
| reader | 1 per connection | reading the socket; classifies and routes every frame; never blocks on a consumer |
| heartbeat | 1 per connection | pings at half the advertised interval; declares the connection dead when nothing arrives within the timeout |
| supervisor | 1 per client | notices a dead connection, runs the reconnect loop, restores subscriptions |
| pump | 1 per subscription | the *only* caller of the subscription's handler; blocks on a slow consumer without affecting anyone else |
| book events | 1 per managed book | delivers book events to the consumer (see below) |
| snapshot fetch | 1 per sync attempt of a book | one REST snapshot request with retries |
| event handler | 0 or 1 per client | the user's lifecycle-event callback, when configured |

Every goroutine but the lifecycle-event handler (which ends when the event channel closes
at the end of the client's life) is started through a counted wait group that refuses new
work once the client is closing, and `Close` waits for all of them (bounded); a pump waits
for its book's goroutines before it finishes. The test suite checks for leaked goroutines
after every scenario.

### Invariants

- **One owner per channel.** The pump is the only goroutine that sends on and closes a
  subscription's output channel; `Close` and an abort only *signal* it. There is no
  send-on-closed-channel window, which was the first defect of the previous clients.
- **Writes are serialised** with one mutex per connection and bounded by a write timeout;
  gorilla/websocket permits a single concurrent writer.
- **Frames carry their connection generation.** After a reconnect the subscription's queue
  receives an ordered *reset marker* before anything of the new connection, and frames still
  being drained from a superseded connection are dropped, so a handler never sees old-connection
  data behind a reset.
- **State and session change together.** A subscribe can never observe `Connected` without a
  live session: dropping the dead session and entering `Reconnecting` are one step.
- **No lock is held while user code or a blocking send runs.** Handler calls, event delivery,
  logging and channel sends all happen outside the engine's and the order-book syncer's locks.
- **Back-pressure never reaches the socket reader.** Each subscription has a bounded queue
  with an overflow policy (drop oldest, drop newest, or fail the subscription); a drop is
  counted and reaches a sequenced stream as an ordered gap marker, written into the queue at
  the place of the loss.
- **One winner ends a subscription, once.** Whoever claims the end first tells the handler
  (exactly once, a panic in it contained); delivery stops at once, but the topic stays taken
  until the unsubscribe frame has been written, so a new subscribe for the same topic can
  never be overtaken by the old unsubscribe.
- **The server's message limit is respected.** Subscribe, unsubscribe and authentication
  messages go through a sliding window sized to KuCoin's limit on client messages (heartbeats
  are counted, never delayed); the restore after a reconnect, which re-sends every
  subscription, is paced the same way.
- **Fresh credentials per attempt.** Every (re)connection obtains a new token and endpoint
  list; permanent failures (rejected key, missing credentials) stop the retry loop.

### Managed order books

`orderbook.Syncer` is a small state machine (idle → syncing → synced, failed, closed) over
an `orderbook.Book`. It is fed from a pump with deltas, snapshots, reset markers and gap
markers, and from a snapshot goroutine; it produces ordered `Event`s.

Events are queued while the Syncer's lock is held — which fixes their order — and delivered
by a goroutine of the Syncer's own with no lock held. A slow or absent consumer therefore
cannot stall the book, deadlock a caller of `State()`, or freeze a program that only polls
the book; a consumer more than `EventQueue` events behind gets its backlog of updates
collapsed into one reload marker, or, with `stream.FailSubscription`, loses the
subscription. A permanent failure ends the subscription with its cause even when nobody
reads. See [ADR 0004](adr/0004-typed-streaming-and-managed-order-books.md).

### How it is tested

- Unit tests of the engine against `internal/wstest`'s scriptable servers: every lifecycle
  edge (welcome timeout, rejected subscribe, ack timeout, ping watchdog, oversized frame,
  token failure, reconnect with backoff, give-up, permanent errors, resubscribe rejection,
  shutdown during connect/reconnect, handler panic) is a named test.
- A randomised *storm* test kills the connection at random moments while a dozen
  subscriptions stream and others subscribe and unsubscribe concurrently, asserting the
  ordering contract; a randomised Syncer test drives every entry point from several
  goroutines with a failing REST stand-in and a slow consumer. Both run under `-race`.
- Every engine regression test was checked by re-introducing the defect it guards against and
  confirming that the test then fails (a mutation check, done by hand for the lifecycle and
  ordering rules above).
- Every typed channel decodes the worked examples of KuCoin's own documentation, with
  variants chosen so that a wrongly mapped field cannot go unnoticed; where the live feed
  disagrees with the documentation (Futures candles, extra fields) the fixture is a captured
  live push.
- Root-level tests drive the services exactly as an application does, through
  `kucoin.Client`, against an `httptest` server (tokens, snapshots) and a fake WebSocket.
- `go vet` runs the `stdversion` analyser, which fails the build when a standard-library
  symbol newer than the Go version in `go.mod` (1.22) is used.

## What's deliberately not abstracted away

- **UTA vs Classic** are separate service roots (`Client.UTA`, `Client.Classic`), not a merged
  type — see the root README's "UTA versus Classic" section.
- **No generic `Request(method, path, data)` escape hatch** is exposed for REST. For
  WebSocket, the low-level clients (`websocket/classic`, `websocket/uta`) remain available
  with `SubscribeTyped[T]` for channels KuCoin adds after a release of this module.
- **WebSocket order entry** (add/cancel/amend over the socket) is trading, not market data,
  and is not provided; use the REST order endpoints.
