# Changelog

All notable changes to this project are documented here. Format loosely
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this
project follows [SemVer](https://semver.org/).

## [Unreleased]

### Streaming hardening: typed market-data streaming for Futures, Spot, Margin and UTA

#### Added

- **Typed WebSocket streaming for every channel of KuCoin's current documentation**,
  reachable as `Client.Classic.Futures.Stream`, `Client.Classic.Spot.Stream`,
  `Client.Classic.Margin.Stream` and `Client.UTA.V2.Stream` (packages
  `classic/futures/streaming`, `classic/spot/streaming`, `classic/margin/streaming`,
  `uta/v2/streaming`). A service dials sessions (`DialPublic`/`DialPrivate`; UTA:
  `DialFutures`/`DialSpot`/`DialPrivate`) and a session has one `Subscribe…` method per
  channel returning a `*stream.Subscription[T]` of structs: Classic Futures 10 public
  and 6 private channels, Classic Spot 12 and 4, Classic Margin 2 and 6, UTA v2 8 and 7.
  Applications never parse JSON and never implement the protocol. WebSocket order entry
  (trading, not market data) is not provided.
- **Managed local order books** (`SubscribeOrderBook`) for Classic Futures, Classic Spot
  (which needs credentials for its signed REST snapshot) and UTA (`increment@10ms`, which
  pushes its own snapshot): exact-decimal levels, KuCoin's documented procedure (buffer,
  REST snapshot, drop what the snapshot contains, replay, require consecutive
  sequences), automatic resynchronisation after a sequence gap, a reconnect, dropped
  updates, an undecodable update or a buffer overflow, bounded retries and a typed
  terminal error. The live Futures book matched KuCoin's own REST snapshot at the same
  sequence number in every comparison.
- **`Client.Classic.Futures.Market`**: the complete public Classic Futures market-data
  REST group — contract and all contracts, ticker and all tickers, full and part order
  book, trade history, klines, mark price, spot index price, interest-rate index, premium
  index, 24-hour platform statistics, server time, service status — plus the two public
  funding-rate methods. Prices, rates and sizes are `types.Decimal`.
- New public packages: `types` (`Decimal`, `Int64`, `ID`: exact numbers that accept the
  string, bare-number and exponent spellings KuCoin mixes), `stream` (connection states,
  lifecycle events, typed errors, `Subscription[T]`, `Handler`, `Config` and its options)
  and `orderbook` (`Book`, `Syncer`, `Level`, `Snapshot`, `Delta`).
- `kucoin.WithStreamOptions` (default connection options of every session) and
  `kucoin.WithUTAWebSocketHosts` (proxy / test endpoints for UTA).
- `transport.Executor.DoOptional`: signs when complete credentials are configured and sends
  unsigned otherwise, for the endpoint KuCoin documents as public but serves to signed
  callers only (Futures 24-hour statistics).
- Low-level clients: `websocket/classic` gains `TokenSource`, `NewClientWithTokenSource`,
  `SubscribeTyped[T]` and `SubscribeHandler` (a token and endpoint list per connection
  attempt); `websocket/uta` gains `SubscribeSpec` (channel, trade type, symbols, interval,
  depth, RPI filter, account type), `SubscribeTyped[T]` and `SubscribeHandler`.
- `internal/channels.yaml` (one row per typed subscription) and the generated
  `docs/CHANNELS.md`; reflection-based manifest tests in the root package fail the build
  when a manifest row does not resolve to a real method, a referenced test does not exist,
  a tracked REST method or a `Subscribe…` method has no row, or a subscription payload could
  hand its consumer undecoded data (`json.RawMessage`, `[]byte`, an interface).
- Documentation: `docs/STREAMING.md` (the guide), `docs/STREAMING_READINESS_AUDIT.md`,
  ADRs 0002–0004, a rewritten `docs/ARCHITECTURE.md` and `docs/API_COVERAGE.md` (computed
  page by page against KuCoin's documentation index), `CONTRIBUTING.md`, and the examples
  `futures_market`, `futures_stream`, `futures_orderbook`, `uta_stream`, `spot_stream`.
- `make vet` and `make race`; `make check` now includes `go vet`.
- README rewritten for this release: a friendlier introduction, quick starts for a REST call,
  a live stream and a local order book (with real output), a Futures REST tour, a streaming
  cookbook (several channels at once, closed candles only, top of book, mirroring a book, mark
  price and funding, connection events, lossless consumers, shutdown, UTA, Spot and Margin,
  private channels, errors, channels newer than the release), exact numbers, connection
  options with defaults, a proxy example and an FAQ. Every Go example compiles against the
  module; the quick starts and the closed-candle recipe were run against the live public API.
  The README no longer presents the UTA REST order book as public (KuCoin serves it to signed
  requests only).
- **Request pacing to KuCoin's message limit**: a connection keeps its subscribe, unsubscribe
  and authentication messages within 90 % of KuCoin's limit on client messages (Classic 100
  per 10 s, UTA 300 per 10 s public and 100 private; heartbeats keep the rest), also while it
  restores subscriptions after a reconnect, which used to send them at 100 per second and
  could get the connection dropped again. `stream.WithMessageLimit(n, window)` changes the
  budget, `stream.WithoutMessagePacing()` turns it off (`stream.Config.MessageLimit`).
- `market.Granularity3Min` and `market.Granularity1Month` for `Futures.Market.GetKlines`: the
  live endpoint serves both although the documentation omits them. `uta/v2/market`
  `OrderBookOptions.Full` requests the whole book (`limit=FULL`; KuCoin requires 20, 100 or
  FULL). `SymbolSnapshot.FundingRate` and `InstrumentEvent.Period`: fields the live Futures feed
  sends and the documentation does not list.

#### Changed (behaviour of the existing low-level clients; no exported signature changed)

- `websocket/classic` and `websocket/uta` are thin adapters over one shared connection engine
  (`internal/wsengine`). Their `Logger` is now an alias of `stream.Logger` (same methods).
- UTA subscriptions deliver only the symbol they asked for (before, every subscription of a
  channel and trade type received every symbol), and channels whose push type has no
  `.TRADETYPE` suffix (`mark-price`, `funding-fee`, `funding-fee-all-symbols`, `lw`, ...) are
  now delivered; multi-symbol Classic topics (`/market/ticker:A,B`) are routed.
- A rejected subscription fails at once with a typed `*stream.ServerError` instead of
  hanging for the acknowledgement timeout; overlapping subscriptions on one connection are
  refused (`stream.ErrAlreadySubscribed`), because KuCoin keys its own subscriptions the
  same way.
- Heartbeat: ping at half of the interval KuCoin advertises (from the token response or the
  welcome message) instead of fixed values, never faster than once per second; any inbound
  frame counts as proof of life.
- Reconnect: every attempt fetches a fresh token and endpoint list (tokens expire after 24
  hours); jittered exponential backoff that resets only after a stable connection;
  permanent failures (missing credentials, rejected key) stop the loop; resubscription is
  acknowledged and ordered behind a reset marker.
- `Close` ends every subscription, sends a WebSocket close frame and waits (bounded) for
  the client's goroutines; subscription channels are closed by their one owner goroutine.
- Slow consumers: the default overflow policy is now "drop the oldest" (was: drop the newest),
  the default queue holds 1024 updates (was 256), and every drop is counted
  (`Subscription.Dropped`, `Stats.PushesDropped`) and reported to sequenced streams as a gap.
- `Subscribe` waits for a reconnect in progress instead of failing; subscribing the same
  topic twice returns the same channel on the low-level client (typed APIs return
  `stream.ErrAlreadySubscribed`).
- `transport.Executor` waits between GET retries with a stoppable timer (no leaked timer when
  the context ends first).
- `Subscribe` also survives a connection that drops before KuCoin has answered it: the
  registered subscription is subscribed again by the reconnect and the call returns when that
  has happened. A resubscription that KuCoin never answers on an otherwise live connection
  ends that subscription with `stream.ErrAckTimeout` instead of keeping every subscription of
  the connection in an endless reconnect loop.
- Unsubscribing a topic keeps the topic taken until the unsubscribe frame has been written, so
  an immediate new subscription to it cannot have its subscribe frame overtaken (and then
  cancelled) by the old unsubscribe. A subscription ended by an overflow (`FailSubscription`),
  a handler panic, or a `Close` that raced with its restore is unsubscribed at KuCoin too.
- `ReconnectPolicy.MaxAttempts` budgets the consecutive failed attempts of one outage; the
  backoff exponent still spans outages that did not last `StableAfter`.
- `Close` returns, for every concurrent caller, only once the connection is closed; `Connect`
  after a close can no longer receive the old life's terminal event.
- `DropNewest` reports the loss behind the backlog (the lost updates are the newest ones) and
  also when no update follows; `DropOldest` reports it at the place of the dropped update, so a
  reset marker in front of it is still delivered first.
- Subscribing one topic from several goroutines at once returns the same channel to all of them
  on the low-level clients (the losers used to get `stream.ErrAlreadySubscribed`); a symbol
  listed twice in one Futures request is subscribed once.

#### Fixed

- Cleared `errcheck` and `staticcheck` findings across streaming tests, examples, and
  transport response cleanup: session and order-book teardown now reports `Close` failures,
  concurrent close tests await their result, and equivalent predicates and switches use
  idiomatic Go forms.
- Closing or unsubscribing while the reader goroutine was sending to the subscription
  channel was a data race and a `send on closed channel` panic (both WebSocket clients).
- A slow consumer silently lost the newest messages, which corrupts sequenced streams such
  as order books; there was no counter and no signal.
- Reconnecting reused the original bullet token (expired after 24 hours) and ignored the
  heartbeat parameters KuCoin advertises.
- Server error frames were ignored by the Classic client; the UTA client could not request
  `kline` and `obu` channels at all (no interval or depth parameters); UTA authentication used
  the wall clock instead of the injected one.
- A lagging consumer under sustained overflow no longer lets its queue grow without bound;
  a panic in a handler's `OnAbort` is contained instead of taking the socket reader (or the
  caller of `Close`) down; the log and event callbacks no longer run while the connection
  holds its registry lock, so a slow logger cannot block `Subscribe`, `Close` or the reconnect.
- `orderbook.Book.Spread` and `Mid` read the best bid and ask under one lock (a concurrent
  update could pair a bid and an ask of different states).
- `SubscribeOrderBookIncrement` (UTA, deprecated) fetches the full REST snapshot instead of a
  truncated one; the Futures position-settlement event carries its symbol (taken from the
  topic).
- Found while hardening, in new code: an order-book event consumer that was slow, absent or
  calling back into the book could deadlock it or freeze the book (events are now queued
  under the lock and delivered by a goroutine of their own, with a reload marker for a
  consumer that falls behind); `Subscribe` could dereference a missing session in the instant
  between a connection dying and the client entering the reconnecting state; frames of a
  superseded connection could be delivered behind a reset marker. A randomised
  connection-kill test and a randomised book test now guard all three.
- The documentation links of 25 methods pointed at pages KuCoin has renamed (the order, stop
  order, OCO order and leverage pages of Classic Spot and Margin, and the Margin risk-limit
  page); every one was checked against the current page's path and HTTP method.

#### Documentation inconsistencies found and handled

- Futures `GET /api/v1/trade-statistics` is documented as public but answers `400001` without
  credentials; it uses `DoOptional`.
- Futures WebSocket candles are `[start, open, close, high, low, turnover, volume]`: close
  before high/low, unlike REST, and the last two elements the other way round from the
  documented "volume, turnover" (element 5 is the quote-currency turnover, element 6 the
  contract count; checked against the REST candle of the same minute). `Kline.Turnover` and
  `Kline.Volume` follow the live order. The Futures snapshot's `volume` is in the base
  currency; the live feed adds `fundingRate` to the snapshot and `period` to the funding-rate
  push; the kline REST endpoint accepts the granularities 3 and 43200 the documentation omits.
- Futures funding-rate REST answers use exponent notation (`1.0E-4`), add `dailyInterestRate`
  and `lastTimeFundingRate` and omit the documented `predictedValue`; order-book levels are
  bare JSON numbers (some with a trailing `.0`) on REST and strings on the WebSocket.
- `/contract/announcement` is a global topic although the title says `:{symbol}`; the
  documented balance subjects `orderMargin.change`, `availableBalance.change` and
  `withdrawHold.change` are deprecated in favour of `walletBalance.change`.
- UTA `increment` depth is documented as deprecated from 2026-07-15 but still answers;
  KuCoin rejects the `symbols` list form for the trade, kline, obu, mark-price and call-auction
  channels (one request per symbol is sent).
- The Margin documentation reuses the Spot specifications for its order, balance and stop-order
  channels; the Spot full order-book snapshot is a signed endpoint although it is market data.

## [1.2.0] and earlier

These entries were collected under "Unreleased" up to and including the v1.2.0 tag.

### Added

- `Client.UTA.V2.Market`: all 23 methods in KuCoin's current UTA REST v2
  Market Data group, including current/funding-history/open-interest data,
  order book, risk tiers, interest-rate index, fiat/custody/service/KYC/IP
  helpers and repeated-query currency filters. This additive service leaves
  existing `Client.UTA.*` v1 compatibility APIs unchanged.
- `transport.Executor.DoPublicValues`, used where KuCoin requires repeated
  query keys instead of a comma-delimited parameter.
- `websocket/uta.WithCredentials`: explicit signed private-channel
  authentication after the UTA v2 welcome frame, authentication failure
  reporting, and automatic re-authentication during reconnect. UTA ticker
  now has `SubscribeTicker`, a typed stream that avoids caller JSON parsing.
- Compatibility note: `websocket/uta.Client.Subscribe` now waits for the
  documented acknowledgement (up to 10 seconds) instead of reporting success
  immediately after the write. Its signature is unchanged; this behavioural
  correction lets callers observe rejected subscriptions deterministically.
- [API coverage matrix](docs/API_COVERAGE.md), a current-docs complete /
  partial / absent inventory; `examples/uta_v2_market` for public UTA v2
  ticker and funding-rate calls.

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
- `uta/account`: all 6 UTA account endpoints (overview, currency assets,
  fee rate, ledger, account mode, API-key info).
- `uta/orders`: all 8 UTA order endpoints (place, cancel, batch-cancel by
  ID, batch-cancel by symbol, order details, open-order list, order
  history, trade history). `clientOid` is never auto-generated. Local
  validation (`ErrOrderIDOrClientOidRequired`, `ErrTooManyBatchCancelItems`)
  rejects obviously malformed cancel requests before any network call.
- `uta/positions`: all 6 UTA position endpoints (open positions, position
  history, funding-fee history, batch margin-mode change, position-margin
  adjustment, get margin mode).
- `uta/leverage`: all 3 UTA leverage endpoints (modify futures leverage,
  modify cross-margin leverage, get leverage) — split into its own
  package since all three require the `Unified` permission, unlike most
  read endpoints elsewhere in this SDK.
- `Client.Classic`: a second, independently configured `transport.Executor`
  (own `ClassicBaseURL`, same host as UTA today but kept separate for a
  future divergence) backing the new Classic account-mode service root.
- `classic/spot/market`: all 8 Classic Spot market-data endpoints
  (currency, all-symbols, ticker, all-tickers, klines, part-orderbook,
  full-orderbook, server-time).
- `classic/spot/orders`: all 10 Classic Spot order endpoints (add, add
  test, batch-add, cancel by order ID, cancel by client OID, cancel all,
  get by order ID, open orders, closed orders, trade history/fills) — all
  under KuCoin's `/api/v1/hf/...` "High-Frequency" path family, a
  different path family from UTA's `/api/ua/v1/...` orders, not just a
  version bump. Local validation
  (`classicspotorders.ErrLimitOrderRequiresPrice`,
  `ErrTooManyBatchOrders`) rejects obviously malformed requests before
  any network call.
- `Client.Classic.Futures`: a third, independently configured
  `transport.Executor` on `DefaultClassicFuturesBaseURL`
  (`https://api-futures.kucoin.com`) — a genuinely distinct host from
  UTA/Classic-Spot's `api.kucoin.com`, confirmed across every Futures
  endpoint fetched.
- `classic/futures/orders`: a Phase-1 seed set — PlaceOrder, PlaceOrderTest,
  GetOrderByID, GetOrderList (cancel and the broader stop/OCO order
  family are not yet implemented). `clientOid` is required by KuCoin and
  never auto-generated. Local validation
  (`ErrMutuallyExclusiveSize`, `ErrLimitOrderRequiresPrice`) rejects
  obviously malformed requests before any network call.
- `classic/futures/positions`: a Phase-1 seed set — GetPositionDetails,
  GetPositionList, GetMarginMode, GetPositionMode.
- WebSocket bullet-token issuance: legacy `uta/ws.GetPrivateToken`,
  `classic/spot/ws.{GetPublicToken,GetPrivateToken}`,
  `classic/futures/ws.{GetPublicToken,GetPrivateToken}`. Exposed on the
  main `Client` as `UTA.Ws`, `Classic.Spot.Ws`, `Classic.Futures.Ws`.
- `websocket/classic` and `websocket/uta`: reconnecting WebSocket clients
  for KuCoin's two structurally incompatible wire protocols. Both handle
  dial, welcome handshake, ping/pong heartbeat with dead-connection
  detection (`SetReadDeadline` extended on every received frame; a missed
  heartbeat window surfaces as a read error and triggers reconnect),
  exponential-backoff reconnect (1s-60s cap), and automatic
  resubscription after reconnect. `websocket/classic.Client.Subscribe` and
  `websocket/uta.Client.Subscribe` both wait for KuCoin's successful
  subscription acknowledgement. Current UTA private channels use a signed
  post-welcome authentication frame rather than the retained legacy token;
  UTA ticker has a typed stream. Remaining channels are delivered as raw
  `json.RawMessage` pending typed models.
- `github.com/gorilla/websocket` added as the WebSocket transport
  dependency.
- `classic/margin/market`: a seed set of Classic Margin's public
  market-data endpoints — cross/isolated symbol specs, mark price
  (list/detail), margin config, and cross/isolated risk limits.
  Collateral-ratio and market-available-inventory are not yet
  implemented pending confirmed field-level schemas.
- `classic/margin/orders`: Classic Margin's core order-management
  endpoints (place, cancel by ID/clientOid/all, get by ID/clientOid,
  open orders, closed orders, trade history), all under the
  `/api/v3/hf/margin/...` path family — a different version *and* path
  family from Classic Spot's `/api/v1/hf/...` orders. `tradeType`
  (`MARGIN_TRADE`/`MARGIN_ISOLATED_TRADE`) discriminates cross vs
  isolated on listing/cancel-all calls; `IsIsolated` does the same on
  `PlaceOrder` — two different KuCoin conventions for the same
  distinction, preserved as-is rather than papered over.
- `classic/margin/debit`: Classic Margin's borrow/repay/interest-history
  endpoints plus leverage modification. `ModifyLeverage` posts to
  `/api/v3/position/update-user-leverage`, a different path family from
  every other endpoint in this package (`/api/v3/margin/...`) —
  confirmed against KuCoin's docs, not a typo.
- `Client.Classic.Margin`: wired into the main `Client` alongside Spot
  and Futures, sharing Classic Spot's `api.kucoin.com` host.
- `Classic.Spot.Orders`: the stop-order family — AddStopOrder,
  CancelStopOrderByID, CancelStopOrderByClientOid, CancelStopOrders
  (batch), GetStopOrderByID, GetStopOrderByClientOid, GetStopOrderList
  — a distinct, older order family from the existing HF orders, living
  under `/api/v1/stop-order...` rather than `/api/v1/hf/orders...` but
  sharing the same host and signing scheme. Unlike PlaceOrder,
  ClientOid is optional on AddStopOrder. GetStopOrderByClientOid
  decodes as an array (a clientOid is not guaranteed unique across a
  stop order's lifecycle the way an orderId is), while
  CancelStopOrderByID/CancelStopOrders both decode `cancelledOrderIds`
  as an array even for a single-order cancel. GetStopOrderList uses
  page-number pagination, unlike GetClosedOrders/GetTradeHistory's
  cursor pagination.
- `Classic.Spot.Orders`: the OCO (One-Cancels-the-Other) order family —
  AddOCOOrder, CancelOCOOrderByID, CancelOCOOrderByClientOid,
  CancelOCOOrders (batch), GetOCOOrderByID, GetOCOOrderByClientOid,
  GetOCOOrderDetails, GetOCOOrderList — living under `/api/v3/oco/...`,
  a third distinct path family from both HF orders and stop orders.
  Unlike AddStopOrder, ClientOid is required on AddOCOOrder. The
  Info/List endpoints return a flat pair summary with no side/price;
  only GetOCOOrderDetails exposes the two constituent legs (via an
  `orders` array whose items key their ID as `id`, not `orderId` — a
  KuCoin naming inconsistency between the leg shape and the
  pair-summary shape, preserved as-is rather than silently unified).
  CancelOCOOrderByID/CancelOCOOrders both decode `cancelledOrderIds` as
  an array holding both leg IDs of each cancelled pair.
- `Classic.Spot.Orders`: Disconnect Cancel Protocol — SetDCP, GetDCP,
  DeactivateDCP (a convenience wrapper; KuCoin has no dedicated
  deactivate endpoint, so this calls SetDCP with `Timeout: -1`). No
  separate Classic Margin DCP endpoint exists — confirmed via
  documentation research; the older unified `/api/ua/v1/dcp/...`
  endpoints are marked abandoned by KuCoin and were not implemented.
- `Classic.Margin.Orders`: the stop-order and OCO-order families —
  AddStopOrder, CancelStopOrderByID, CancelStopOrderByClientOid,
  CancelStopOrders, GetStopOrderByID, GetStopOrderByClientOid,
  GetStopOrderList, AddOCOOrder, CancelOCOOrderByID,
  CancelOCOOrderByClientOid, CancelOCOOrders, GetOCOOrderByID,
  GetOCOOrderByClientOid, GetOCOOrderDetails, GetOCOOrderList (15
  endpoints). Confirmed via documentation research that Classic Margin
  has its own parallel stop/OCO endpoint families under
  `/api/v3/hf/margin/stop-order...`/`/api/v3/hf/margin/oco-order...` —
  structurally distinct from Classic Spot's `/api/v1/stop-order`/
  `/api/v3/oco/order`, not shared endpoints reached via a query
  parameter. Margin mode is selected via `IsIsolated` on the two Add
  requests, while `TradeType` (matching this package's existing HF
  order convention) discriminates cross vs isolated on the
  cancel-batch/list endpoints — two different KuCoin conventions,
  preserved as-is. `GetStopOrderByClientOid` decodes a single object
  here, unlike Classic Spot's array-returning equivalent.
  `CancelStopOrderByID` sends `orderId` as a query parameter despite
  KuCoin's own OpenAPI spec documenting that endpoint with zero
  parameters — a confirmed documentation bug, implemented by analogy
  with every sibling cancel-by-id endpoint and flagged as unverified
  against a live account in the method's docblock.

### Fixed

- Classic and UTA WebSocket workers are now bound to their connection
  generation. Frames from a superseded socket cannot complete a new handshake
  or be delivered to current subscriptions; close and reconnect paths also
  consistently close subscriber streams and cancel pending backoff waits.
- GitHub Actions workflows now test and lint against the module's declared
  Go 1.22 baseline instead of the unsupported Go 1.21 matrix entry, and all
  Codecov uploads use the configured `unit` flag so the 80% policy applies
  consistently.

- `UTA.Market.GetOrderBook` now signs the request. A live smoke test
  during development confirmed this endpoint requires credentials (HTTP
  400 `400001` when unauthenticated), unlike every sibling UTA
  market-data endpoint — see `GetOrderBook`'s docblock.

### Documentation inconsistencies found and handled (not assumed)

- `GET /api/ua/v1/unified/position/open-list` (`UTA.Positions.GetPositions`):
  KuCoin's schema table names a required field `positionValue`, but the
  worked JSON example in the same doc shows `positionMargin` in that slot
  instead. Both fields are decoded; `Position.Value()` returns whichever
  one KuCoin actually sent.
- `GET /api/ua/v1/unified/account/leverage` (`UTA.Leverage.GetLeverage`):
  the documented `marginMode` enum is literally `ISOLATE, CROSS` (missing
  the "D"), inconsistent with every other endpoint's `ISOLATED`. The raw
  value is preserved as-is rather than silently coerced.
- `POST /api/ua/v1/unified/position/modify-margin` (`UTA.Positions.ModifyPositionMargin`,
  docs slug `modify-isolated-futures-margin`): the prose says "Only
  FUTURES applicable" but the `tradeType` enum lists both `FUTURES` and
  `MARGIN`. Both are accepted as-is.
- `GET /api/v3/currencies/{currency}` (`Classic.Spot.Market.GetCurrency`):
  the schema table documents `data` as an array; the worked example in the
  same doc shows a single object. This SDK follows the example.
- `POST /api/v1/hf/orders/multi` (`Classic.Spot.Orders.BatchAddOrders`):
  the per-item schema marks `price` unconditionally required, inconsistent
  with the single Add Order endpoint (`price` required only for `limit`
  orders). Treated as a doc bug; validated per-item to match Add Order's
  real behavior.
- `DELETE /api/v1/hf/orders/cancelAll` (`Classic.Spot.Orders.CancelAllOrders`):
  `failedSymbols`' item shape is undocumented (KuCoin's example always
  shows an empty array) — decoded as a raw map so nothing is silently
  dropped if a real partial failure returns fields.
- `GET /api/v1/orders/test` (`Classic.Futures.Orders.PlaceOrderTest`): its
  `timeInForce` enum is documented narrower (`GTC`\|`IOC`) than the live
  `PlaceOrder` endpoint's (`GTC`\|`IOC`\|`RPI`) — not enforced client-side
  either way, since it's unclear whether this is a real functional
  difference or a docs gap.
- `GET /api/v2/position` (`Classic.Futures.Positions.GetPositionDetails`):
  despite the singular endpoint name/description, `data` is a JSON array,
  not a single object — modeled as `[]Details`.
- `GET /api/v1/positions` (`Classic.Futures.Positions.GetPositionList`) vs.
  `/api/v2/position` (`GetPositionDetails`): two structurally incompatible
  position schemas exist server-side — different field names, and v1's
  money/ratio fields are JSON numbers while v2's are strings. Modeled as
  two distinct, non-interchangeable types (`ListItem` vs `Details`); v1's
  numeric fields use `json.Number` to preserve full precision rather than
  risking float64 rounding.
- `GET /api/v3/margin/currencies` (`Classic.Margin.Market.GetRiskLimitCross`/
  `GetRiskLimitIsolated`): KuCoin's doc title is "Get Margin Risk Limit"
  but the actual path is `.../currencies` — this SDK's method names
  follow the documented behavior, not the mismatched title. The
  `borrowCoefficient` field on the cross-margin shape is explicitly
  marked "Abandoned" by KuCoin (kept in the struct for back-compat only,
  documented as unreliable).
- `GET /api/v3/margin/repay` (`Classic.Margin.Debit.GetRepayHistory`):
  the page-description prose literally says "borrowing orders" though
  the endpoint demonstrably returns repayment orders (principal/interest
  fields, not size/actualSize) — treated as a doc copy/paste error, not
  followed.
- Classic Margin's borrow/repay/interest-history endpoints send `symbol`
  as JSON `null` (not omitted) for cross-margin entries; `BorrowHistoryItem.Symbol`/
  `RepayHistoryItem.Symbol` decode this as an empty string rather than a
  pointer, consistent with this SDK's existing preference for zero
  values over nil where the distinction carries no real signal.

### Known limitations (this checkpoint)

- WebSocket order entry (Classic add/cancel order, UTA add/cancel/amend order) is not
  provided: it is trading, not market data. Use the REST order endpoints.
- The managed Classic Spot order book needs API credentials, because KuCoin serves the Spot
  full order-book snapshot only to signed requests; Futures and UTA books need none.
- The private WebSocket channels (orders, balances, positions, ...) are tested against the
  documentation's worked examples and fake servers, never against a customer account. A
  session is one connection: more than 300 topics (Classic) or 600/400 (UTA public/private)
  need more sessions, which the library does not shard automatically. KuCoin offers no replay
  of updates missed while a connection was down; sequenced streams resynchronise instead.
- REST gaps are listed per market in [docs/API_COVERAGE.md](docs/API_COVERAGE.md): among
  them most Classic Spot market-data pages other than the core ones, the Futures order
  cancellation and position-management endpoints, and all UTA v2 account/trading REST.

- UTA Market/Account/Orders/Positions/Leverage, Classic Spot (including
  stop orders, OCO orders, and Disconnect Cancel Protocol), a Classic
  Futures seed set (place/test/query orders; position/margin/
  position-mode reads), and a Classic Margin seed set (market data,
  order management including stop/OCO orders, borrow/repay/interest)
  are implemented, as is the complete Classic Futures market-data group.
  Futures order cancellation, Classic Margin's lending-side ("Credit")
  endpoints, and all Phase 3 domains are not yet implemented — see
  [docs/ENDPOINTS.md](docs/ENDPOINTS.md).
- `Classic.Spot.Orders.Order.CancelReason` is an opaque integer code
  (0-18, 34-39, 99) — KuCoin's docs list the allowed values with no
  semantic labels for any of them.
- Business-level KuCoin error codes (per-domain, e.g. Spot/Margin/Futures)
  are not yet mapped to sentinels — only HTTP-status-derived sentinels
  exist so far. The raw code/message is always available via
  `*transport.KucoinError`.
- No pagination iterator helper yet — `GetOrderHistory`/`GetTradeHistory`/
  `GetPositionHistory`/`GetFundingFeeHistory`/`GetLedger` expose their
  cursor (`LastID`) but callers must loop manually (see the README's
  pagination example).
