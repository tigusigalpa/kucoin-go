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
- WebSocket bullet-token issuance: `uta/ws.GetPrivateToken`,
  `classic/spot/ws.{GetPublicToken,GetPrivateToken}`,
  `classic/futures/ws.{GetPublicToken,GetPrivateToken}`. Exposed on the
  main `Client` as `UTA.Ws`, `Classic.Spot.Ws`, `Classic.Futures.Ws`.
- `websocket/classic` and `websocket/uta`: reconnecting WebSocket clients
  for KuCoin's two structurally incompatible wire protocols. Both handle
  dial, welcome handshake, ping/pong heartbeat with dead-connection
  detection (`SetReadDeadline` extended on every received frame; a missed
  heartbeat window surfaces as a read error and triggers reconnect),
  exponential-backoff reconnect (1s-60s cap), and automatic
  resubscription after reconnect. `websocket/classic.Client.Subscribe`
  blocks until KuCoin acknowledges the subscription;
  `websocket/uta.Client.Subscribe` does not block on an ack, since UTA's
  ack format is inconsistently documented — see the method's docblock.
  Only a handful of channels are hand-typed; everything else is delivered
  as raw `json.RawMessage` for the caller to decode. The UTA WebSocket API
  is documented by KuCoin as pre-release/beta and unsuitable for
  production — see the `websocket/uta` package docblock.
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

### Fixed

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

- UTA Market/Account/Orders/Positions/Leverage, Classic Spot (including
  stop orders, OCO orders, and Disconnect Cancel Protocol), a Classic
  Futures seed set (place/test/query orders; position/margin/
  position-mode reads), and a Classic Margin seed set (market data,
  order management, borrow/repay/interest) are implemented. Classic
  Futures market data, Futures order cancellation, Classic Margin's
  stop/OCO orders and lending-side ("Credit") endpoints, and all Phase
  3 domains are not yet implemented — see
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
