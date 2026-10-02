# KuCoin API Coverage Matrix

**Documentation snapshot:** 2026-10-03. This matrix is reconciled against
KuCoin's current [docs-new index](https://www.kucoin.com/docs-new), not
against legacy endpoint names or another SDK. The generated
[endpoint manifest](ENDPOINTS.md) is the method-level source of truth for
what this repository actually implements.

Status meanings:

- **Full** — every method currently listed by KuCoin for that narrowly named
  group has a typed SDK method and mock HTTP coverage.
- **Partial** — at least one documented method or channel is intentionally
  absent, raw-only, or not verified with a live authenticated account.
- **Absent** — no current endpoint in that group is implemented.

“Full” here does not mean that a credential-protected production request has
been exercised with a real account: CI deliberately never uses customer
credentials or moves funds.

## REST

| API family in current KuCoin docs | Status | Evidence and boundary |
|---|---|---|
| UTA REST v2 — Market Data (23 listed methods) | **Full** | `Client.UTA.V2.Market` implements announcements, currencies, ticker/instruments/klines/orderbook/trades, index/funding/open-interest data, risk tiers, platform stats, fiat/custody/service/KYC/IP helpers. The v2 order-book endpoint is correctly private (`General` permission); the other public methods do not require credentials. |
| UTA REST v2 — Account & Funding (18 listed methods) | **Absent** | The old `Client.UTA.Account` and `Leverage` services preserve v1 compatibility only. Their endpoints are listed under KuCoin's abandoned UTA REST section, not counted as v2 coverage. |
| UTA REST v2 — Sub Accounts (9 listed methods) | **Absent** | No current v2 sub-account service. |
| UTA REST v2 — Deposit / Transfer / Withdrawal (including 4 withdrawal methods) | **Absent** | No fund-moving v2 endpoints are exposed. This remains deliberately absent rather than guessed. |
| UTA REST v2 — Orders (10 listed methods) | **Absent** | The legacy v1 order service is retained, but current v2 place/amend/cancel/query methods are not yet implemented. |
| UTA REST v2 — Positions (6 listed methods) | **Absent** | The legacy v1 position service is retained, but current v2 methods are not yet implemented. |
| UTA REST v2 — Affiliate (5 listed methods) | **Absent** | No service. |
| UTA REST v2 — VIP Lending (3 listed methods) | **Absent** | No service. |
| UTA REST v2 — Broker | **Absent** | No service. |
| Classic — Account, funding, sub-account, deposits, withdrawals, transfers and fee APIs | **Absent** | No Classic account/funding root has been implemented yet. |
| Classic Spot — Market Data (20 listed methods) | **Partial** | Currency, all symbols, ticker/all tickers, klines, part/full order book and server time are implemented. Announcements, trade history, call-auction data, fiat price, stats, market list, client IP, service status and KYC regions remain absent. |
| Classic Spot — Orders | **Partial** | HF order, stop, OCO and DCP families have broad coverage; documented synchronous, partial-cancel and some list variants remain absent. |
| Classic Margin — Market Data (7 listed methods) | **Full** | Cross/isolated symbols, mark price detail/list, config and both risk-limit methods are implemented. |
| Classic Margin — Orders / Debit | **Partial** | Core, stop and OCO orders plus borrow/repay/interest/leverage are implemented. The standalone order-test method and other current variants need an explicit audit. |
| Classic Margin — Credit (7 listed methods) | **Absent** | Lending purchase/redeem endpoints are not implemented. |
| Classic Futures — Market Data (15 listed methods) | **Absent** | No current futures market-data service; use UTA v2 Market where the account model permits it. |
| Classic Futures — Orders (17 listed methods) | **Partial** | Place, place-test, get-by-id and list are implemented. Cancel, batch, TP/SL, stop, closed-order/value and trade-history methods remain absent. |
| Classic Futures — Positions (18 listed methods) | **Partial** | Position details/list and read-only margin/position mode are implemented. Mode switches, limits, margin adjustment, leverage and risk APIs remain absent. |
| Classic Futures — Funding Fees (3 listed methods) | **Absent** | No service. |
| Classic Earn, VIP Lending, Affiliate, Broker, Copy Trading and Convert | **Absent** | These specialty domains have no implementation. |

## WebSocket

| Current KuCoin channel family | Status | Evidence and boundary |
|---|---|---|
| UTA WebSocket v2 — public (ticker, kline, trade, order book, mark price, funding rate, all funding rates, call auction) | **Partial** | `websocket/uta` implements the documented lowercase `subscribe`/`unsubscribe`, ping/pong, cancellable exponential reconnect and automatic resubscription. `SubscribeTicker` emits a typed `Ticker`; remaining channels are raw `Push` payloads. |
| UTA WebSocket v2 — private (order, execution lite/execution, balance, position, liquidation warning, leverage) | **Partial** | `websocket/uta.WithCredentials` signs the documented post-welcome `auth` frame, checks the result and re-authenticates on reconnect. Its signature and failure paths have mock coverage. Channel payloads remain raw and no customer-key integration test is run in CI. |
| UTA WebSocket v2 — trade | **Absent** | No authenticated WebSocket order-entry API. |
| Classic WebSocket — Spot public (12 listed channels) | **Partial** | Generic Classic topic subscription, heartbeat/reconnect/resubscribe and raw `Message` envelope are implemented; per-topic typed decoders are absent. |
| Classic WebSocket — Margin private (5 listed channels) | **Partial** | Generic private topic lifecycle only; raw payloads. |
| Classic WebSocket — Futures public (9 listed channels) | **Partial** | Generic topic lifecycle only; raw payloads. |
| Classic WebSocket — Futures private (6 listed channels) | **Partial** | Generic topic lifecycle only; raw payloads. |
| Classic WebSocket — add/cancel order | **Absent** | No WebSocket order-entry API. |

## Lifecycle and verification boundary

The Classic and UTA clients serialize writes, bind reader/ping workers to a
specific connection generation, ignore late frames from superseded sockets,
close subscriptions on shutdown, stop reconnect waits when closed, and
prevent duplicate reconnect loops. Mock WebSocket tests cover welcome timeout
cleanup, UTA v2 authentication success/failure, concurrent writes, explicit
close, reconnect and automatic resubscription. They do not replace a
credentialed exchange integration test; any exchange-side behaviour and all
fund-moving behaviour remain unverified against a customer account.

## Source references

- [UTA REST v2 market index and adjacent account/order/position groups](https://www.kucoin.com/docs-new/v2/rest/ua/introduction)
- [KuCoin's current UTA REST v2 endpoint navigation and WebSocket v2 channel navigation](https://www.kucoin.com/docs-new/3470355w0)
- [Classic REST and Classic WebSocket navigation](https://www.kucoin.com/docs-new)
- [KuCoin change log](https://www.kucoin.com/docs-new/change-log)
- [Method-level generated coverage map](ENDPOINTS.md)
