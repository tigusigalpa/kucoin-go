# KuCoin API Coverage Matrix

**Documentation snapshot:** 2026-10-03. This matrix is reconciled page by page
against the index of KuCoin's current documentation
([llms.txt](https://www.kucoin.com/docs-new/llms.txt)), not against legacy
endpoint names or another SDK. The counts below are computed by comparing every
API page of that index with the `doc_url` of every row of the two manifests; the
generated [REST method map](ENDPOINTS.md) and [WebSocket channel map](CHANNELS.md)
are the method-level sources of truth for what this repository implements, and
the root package's manifest tests fail the build if a row does not resolve to a
real method.

Status meanings:

- **Full** — every page KuCoin currently lists for that group has a typed SDK
  method (REST) or typed subscription (WebSocket), with tests.
- **Partial** — at least one listed page is intentionally absent; the missing pages
  are named.
- **Absent** — nothing in that group is implemented.

"Full" does not mean that a credential-protected production request has been
exercised with a real account: CI deliberately never uses customer credentials or
moves funds. Public data was verified against the live service.

## At a glance: market data per market

| Market | REST market data | WebSocket market data | WebSocket account channels |
|---|---|---|---|
| **Classic Futures** | **Full** — 15 market-data methods + 2 public funding-rate methods | **Full** — 10 public channels, typed, plus a managed local order book | **Full** — 6 private channels |
| **Classic Spot** | Partial — 8 of 20 methods | **Full** — 12 public channels, typed, plus a managed local order book (its REST snapshot needs credentials) | **Full** — 4 private channels |
| **Classic Margin** | Partial — 5 of 7 methods (+ the risk-limit page) | **Full** — 2 public channels | **Full** — 6 private channels |
| **UTA (v2)** | **Full** — 23 of 23 methods | **Full** — 8 public channels, typed, plus a managed local order book | **Full** — 7 private channels |

Every WebSocket channel of the current documentation for these four markets has a
typed subscription; the only WebSocket pages not implemented are the order-entry
("WS trade") pages, which are trading, not market data (see
[Not provided](#not-provided)). The channel list with topics, payload types and
tests is [CHANNELS.md](CHANNELS.md); how the connections behave is described in
[STREAMING.md](STREAMING.md).

## REST

### UTA REST v2

| Group in current KuCoin docs | Status | Notes |
|---|---|---|
| Market Data (23 pages) | **Full** | `Client.UTA.V2.Market`: announcements, currencies, ticker/instruments/klines/orderbook/trades, index/funding/open-interest data, risk tiers, platform stats, fiat/custody/service/KYC/IP helpers. The v2 order-book endpoint is private (`General` permission); the other methods need no credentials. |
| Account & Funding (18), Sub Account (9), Deposit (2), Transfer (2), Withdraw (4) | **Absent** | The retained `Client.UTA.Account` and `Leverage` services preserve v1 compatibility only; their endpoints are in KuCoin's abandoned UTA REST section and are not counted here. Fund-moving endpoints are absent on purpose rather than guessed. |
| Orders (10), Positions (6) | **Absent** | The retained v1 order and position services are not counted as v2 coverage. |
| Affiliate (5), VIP Lending (3) | **Absent** | |

### Classic Spot

| Group | Status | Implemented / missing |
|---|---|---|
| Market Data (20 pages) | Partial — 8 | Implemented: all symbols, currency, ticker, all tickers, klines, part and full order book, server time. **Missing:** Get Announcements, Get All Currencies, Get Symbol, Get Trade History, Get Call Auction Part OrderBook, Get Call Auction Info, Get Fiat Price, Get 24hr Stats, Get Market List, Get Client IP Address, Get Service Status, Get KYC Regions. Note that the full order book is a signed endpoint although it is market data. |
| Orders (37 pages) | Partial — 27 | Implemented: Add Order, Add Order Test, Batch Add Orders, Cancel Order By OrderId / ClientOid, Cancel All Orders, Get Order By OrderId, Get Open / Closed Orders, Get Trade History, Get / Set DCP; stop orders (add, cancel by id / clientOid, batch cancel, list, get by id / clientOid); OCO orders (add, cancel by id / clientOid, batch cancel, get by id / clientOid, detail, list). **Missing:** the *Sync* add/cancel variants (Add Order Sync, Batch Add Orders Sync, Cancel Order By OrderId Sync, Cancel Order By ClientOid Sync), Cancel Partial Order, Cancel All Orders By Symbol, Modify Order, Get Order By ClientOid, Get Symbols With Open Order, Get Open Orders By Page. |

### Classic Margin

| Group | Status | Implemented / missing |
|---|---|---|
| Market Data (7) | Partial — 5 | Implemented: cross and isolated symbols, mark price list and detail, margin config. **Missing:** Get Margin Collateral Ratio, Get Market Available Inventory. |
| Risk Limit (1) | **Full** | `GetRiskLimitCross` / `GetRiskLimitIsolated` (one endpoint, two response shapes). |
| Orders (26) | Partial — 24 | Implemented: Add Order, cancel by OrderId / ClientOid, Cancel All Orders By Symbol, Get Open / Closed Orders, Get Trade History, Get Order By OrderId / ClientOid; stop orders (add, cancel by id / clientOid, batch cancel, list, get by id / clientOid); OCO orders (add, cancel by id / clientOid, batch cancel, get by id / clientOid, detail, list). **Missing:** Add Order Test, Get Symbols With Open Order. |
| Debit (7) | **Full** | Borrow, repay, borrow/repay/interest histories, modify leverage multiplier. |
| Credit (7) | **Absent** | Lending purchase/redeem endpoints. |

### Classic Futures

| Group | Status | Implemented / missing |
|---|---|---|
| Market Data (15) | **Full** | `Client.Classic.Futures.Market`: contract, all contracts, ticker, all tickers, full and part order book, trade history, klines, mark price, spot index price, interest-rate index, premium index, 24-hour platform statistics (served to signed callers only), server time, service status. |
| Funding Fees (3) | Partial — 2 | Current funding rate and public funding history. **Missing:** Get Private Funding History (private). |
| Orders (17) | Partial — 4 | Place, place-test, get-by-id, list. **Missing:** Batch Add Orders, Add Take Profit And Stop Loss Order, Cancel (by OrderId, by ClientOid, batch, all), Cancel All Stop Orders, Get Order By ClientOid, Get Recent Closed Orders, Get Stop Order List, Get Open Order Value, Get Recent Trade History, Get Trade History. |
| Positions (18) | Partial — 4 | Position details and list, margin mode, position mode. **Missing:** margin/position mode switches, max open size, positions history, max withdraw margin, cross-margin leverage get/modify, add/remove isolated margin, cross/isolated risk limit and requirement calls. |

### Not implemented domains

Classic Account Info (Account & Funding 12, Sub Account 7, Sub Account API 4, Deposit 3,
Withdrawals 5, Transfer 2, Trade Fee 3), Earn (12), VIP Lending, Affiliate, Broker
(24), Copy Trading (15) and Convert (11) have no implementation.

## WebSocket

The four streaming services are `client.Classic.Futures.Stream`,
`client.Classic.Spot.Stream`, `client.Classic.Margin.Stream` and
`client.UTA.V2.Stream`.

| Current KuCoin channel family | Status | Typed subscriptions |
|---|---|---|
| Classic Futures — public (10 pages) | **Full** | ticker v2 and v1 (deprecated), depth 5 and 50, level-2 incremental, klines, trades, instrument (mark/index price and funding rate), funding settlement, 24-hour snapshot — plus `SubscribeOrderBook`, a managed local book built from the level-2 feed and the REST snapshot |
| Classic Futures — private (6) | **Full** | orders, stop orders, balance, positions, margin mode, cross leverage |
| Classic Spot — public (12) | **Full** | ticker, all tickers, symbol and market snapshots, level 1, depth 5 and 50, level-2 incremental, call-auction depth and data, klines, trades — plus a managed local order book |
| Classic Spot — private (4) | **Full** | orders v2 and v1, balance, stop orders |
| Classic Margin — public (2) | **Full** | index price, mark price |
| Classic Margin — private (6) | **Full** | orders v2 and v1, balance, stop orders (the spot topics), cross and isolated margin position |
| UTA WebSocket v2 — public (8) | **Full** | ticker, klines, trades, order book (depth 1, 5, 50, increment, increment@10ms; with and without RPI), mark price, funding rate, all funding rates, call auction — plus a managed local order book |
| UTA WebSocket v2 — private (7) | **Full** | orders, executions, executions lite, balance, positions, liquidation warning, leverage |

### Not provided

- **WebSocket order entry** — Classic "Add Order / Cancel Order" (2 pages) and UTA
  "WS trade": add, cancel and amend order (3 pages). They place and cancel orders,
  which is trading rather than market data; use the REST order endpoints.
- The deprecated UTA order-book depth `increment` is provided (with a caller-supplied
  REST snapshot), but KuCoin documents its removal; `increment@10ms` is the
  supported replacement.

## Where the live service differs from the documentation

The library follows the live service and records each case in the Godoc of the
affected type or method; the list is in [STREAMING.md](STREAMING.md#kucoin-documentation-versus-live-behaviour)
and the REST discrepancies are in the field comments of
`classic/futures/market`.

## Lifecycle and verification boundary

Every WebSocket connection is managed by one shared engine: heartbeat from the
advertised intervals, reconnect with jittered exponential backoff and a **fresh
token per attempt**, ordered reset markers for the subscriptions, bounded queues
with explicit overflow policies, graceful shutdown that waits for every goroutine,
typed errors. The engine and every typed channel are covered by unit tests against
scriptable fake KuCoin servers (including a randomised connection-kill "storm" and
goroutine-leak checks) and run under the race detector; public Futures data and
order books were additionally compared with the live service. See
[ARCHITECTURE.md](ARCHITECTURE.md) and the
[streaming readiness audit](STREAMING_READINESS_AUDIT.md).

They do not replace a credentialed exchange integration test: the private channels
(orders, balances, positions) are tested against the documentation's worked
examples and fake servers only, and all fund-moving behaviour remains unverified
against a customer account.

## Source references

- [KuCoin documentation index (llms.txt)](https://www.kucoin.com/docs-new/llms.txt)
- [UTA REST v2 market index and adjacent groups](https://www.kucoin.com/docs-new/v2/rest/ua/introduction)
- [UTA WebSocket v2 introduction](https://www.kucoin.com/docs-new/websocket-api/introduction)
- [Classic WebSocket introduction](https://www.kucoin.com/docs-new/websocket-api/base-info/introduction)
- [KuCoin change log](https://www.kucoin.com/docs-new/change-log)
- [Generated REST map](ENDPOINTS.md) · [generated channel map](CHANNELS.md)
