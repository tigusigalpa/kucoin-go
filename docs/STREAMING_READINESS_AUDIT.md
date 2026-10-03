# Streaming readiness audit — kucoin-go as a standalone KuCoin Futures market-data source

Audit date: 2026-10-03. Subject: the working tree on top of release v1.2.0. Question
asked: can an external Go application use kucoin-go on its own as a source of **Classic
Futures market data** — without parsing raw messages and without implementing any part of
KuCoin's protocol?

## 1. Verdict

**Yes.** The application needs `kucoin.NewClient()`, a `DialPublic` call and one typed
`Subscribe…` call per channel; it receives Go structs. Every public Futures WebSocket
channel of KuCoin's current documentation (ten) and the complete public Futures market-data
REST group (seventeen methods) are covered, the connection manages its own heartbeat,
reconnects, token renewal and resubscription, and a managed local order book keeps an exact
book synchronised with the exchange. The remaining limits are listed in
[section 7](#7-residual-risks-and-out-of-scope); none of them prevents the use asked about.

The same machinery serves Classic Spot, Classic Margin and UTA v2 (see
[API_COVERAGE.md](API_COVERAGE.md)); this audit examines Futures in depth because that was
the question, and notes where the other markets differ.

## 2. Acceptance criteria and evidence

| # | Criterion | Result | Evidence |
|---|---|---|---|
| 1 | The consumer never parses raw JSON | **Met** | Every `Subscribe…` method of all four streaming services returns a typed subscription; `TestSubscriptionPayloadsNeverExposeRawJSON` walks every payload type reachable from them and fails on a `json.RawMessage`, `[]byte`, empty interface, function or channel (the walker is itself tested against a payload that breaks every rule). Prices and sizes are `types.Decimal` (exact text). |
| 2 | The consumer implements no protocol | **Met** | Tokens (`bullet-public`/`bullet-private`), welcome, ping/pong, subscribe/ack, resubscribe after a reconnect, multi-symbol topic routing, 24-hour token renewal are inside the library: `internal/wsengine` tests and `classic/futures/streaming` tests; the root tests drive everything through `kucoin.Client` (`streaming_test.go`). |
| 3 | Every current Futures channel and public REST endpoint is covered | **Met** | [CHANNELS.md](CHANNELS.md) (10 public + 6 private channels, plus the managed book) and [ENDPOINTS.md](ENDPOINTS.md) (17 methods); the manifest tests tie each row to a real method and a real test, and `API_COVERAGE.md` is computed page by page against KuCoin's documentation index. |
| 4 | Order-book semantics are correct | **Met** | The documented procedure and one sequence rule (see below); exact decimal levels keyed by canonical price; tests for the worked example, gaps, reconnects, quiet symbols, snapshot failures, malformed updates, slow and absent consumers, and a randomised comparison against a reference model; live comparison with KuCoin's own snapshot (section 5). |
| 5 | The WebSocket lifecycle is production-grade | **Met** | Section 3. |
| 6 | No data races, goroutine leaks or deadlocks | **Met** after fixes | Section 4: the whole suite runs under `-race` (Windows, and Linux in Docker with Go 1.22); every WebSocket test checks for leaked goroutines; two randomised stress tests; the defects they and two independent adversarial reviews found (section 4) are fixed, each with a regression test that was confirmed to fail without its fix. |
| 7 | Errors are typed | **Met** | `*stream.ServerError` with sentinels (`ErrTopicNotFound`, `ErrLoginRequired`, `ErrSubscriptionLimit`, `ErrSessionLimit`, `ErrRateLimited`, `ErrServiceBusy`, ...), `stream.ErrPingTimeout`, `ErrAckTimeout`, `ErrSlowConsumer`, `ErrResyncFailed`, `*stream.DecodeError`, `orderbook.ErrSequenceGap` with `*GapError`; REST errors stay reachable through `errors.As` (`*transport.KucoinError`). Local validation errors are returned before anything is sent. |
| 8 | Slow consumers are handled explicitly | **Met** | Bounded queues, three overflow policies, counters, gap markers for sequenced streams; a slow or absent *book* consumer cannot stall the book (section 4). |
| 9 | Examples and documentation | **Met** | [STREAMING.md](STREAMING.md), [README](../README.md), Godoc on every public type, five runnable examples that were run against the live public API. |
| 10 | Compatibility | **Met** | No exported signature of a pre-existing package changed (AST-level comparison with release v1.2.0: the only differing declarations are the `Logger` interfaces of the two WebSocket clients, now aliases of `stream.Logger` with identical method sets); behavioural changes are listed in the changelog. |

## 3. Lifecycle edge cases and the tests that pin them

| Situation | Behaviour | Test (package `internal/wsengine` unless noted) |
|---|---|---|
| Welcome never arrives | `stream.ErrWelcomeTimeout`, nothing leaks | `TestConn_WelcomeTimeout` |
| Token endpoint fails (transient / permanent) | typed `ErrTokenUnavailable` wrapping the REST error; permanent errors are not retried | `TestConn_ConnectErrorsAreTyped`, `TestConn_PermanentErrorStopsReconnecting`; root: `TestClassicFuturesStream_TokenFailureIsTypedAndCarriesKuCoinsError` |
| First connection retried | `WithInitialConnectAttempts` | `TestConn_InitialConnectAttemptsRetry` |
| Subscribe rejected (404, 403, 509, ...) | typed `*stream.ServerError`, no state left behind | `TestConn_SubscribeRejectedByServerIsTypedAndLeavesNoState` |
| Acknowledgement never arrives | `ErrAckTimeout` plus a best-effort unsubscribe | `TestConn_SubscribeAckTimeoutSendsBestEffortUnsubscribe` |
| Same or overlapping topic subscribed twice | `ErrAlreadySubscribed` | `TestConn_DuplicateSubscriptionIsRejected`, `TestConn_OverlappingRoutesAreRejected` |
| Silent peer | no frame within the timeout after a ping kills and reconnects the connection; any frame counts as liveness | `TestConn_PingWatchdogDetectsSilentPeerAndRecovers`, `TestConn_AnyInboundFrameCountsAsLiveness` |
| Heartbeat parameters | taken from the welcome frame or the token response, half the interval, never above 1 ping/s | `TestResolveHeartbeat`, `TestConn_HeartbeatUsesWelcomeAndEndpointParameters` |
| Connection dropped | reconnect with jittered backoff, **fresh token per attempt**, acknowledged resubscription, ordered reset marker | `TestConn_ReconnectRefreshesTokenResubscribesAndMarksReset`; root: `TestClassicFuturesStream_EveryReconnectFetchesAFreshToken` |
| Reconnect keeps failing | backoff, `MaxAttempts`, then a fatal error ends every subscription with the cause | `TestConn_ReconnectBacksOffThenGivesUpAfterMaxAttempts` |
| Resubscribe rejected after a reconnect | only that subscription ends (`EventSubscriptionFailed`) | `TestConn_ResubscribeRejectionEndsOnlyThatSubscription` |
| Subscribe while reconnecting | waits (bounded by the context) | `TestConn_SubscribeWaitsWhileReconnecting`, `TestConn_SubscribeWhileReconnectingHonoursContext` |
| Connection drops before a subscribe is acknowledged | the registered subscription is subscribed again by the reconnect and the call returns after it (or fails with the fatal cause) | `TestConn_SubscribeSurvivesTheConnectionDroppingBeforeTheAck`, `TestConn_SubscribeReportsTheCauseWhenTheConnectionDiesForGood` |
| A resubscribe KuCoin never answers | on a live connection only that subscription fails (`ErrAckTimeout`, unsubscribed at KuCoin); a connection that answers nothing is replaced | `TestConn_ResubscribeThatKuCoinNeverAnswersFailsAloneOnALiveConnection`, `TestConn_ResubscribeTimeoutOnASilentConnectionStillRetriesTheReconnect` |
| Unsubscribe, then subscribe the same topic at once | the topic stays reserved until the unsubscribe frame is written, so the new subscribe cannot be overtaken | `TestConn_NameStaysReservedUntilTheUnsubscribeFrameIsWritten` |
| Subscription closed while its resubscribe is in flight | the subscribe that reached KuCoin is cancelled afterwards | `TestConn_ASubscriptionClosedBeforeItsResubscribeWasWrittenIsStillCancelled` |
| Slow consumer ends a subscription (`FailSubscription`) / handler panics | the subscription is unsubscribed at KuCoin too | `TestConn_OverflowFailSubscription`, `TestConn_HandlerPanicEndsOnlyItsSubscription`, `TestSub_APanicInOnAbortIsContainedAndStillReleasesTheQueue` |
| Many subscriptions against KuCoin's limit on client messages (100 per 10 s) | requests are paced to 90 % of the limit, restore after a reconnect included; a request that must wait honours its context | `TestSendWindow_*`, `TestConn_SubscribesAreKeptWithinTheServersMessageLimit`, `TestConn_RestoreStaysWithinTheServersMessageLimit`, `TestConn_SubscribeWaitingForTheMessageBudgetHonoursItsContext` |
| `MaxAttempts` with an outage that follows a recovered one | each outage has its own budget | `TestConn_MaxAttemptsBudgetsEveryOutageSeparately` |
| Several `Close` calls at once / `Connect` straight after `Close` | every caller returns only once the connection is closed; a new life never receives the old life's terminal event | `TestConn_ASecondCloseWaitsForTheFirstToFinish`, `TestConn_ConnectCannotStartANewLifeWhileTheOldOneIsStillPublishingItsEnd` |
| Overflow with a stalled consumer | the queue stays bounded; the loss is reported in order, at its place | `TestMailbox_StalledConsumerUnderSustainedOverflowStaysBounded`, `TestMailbox_GapSitsWhereTheDroppedFrameWas`, `TestMailbox_DropNewestReportsTheLossEvenWhenNothingFollows` |
| A slow logger | never holds up `Subscribe`, `Close` or the reconnect | `TestConn_RoutingNeverCallsOutWhileHoldingTheRegistryLock` |
| The same topic subscribed from several goroutines (low-level clients) | one subscription, the same channel for all | `TestClient_ConcurrentSubscribesOfOneTopicShareOneSubscription` (`websocket/classic`), `TestClient_ConcurrentSubscribesOfOneSpecShareOneSubscription` (`websocket/uta`) |
| Oversized frame | the connection is killed and restored | `TestConn_OversizedFrameKillsTheConnectionAndReconnects` |
| Server error frame not tied to a request | reported as `EventServerError` | `TestConn_UnsolicitedServerErrorIsReportedAsEvent` |
| Undecodable push | counted, `EventDecodeError`, the subscription continues | `TestConn_DecodeErrorsAreCountedAndPublished`; `classic/futures/streaming` decoder tests |
| Handler panics | only that subscription ends | `TestConn_HandlerPanicEndsOnlyItsSubscription` |
| Close during connect / during a reconnect / with a blocked consumer | prompt, idempotent, waits for the goroutines, honours a deadline | `TestConn_CloseDuringConnect`, `TestConn_ShutdownDuringReconnectIsPrompt`, `TestConn_ShutdownReleasesBlockedHandlersAndLeaksNothing`, `TestConn_ShutdownHonoursItsContext` |
| Connect twice / after Close | typed errors / a fresh life | `TestConn_ConnectTwiceAndReconnectAfterClose` |
| 100+ symbols, 300 topics, 509 limits | validated locally / typed errors | `classic/futures/streaming` validation tests, `TestServerError_MapsToSentinels` (`stream`) |

## 4. Concurrency audit

**Method.** Review of every goroutine and lock; the whole suite under `go test -race`
(repeated, with `-shuffle`, with one and with four CPUs); a goroutine-leak detector after every WebSocket
test; two randomised stress tests; the suite on Linux in a container as well as on Windows;
two independent adversarial reviews, one of concurrency (engine, order books, streaming
packages) and one of data correctness (decoders, models, documentation claims, checked against
the live service); each regression test written for a finding was then confirmed to fail when
its defect is put back (a mutation check, section 6).

**Design guarantees** (details in [ARCHITECTURE.md](ARCHITECTURE.md)): one owner per output
channel, writes serialised per connection, frames tagged with their connection generation,
no lock held across user code or a blocking send, bounded queues that never block the socket
reader, graceful shutdown that waits for every goroutine.

**Defects found and fixed during this work** (each has a regression test):

1. *The previous clients* — closing or unsubscribing while the reader was sending on the
   channel (data race and `send on closed channel`); silent loss of the newest messages under
   a slow consumer; token reuse after 24 hours; ignored heartbeat parameters; ignored error
   frames; unroutable UTA channels. Replaced by the shared engine.
2. *Order-book event delivery held the Syncer's lock while blocking on the consumer.* A flake
   hunt on a second machine produced a hang: a consumer that called `State()` from its own
   event loop, a first frame racing `Start()`, or a program that only polled the book and
   never read its events could deadlock or freeze the book. Events are now queued under the
   lock and delivered by a goroutine of their own, a lagging consumer receives a reload
   marker instead of back-pressure, and a failure ends the subscription even if nobody reads
   (`TestSyncer_StartAndStateNeverWaitForTheConsumer`, `…SlowEventConsumerNeverStallsTheBook`,
   `…ReloadMarkersLetASlowMirrorCatchUp`, `…RandomOperationsFromManyGoroutines`, ...).
3. *`Subscribe` could dereference a missing session* in the instant between a connection
   dying and the client entering the reconnecting state (found by the connection-kill storm
   test, `TestConn_ChaosConnectionStorm`): the two are now one step.
4. *Frames of a superseded connection could be delivered behind a reset marker*
   (`TestConn_RouteDropsFramesOfSupersededSessions`).
5. *A reload from a book that was being rebuilt could hand a consumer an empty copy*;
   `Book.SnapshotIfReady` reports readiness atomically and the consumer protocol (reload on
   `EventSnapshot`, invalidate on `EventStale`, apply only newer updates) is documented and
   tested.
6. *Test-suite artefacts that only a fast Linux machine exposed*: unthrottled push floods
   that delay acknowledgements for minutes, and tests whose outcome depended on how many
   frames fit in a queue. Both were rewritten to assert end states.
7. *Found by the concurrency review* (all fixed, regression-tested): an unsubscribe could be
   overtaken by the subscribe of the same topic right after it (KuCoin would have applied the
   unsubscribe to the new subscription); a handler could be told twice that its subscription
   ended (an overflow racing `Close`); `Connect` straight after `Close` could receive the old
   life's terminal event and have its own event stream closed; a second concurrent `Close`
   returned before the connection was closed; `MaxAttempts` was spent by earlier, recovered
   outages; one resubscribe KuCoin never answered kept every subscription of the connection in
   an endless reconnect loop; a `Subscribe` whose connection dropped before the acknowledgement
   failed although the reconnect would have carried it; a subscription ended by an overflow, a
   handler panic or a racing `Close` stayed subscribed at KuCoin; the log and event callbacks
   ran while the registry lock was held (a slow logger froze `Subscribe`, `Close` and the
   reconnect); a lagging consumer under sustained overflow let its queue grow without bound,
   and the loss of the *newest* frames was reported in front of the backlog instead of behind
   it; a route listed twice delivered every push twice; two goroutines subscribing one topic
   through the low-level clients made one of them fail.
8. *Found by the data review* (all fixed): the Futures candle's turnover and volume were
   swapped (the live feed sends `[…, turnover, volume]`, the documentation says the opposite;
   confirmed against the REST candle of the same minute on 2026-10-03, and the UTA candle
   named `v`/`a` was confirmed the same way); `Book.Spread` and `Mid` could pair a bid and an
   ask of different states under concurrent updates; the ranged-feed recipe for keeping a
   mirror of a managed book was only right for single-sequence feeds; the Futures
   position-settlement event had no symbol; the UTA deprecated increment book fetched a
   truncated REST snapshot (`limit=FULL` was never sent); a symbol listed twice in one Futures
   request was subscribed twice; fields the live feed sends and the documentation lacks
   (`fundingRate` in the snapshot, `period` in the funding-rate push) were dropped; the REST
   kline granularities 3 and 43200 that the live endpoint serves were refused locally.
9. *Rate limit.* KuCoin allows 100 client messages per 10 s per Classic connection (UTA: 300
   public, 100 private; heartbeats count) and may drop a connection that exceeds it; the
   restore after a reconnect sent one subscribe every 10 ms. Requests are now paced to 90 %
   of the limit (`stream.WithMessageLimit` / `WithoutMessagePacing` change or disable it).

## 5. Live verification (public, read-only)

All of this was run on 2026-10-03 against KuCoin's production public API: public, read-only
endpoints only, no credentials, no order or account endpoint.

**Futures soak (240 seconds, through `kucoin.Client`, three symbols, final code).** One public
session subscribed to all ten public channels for `XBTUSDTM`, `ETHUSDTM` and `SOLUSDTM`, a second
session to the raw level-2 feed, and two managed order books:

| Channel | Updates | Plausibility checked on every update |
|---|---|---|
| ticker v2 / v1 | 4 697 / 215 | bid < ask, positive sizes, sequences never go back, known symbol |
| depth 5 / depth 50 | 7 272 / 7 265 | levels strictly sorted, at most 5 / 50 levels, top of book not crossed |
| level-2 increments | 94 568 | the sequence of every symbol advances by exactly one |
| klines (1 min) | 220 | high ≥ open, close; low ≤ open, close; **turnover ÷ contracts lies between low × multiplier and high × multiplier** (a price per contract: it would be off by orders of magnitude if the two size fields were swapped) |
| trades | 377 | taker side, positive size and price, trade id |
| instrument (mark/index price, funding rate) | 332 | positive prices, known subject |
| symbol snapshot | 144 | last price between the 24-hour low and high |
| funding settlement | 0 | (settles every eight hours; none occurred) |

Result: **0 decode errors, 0 dropped pushes, 0 reconnects, 0 anomalies** over 76 210 frames on the
main connection; the kline unit check held for **220 of 220** candles. The two managed books received
33 654 and 21 589 updates and, every eight seconds, the top 20 levels of KuCoin's own REST snapshot
were compared with the local book *at the same sequence number*: **58 of 58 comparisons identical**
(29 per book), **0 resynchronisations and 0 dropped events** on either book. Two earlier runs on the
same code agreed (150 s: 36 of 36 identical; 240 s: 59 of 59 identical, one more comparison skipped
because the local mirror never stood at the snapshot's exact sequence). Over the whole work the
Futures book matched KuCoin's snapshot in every comparison that was made.

**Public channels of the other markets (60 seconds, final code).** Every public channel of Classic
Spot, Classic Margin, UTA spot and UTA futures was subscribed through the root client — including all
UTA order-book depths (1, 5, 50 and `increment@10ms`, with and without RPI) and the managed UTA book —
and every channel that was expected to be busy delivered data: **0 decode errors, 0 dropped pushes and
0 reconnects in 185 407 Spot, 147 Margin, 7 760 UTA-spot and 8 072 UTA-futures frames**. Channels that
only speak when something happens (call auctions, funding settlement) were silent, as expected.

**Documentation versus live, settled by evidence.** The Futures WebSocket candle was compared, for the
final push of a minute, with the REST candle of the same minute: in every comparison where the REST
candle had caught up (four of seven) element 5 of the push equalled the REST *turnover* and element 6
the REST *volume* (contracts), the reverse of the documented order; in the other three (illiquid
symbols, REST still behind) the two figures still stood in the ratio of price × multiplier. The UTA
futures candle, which names its fields, was checked the same way: `v` equals the REST volume and `a`
the REST turnover (four of six exact, the rest REST lag). The raw pushes also showed fields the
documentation does not list (`fundingRate` in the 24-hour snapshot, `period` in the funding-rate
push); the REST kline endpoint accepted granularities 3 and 43200 and rejected 2 and 7 with
`300000 Unsupported granularity`.

**Examples.** `futures_market`, `futures_stream`, `futures_orderbook`, `spot_stream` and
`uta_stream` were run against the live API and exit cleanly on a signal or a deadline.

Reproduce a short version yourself: `go run ./examples/futures_stream -duration 20s` and
`go run ./examples/futures_orderbook -duration 20s`.

## 6. Verification matrix

Run on 2026-10-03 on the final tree (Windows 11, Go 1.26.4; Linux in Docker, Go 1.22.12 and
1.26.4). Everything below passed.

| Check | How | Result |
|---|---|---|
| Formatting | `gofmt -s -l .` | no output |
| Static analysis | `go vet ./...` (its `stdversion` analyser rejects any standard-library API newer than the Go 1.22 of `go.mod`), `staticcheck ./...` | clean |
| Unchecked errors | `errcheck -ignoretests ./...` | three intentional `defer …Close()` (two examples, one response body) |
| Tests | `go test -count=1 ./...` | 31 packages ok, 728 test functions (203 before this work) |
| Race detector | `go test -race -count=1 ./...` | ok |
| Race, repeated, shuffled, one and four CPUs | `go test -race -count=3 -shuffle=on -cpu 1,4` on the engine, `stream`, `orderbook`, the four streaming packages, `websocket/...` and the root package | ok |
| Linux, the minimum Go version (1.22.12), with `-race` | Docker (`golang:1.22`, offline): `go build ./... && go vet ./... && go test -race -count=1 ./...` | ok |
| Linux, Go 1.26.4, shuffled, one and four CPUs | Docker (`golang:1.26.4-alpine`, offline): `go build ./... && go vet ./... && go test -count=2 -shuffle=on -cpu 1,4 ./...` | ok |
| Regression tests really guard their defects | each fixed lifecycle, ordering, pacing and consistency defect was put back (16 mutations) and the test written for it had to fail | all 16 caught |
| Statement coverage | `go test -cover ./...` | `internal/wsengine` 93.4 %, `stream` 100 %, `orderbook` 96.1 %, `types` 96.2 %, `classic/futures/streaming` 93.8 %, `classic/futures/market` 100 %, `classic/spot/streaming` 99.3 %, `classic/margin/streaming` 100 %, `uta/v2/streaming` 98.3 %, `websocket/classic` 90.2 %, `websocket/uta` 92.3 %, root 92.5 % |
| API compatibility | AST-level comparison of every exported declaration with release v1.2.0 | additions only; the two `Logger` interfaces of the WebSocket clients are now aliases of `stream.Logger` with the same methods; `uta/v2/market.OrderBookOptions` gained the field `Full` |
| Modules | `go mod tidy` | `go.mod` and `go.sum` unchanged; no new dependency |
| Generated documentation | `go run ./internal/gendocs` | no difference; the manifest tests tie every row to a real method and test |
| Live service | section 5 | 0 decode errors, 0 drops, 0 anomalies |

Environment note: on the Windows machine used, `go build ./...` can fail to link two of the
original examples (`uta_market`, `uta_trading`) with `Access is denied` on the temporary
executable — an antivirus/file-lock artefact; `go vet ./...` covers them and they build with
`-ldflags="-s -w"` and on Linux.

## 7. Residual risks and out-of-scope

- **WebSocket order entry** (trading) is not provided, by design.
- **Private channels** (orders, balance, positions, ...) are verified against the
  documentation's worked examples and fake servers, never against a real account. Fields the
  documentation does not show for them, or shows wrongly, could therefore only be corrected
  where the public feed gave evidence (the position-settlement symbol, for instance, comes from
  the topic by design, not from an observed push).
- **No replay.** KuCoin offers none; updates lost while a connection is down are reported
  (reset markers, `EventReconnected`, order-book resynchronisation) but cannot be recovered.
- **Sharding and volume.** A session is one connection. KuCoin allows a Classic connection
  100 client messages per 10 s and 100 topics per request (Futures: any number of topics per
  connection; Spot/Margin 400; UTA 600 public, 400 private). The library paces its requests to
  that limit, so a restore of many *single-symbol* subscriptions is slow (about 9 per second,
  200 topics ≈ 20 s) — prefer multi-symbol topics, which put up to 100 symbols into one
  message — and more topics than a connection holds need more sessions, opened by the
  application.
- **Documentation drift.** Where KuCoin's documentation and the live service disagree the
  library follows the live service and records it in Godoc; a future change on KuCoin's side
  surfaces as `EventDecodeError` (counted in `Stats.DecodeErrors`) rather than as corrupt
  data, and the low-level clients' `SubscribeTyped[T]` allow a decoder to be supplied for a
  new channel.
- **Spot managed order book** needs API credentials (a signed snapshot), documented.
