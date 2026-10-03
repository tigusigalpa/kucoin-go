# ADR-0002: One connection engine behind every WebSocket client

- Date: 2026-10-03
- Status: accepted

## Context

`websocket/classic` and `websocket/uta` each carried their own copy of the
connection lifecycle. An audit against KuCoin's current documentation and the
live public feeds found defects that could not be fixed one clone at a time
without diverging further:

- closing a subscription channel while the reader goroutine was sending to it
  (a data race and a `send on closed channel` panic in production);
- multi-symbol Classic topics (`/market/ticker:A,B`) were never routed, because
  pushes carry the topic of a single symbol;
- UTA pushes were fanned out by `channel.TRADETYPE` only, so every ticker
  subscription received every symbol, and `mark-price`, `funding-fee*` and
  `lw` pushes, whose type has no `.TRADETYPE` suffix, were never delivered;
  `kline`/`obu` could not even be requested (no interval/depth parameters);
- server error frames (`{"type":"error","code":404,...}`) were ignored, so a
  rejected subscription hung until a 10 s timeout with an untyped error;
- reconnecting reused the original bullet token although tokens expire after
  24 hours and KuCoin closes the connection then;
- the `pingInterval`/`pingTimeout` that KuCoin advertises were ignored;
- a slow consumer silently lost the newest messages, which corrupts sequenced
  streams such as order books, with no counter and no signal.

## Options

1. Patch both clients in place.
2. Extract the lifecycle into an internal engine and keep the two public clients
   (and their exported APIs) as thin dialect adapters.
3. Replace them with a third-party WebSocket framework.

## Decision

Option 2: `internal/wsengine`, driven by a small `Protocol` interface (endpoint,
frame classification, ping frame, post-welcome step). The public clients keep
every exported identifier; `stream` (public) holds the shared vocabulary:
states, events, typed errors, `Subscription[T]`, `Handler`, `Config`.

Key properties of the engine:

- **Ownership.** One socket reader per connection; one delivery goroutine per
  subscription is the only goroutine that sends to, and the only one that closes,
  its output channel, so close/send races are impossible by construction. (A
  managed order book adds a second sender, the Syncer's event goroutine; the
  subscription's goroutine closes the channel only after waiting for it, see
  [ADR-0004](0004-typed-streaming-and-managed-order-books.md).)
- **Routing.** Subscriptions register routing keys (topic per symbol for Classic;
  lower-cased push type + depth + symbol + interval for UTA, with a wildcard twin
  for non-symbol channels). Two subscriptions may not share a key: KuCoin keys
  its own subscriptions the same way, so unsubscribing one would silently stop the
  other.
- **Request/acknowledgement correlation by id**, so rejections and timeouts are
  typed (`*stream.ServerError` mapped to sentinels, `stream.ErrAckTimeout`).
- **Heartbeat.** Ping at half the advertised interval (the documentation only
  requires one message per interval, and timers lose to jitter), never faster than
  1/s; any inbound frame counts as liveness; no frame within the advertised
  timeout after a ping kills the connection. A read deadline is the last resort.
- **Reconnect.** Jittered exponential backoff that resets only after a connection
  proved stable; the endpoint/token provider is invoked for every attempt, so a
  fresh token is fetched and instance servers rotate; permanent errors (missing
  credentials, rejected key) stop the loop instead of retrying forever.
- **Resubscription** is acknowledged, parallel but bounded, and preceded by an
  ordered *reset marker* in each subscription's queue so a consumer learns, in
  order, that updates may have been missed. A rejection ends only that
  subscription, and so does a missing reply on a connection that is demonstrably
  alive (frames kept arriving): one subscription KuCoin never answers must not hold
  every other one in an endless reconnect loop. A `Subscribe` whose connection drops
  before the acknowledgement is carried over the reconnect the same way.
- **Request pacing.** KuCoin limits the client messages of a connection (Classic
  100 per 10 s; UTA 300 public, 100 private) and may drop one that exceeds it. A
  protocol declares its limit (`MessageLimiter`) and the session keeps its
  subscribe, unsubscribe and authentication messages within 90 % of it with a
  sliding window; heartbeats are counted but never delayed. The restore after a
  reconnect, which used to send 100 messages per second, is what needs it most.
  `stream.Config.MessageLimit` overrides or disables it.
- **Back-pressure.** Bounded queue per subscription; policy `DropOldest`
  (default), `DropNewest` or `FailSubscription`; every drop is counted
  immediately and reported in order as a *gap marker* to handlers that need it.
  The marker is written into the queue at the place of the loss (a run of drops is
  one marker), so a consumer hears about it after the updates that preceded the
  loss and before those that followed it, also when nothing follows.
- **Ending a subscription.** Exactly one caller wins the right to end it; its
  handler is told once. Delivery stops at once but the topic stays taken until the
  unsubscribe frame has been written, so a new subscribe frame for the same topic
  can never be overtaken by the old unsubscribe (KuCoin keys subscriptions by
  topic and would apply it to the new one). A subscription ended by an overflow or
  a handler panic is unsubscribed at KuCoin as well.
- **Shutdown.** `Close` ends every subscription, sends a close frame and waits for
  all goroutines (bounded), also for a second concurrent caller; a panic in a
  handler (data or `OnAbort`) ends only its subscription. Nothing calls out — logger,
  event handler, unsubscribe — while the registry lock is held.
- **Consistent view of the connection.** Dropping a dead session and entering the
  reconnecting state are one step, and frames still being read from a superseded
  connection are discarded once its replacement is current, so a subscriber never
  sees old-connection data after its reset marker. (Both properties were added
  after a randomised connection-kill test found the windows.)

## Consequences

- Both clients gain the same behaviour and are tested by one lifecycle suite plus
  small dialect suites. The exported API of the existing packages is unchanged
  (checked with an AST-level API diff: the only differing declaration is `Logger`,
  now an alias of `stream.Logger` with the identical method set).
- Observable behaviour changes (all bug fixes, listed in the changelog): symbol
  filtering of UTA pushes, delivery of previously unrouted channels, fast typed
  rejections, `Close` waiting for goroutines, default overflow policy dropping the
  oldest instead of the newest update, heartbeat derived from KuCoin's parameters.
- Cost: an internal package of ~2k lines (without tests) that must be kept
  protocol-agnostic; most of it is lifecycle and ordering rules that used to be
  implemented twice, and wrongly.

## Verification

- `internal/wsengine` tests (mock WebSocket server with scripted behaviour:
  welcome timeout, ack/nack, token refresh per reconnect, resubscribe rejection,
  ping watchdog, overflow policies, shutdown with a stuck handler, concurrent
  subscribe/unsubscribe/close) run under `-race` with a goroutine-leak checker
  (`internal/wstest.CheckLeaks`); coverage > 90 %.
- Regression tests for every audited defect in `websocket/classic` and
  `websocket/uta`.
- Live smoke runs against KuCoin's public Futures feeds (Classic and UTA): 40+
  seconds of stable heartbeat, typed 404/403 rejections, all channels routed.
- Two independent adversarial reviews (concurrency and data correctness) of the
  engine and the streaming packages produced 13 and 7 findings; every one was fixed
  with a regression test. Each engine regression test was then checked by re-introducing
  its defect (mutation check) and confirming the test fails: the name reservation
  during unsubscribe, the `finish`/`Connect` ordering, the per-outage `MaxAttempts`,
  the once-only abort, the restore of an unanswered subscription, the `Subscribe`
  that outlives a drop, the logging outside the lock, the best-effort unsubscribes,
  the mailbox compaction and the request pacing.
