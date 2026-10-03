# Streaming guide

kucoin-go turns KuCoin's WebSocket feeds into typed Go values. An application
asks for a channel, receives `chan T` of Go structs and never parses JSON, builds
a subscribe frame, sends a ping, fetches a token or remembers to resubscribe after
a reconnect. This guide describes what the library does for you, the guarantees it
gives, and where those guarantees end. The list of channels is generated from
[internal/channels.yaml](../internal/channels.yaml) into [CHANNELS.md](CHANNELS.md).

- [Quick start](#quick-start)
- [Sessions](#sessions)
- [Subscriptions](#subscriptions)
- [Connection lifecycle](#connection-lifecycle)
- [Back-pressure and slow consumers](#back-pressure-and-slow-consumers)
- [Order books](#order-books)
- [Errors](#errors)
- [Exact numbers](#exact-numbers)
- [Observability](#observability)
- [Protocol notes](#protocol-notes)
- [KuCoin documentation versus live behaviour](#kucoin-documentation-versus-live-behaviour)
- [What is not provided](#what-is-not-provided)

## Quick start

Public market data needs no credentials. This program prints the best bid and ask
of two perpetual contracts until it is interrupted:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	kucoin "github.com/tigusigalpa/kucoin-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	client := kucoin.NewClient()
	session, err := client.Classic.Futures.Stream.DialPublic(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close() // closes every subscription and waits for the goroutines

	ticks, err := session.SubscribeTickerV2(ctx, []string{"XBTUSDTM", "ETHUSDTM"})
	if err != nil {
		log.Fatal(err)
	}
	for {
		select {
		case tick, ok := <-ticks.C():
			if !ok { // the subscription ended; Err says why (nil: requested end)
				log.Fatalf("stream ended: %v", ticks.Err())
			}
			fmt.Println(tick.Symbol, tick.BestBidPrice, tick.BestAskPrice)
		case <-ctx.Done():
			return
		}
	}
}
```

Runnable programs live in [examples/](../examples): `futures_market` (REST),
`futures_stream` (typed channels, lifecycle events, graceful shutdown),
`futures_orderbook` (a managed local order book) and one per other market.

## Sessions

A *session* is one managed WebSocket connection. Each market has a service on the
client that dials sessions:

| Service | Public session | Private session |
|---|---|---|
| `client.Classic.Futures.Stream` | `DialPublic(ctx)` | `DialPrivate(ctx)` |
| `client.Classic.Spot.Stream` | `DialPublic(ctx)` | `DialPrivate(ctx)` |
| `client.Classic.Margin.Stream` | `DialPublic(ctx)` | `DialPrivate(ctx)` |
| `client.UTA.V2.Stream` | `DialFutures(ctx)`, `DialSpot(ctx)` | `DialPrivate(ctx)` |

`Dial…` fetches a connection token (a REST call), opens the socket, waits for
KuCoin's welcome message and returns once the session is usable. Private sessions
need API credentials on the client (`kucoin.WithCredentials`); without them the
call fails at once, before any network access, with an error matching
`transport.ErrCredentialsRequired`. Every method of a session is safe for
concurrent use.

Connection options go to the client once (`kucoin.WithStreamOptions(...)`) or to a
single `Dial…` call; they are `stream.Option` values:

```go
client := kucoin.NewClient(kucoin.WithStreamOptions(
	stream.WithBufferSize(4096),
	stream.WithReconnect(stream.ReconnectPolicy{MinDelay: time.Second, MaxDelay: 30 * time.Second}),
	stream.WithEventHandler(func(ev stream.Event) { log.Println(ev.Type, ev.Err) }),
))
```

KuCoin limits a connection to 300 topics, a single request to 100 topics and the
number of concurrent connections to 50; exceeding a limit is reported as
`stream.ErrSubscriptionLimit` or `stream.ErrSessionLimit`. Open a second session
for more topics. Every subscribe method that takes a symbol list accepts at most
100 symbols per call and validates the list before sending anything
(`ErrNoSymbols`, `ErrTooManySymbols`, `ErrInvalidSymbol`).

## Subscriptions

Every `Subscribe…` method returns a `*stream.Subscription[T]`:

| Method | Meaning |
|---|---|
| `C() <-chan T` | The updates, in the order KuCoin sent them. Closed when the subscription ends, for whatever reason. |
| `Err() error` | Why it ended: `nil` while running and after a requested end (`Close`, or closing the session); otherwise the cause (`*stream.ServerError`, `stream.ErrSlowConsumer`, `stream.ErrResyncFailed`, ...). Final once `C()` is closed. |
| `Done() <-chan struct{}` | Closed as soon as the subscription has ended. |
| `Dropped() uint64` | How many updates the overflow policy discarded because the consumer was too slow. |
| `Key() string` | The topic or channel identity, for logs. |
| `Close() error` | Unsubscribes and ends the subscription. Idempotent; safe from any goroutine. |

Guarantees:

- **Order.** Updates of one subscription arrive in the order KuCoin sent them.
  Different subscriptions are independent: there is no ordering between a trade
  and a ticker.
- **Safe closing.** `C()` is closed only after its last send has returned, so `Close`
  can never race with a delivery and the channel is never written after it was
  closed.
- **No silent loss.** Updates are only ever discarded by the overflow policy
  (below), and every discard is counted.
- **No goroutine outlives the session.** `Close` waits for the connection's
  goroutines; the test-suite checks for leaks after every scenario.

Per-subscription tuning: `stream.WithBuffer(n)` and `stream.WithOverflow(policy)`
can be passed to any `Subscribe…` call.

**Start reading each subscription as soon as you create it.** The queue behind a
subscription starts filling when KuCoin acknowledges it. If you create several
subscriptions in a row and only then start consuming, the busiest ones can overflow
in the meantime — each subscribe call takes a network round trip — and the overflow
policy will drop updates (counted in `Dropped()`). The idiom is one goroutine per
subscription, started right after the subscribe call:

```go
ticks, err := session.SubscribeTickerV2(ctx, symbols)
if err != nil { return err }
go func() {
	for tick := range ticks.C() { handle(tick) }
}()
trades, err := session.SubscribeTrades(ctx, symbols)
// ...
```

A *multi-symbol* subscription (`SubscribeTickerV2(ctx, []string{"A", "B"})`) is one
subscription with one channel: updates of all symbols arrive in it, each carrying
its own `Symbol`. KuCoin routes such pushes under the topic of a single symbol; the
library expands the topic list so that every symbol is delivered.

## Connection lifecycle

```
Idle ──Dial──▶ Connecting ──▶ Connected ◀──────────────┐
                                 │                      │
                       connection lost                  │ restored
                                 ▼                      │
                           Reconnecting ────────────────┘
                                 │
     Close / fatal error         ▼
   ───────────────────▶ Closing ──▶ Closed
```

**Heartbeat.** KuCoin announces `pingInterval` / `pingTimeout` (18 s / 10 s today)
in the connection token response or the welcome message. The library pings at half
the advertised interval (never more than once per second, which KuCoin treats as
abuse) and treats *any* inbound frame — a pong or a data push — as proof of life.
If nothing arrives within the timeout after a ping, the connection is declared dead
(`stream.ErrPingTimeout`) and reconnected. A read deadline is the last-resort guard
if the heartbeat itself cannot run. `stream.WithPingInterval` /
`stream.WithPingTimeout` override KuCoin's values.

**Request pacing.** KuCoin lets a connection send 100 client messages per 10 seconds
(Classic; UTA: 300 on a public and 100 on a private connection, heartbeats included)
and may drop one that sends more. Subscribe, unsubscribe and authentication messages
are therefore paced to 90 % of that limit, the rest being kept for the heartbeats; a
request that has to wait does so until its context is done. The pacing matters most
for the restore after a reconnect, which subscribes everything again at once: 200
single-symbol subscriptions take about 20 s to restore instead of getting the
connection dropped again. A multi-symbol subscription is one message for up to 100
symbols. `stream.WithMessageLimit(n, window)` changes the budget and
`stream.WithoutMessagePacing()` turns it off.

**Reconnect.** A lost connection is restored automatically (disable with
`stream.WithAutoReconnect(false)`):

1. the delay before attempt *n* is `min(MaxDelay, MinDelay·Factor^(n-1))` reduced by
   up to `Jitter` of itself, so that many clients dropped at once do not return in
   lockstep (defaults: 500 ms, 60 s, ×2, 50 %);
2. every attempt fetches a **fresh connection token and endpoint list** — a token
   is valid for 24 hours, so reusing the first one would fail exactly when a
   long-lived client needs a reconnect;
3. after the welcome (and, on private connections, authentication) every surviving
   subscription is subscribed again with the acknowledgement checked; a
   subscription KuCoin now rejects — or never answers, on a connection that is
   otherwise alive (`stream.ErrAckTimeout`) — ends with that error and an
   `EventSubscriptionFailed`, the others continue; a connection that answers nothing
   at all is replaced instead;
4. the backoff restarts once a connection has been stable for `StableAfter` (30 s);
   `MaxAttempts` (default: unlimited) bounds the consecutive failed attempts of one
   outage, so a connection that came back and dropped again gets its attempts anew;
   a *permanent* failure — rejected API key, missing credentials — stops the loop at
   once instead of hammering the API.

Updates that happened while the connection was down are **not replayed** — KuCoin
does not offer replay. The stream tells you instead: a `Handler` (and the managed
order books) receive an ordered *reset marker* between the last update of the old
connection and the first of the new one, and `EventReconnected` carries the new
`Generation`.

**Subscribing during a reconnect** waits for the connection to come back (until the
context is done) rather than failing, and so does a subscribe whose connection drops
before KuCoin has answered it: the registered subscription is subscribed again by the
reconnect and the call returns once that has happened (or fails with the reason it
could not). Unsubscribing a topic and subscribing it again right away is safe: the
topic stays taken until the unsubscribe frame has been written, so the new subscribe
can never be overtaken by the old unsubscribe.

**Shutdown.** `session.Close()` stops reconnecting, ends every subscription (their
channels close, `Err()` is nil), sends a WebSocket close frame, closes the socket and
waits — at most 10 s — until the library's goroutines have exited.
`session.Shutdown(ctx)` is the same with your own deadline. Cancelling the context
passed to `Dial…` only bounds the dial; it does not tie the session to the context,
so close the session explicitly (`defer session.Close()`).

**Fatal errors.** When reconnecting is disabled, exhausted or pointless the session
ends: `session.Done()` is closed, `session.Err()` holds the cause and every
subscription ends with it.

## Back-pressure and slow consumers

Each subscription owns a bounded queue (default 1024 updates) between the socket
reader and your channel. The reader never blocks on a slow consumer — a stalled
consumer must not be able to starve heartbeats of the whole connection. When a
queue is full the *overflow policy* decides:

| Policy | Behaviour |
|---|---|
| `stream.DropOldest` (default) | Discard the oldest queued update; the consumer always catches up to the newest data. |
| `stream.DropNewest` | Discard the incoming update; the backlog is kept. The loss is reported behind the backlog. |
| `stream.FailSubscription` | End the subscription with `stream.ErrSlowConsumer` — for consumers that must never lose data silently. |

Drops are counted (`Subscription.Dropped`, `Stats.PushesDropped`), reported once a
second at most through `EventOverflow`, and — for sequenced streams — turned into a
*gap*: a managed order book that sees a gap resynchronises instead of continuing
on a book with a hole in it. A gap is reported in order, at the place where the
updates are missing — after the updates that preceded the loss and before those that
followed it; a run of consecutive drops is one gap. A subscription that is ended for
being too slow (`FailSubscription`), or whose handler panicked, is unsubscribed at
KuCoin too, so the exchange stops sending for it.

The managed order books add a second queue, between the book and *its* consumer;
see [the events of a managed book](#the-events-of-a-managed-book).

## Order books

KuCoin offers three kinds of book feed, and the library covers each with the right
tool.

| Need | Use | Semantics |
|---|---|---|
| Top 5 / 50 levels, no maintenance | `SubscribeDepth5`, `SubscribeDepth50` (Futures, Spot); UTA `orderbook` with depth `1`/`5`/`50` | Every push is a complete snapshot that replaces the previous one. |
| A full local book | `SubscribeOrderBook(ctx, symbol)` | A managed `orderbook.Book` built from a snapshot plus the level-2 incremental feed; resynchronises by itself. |
| The raw incremental feed | `SubscribeOrderBookChanges` | Sequenced deltas for your own bookkeeping. |

### The managed book

```go
book, err := session.SubscribeOrderBook(ctx, "XBTUSDTM")
if err != nil {
	log.Fatal(err)
}
defer book.Close()

<-book.Ready() // closed after the first synchronisation
if bid, ok := book.Book().BestBid(); ok {
	fmt.Println("best bid", bid.Price, bid.Size)
}

for ev := range book.C() {
	switch ev.Type {
	case orderbook.EventSnapshot: // (re)initialised; ev.Book is ready
	case orderbook.EventUpdate:   // ev.Changes lists the price levels that changed
	case orderbook.EventStale:    // synchronisation lost (ev.Err says why); a snapshot follows
	case orderbook.EventFailed:   // gave up; the subscription ends (see book.Err())
	}
}
```

`book.Book()` returns the live book; it is safe to read from any goroutine while
updates are applied. Prices are exact decimals in canonical form (`"84486.0"` and
`"84486"` are the same level) and sizes keep the text KuCoin sent. Helpers:
`BestBid`, `BestAsk`, `Mid`, `Spread`, `Top(n)`, `Snapshot(depth)`,
`SnapshotIfReady(depth)` (the same, but reports whether the book is ready) and
`Sequence`.

**Procedure** (KuCoin's documented one, for feeds that need a REST snapshot):

1. subscribe to the incremental feed and start buffering updates;
2. fetch the full REST snapshot (it carries a sequence number);
3. drop buffered updates the snapshot already contains, replay the rest;
4. apply every further update, requiring it to continue the sequence.

**Sequence rule.** An update covering sequence numbers `Start..End` applies when
`Start <= sequence+1` and `End > sequence`; an update that ends at or before the
current sequence is stale and ignored; an update that starts after `sequence+1`
means updates were missed (`*orderbook.GapError`, matching
`orderbook.ErrSequenceGap`). Futures and Spot deliver one number per message, UTA
delivers ranges; the same rule covers both.

**Automatic resynchronisation.** The book is cleared, an `EventStale` is emitted
(`ErrSequenceGap`, `ErrReconnected`, `ErrUpdatesDropped`, `ErrBufferOverflow`, or a
decode failure) and the procedure restarts when:

- a sequence gap is detected;
- the connection was lost and restored;
- updates were dropped before they reached the book, because this process could not
  keep up with the feed (the socket-to-book queue overflowed);
- an update could not be decoded, or too many updates piled up while the snapshot
  was being fetched.

The snapshot request is retried with backoff; after `Tuning.MaxAttempts`
consecutive failures (default 8) — or at once for a permanent error such as a
rejected API key — the subscription ends with `stream.ErrResyncFailed`. Use
`SubscribeOrderBookTuned` to change the buffer size, retry backoff and attempt limit.

UTA's `increment@10ms` depth pushes its own snapshot first and deltas afterwards, so
that book needs no REST call; Spot's REST snapshot is a signed (private) endpoint,
so a managed Spot book needs credentials on the client — see the notes below.

### The events of a managed book

The book follows the stream whether or not anybody reads `C()`: an application that
only polls `book.Book()` may ignore the events. They are delivered in order from a
queue of its own (default 1024 events, `stream.WithBuffer(n)` to change it), so a
slow reader can never stall the book, the connection or its heartbeat.

If a reader falls further behind than the queue, the queued `EventUpdate`s are
replaced by **one `EventSnapshot` marker**. The discarded updates are counted by
`Dropped()`; the book itself is unaffected and no REST call is made. A reader that
keeps its own copy of the book follows three rules:

1. on `EventSnapshot` — first synchronisation, a rebuild, or "you fell behind" — reload
   the copy from the book with `ev.Book.SnapshotIfReady(0)`; if that reports `false`
   the book is being rebuilt right now, so ignore the event (another `EventSnapshot`
   follows when the rebuild ends);
2. on `EventStale`, regard the copy as invalid until the next successful reload;
3. on `EventUpdate`, apply `ev.Changes` only when the copy is valid and `ev.Sequence`
   is greater than the copy's sequence (the reload may already contain the update).
   `ev.Sequence` is the *last* sequence number the update covers, which for the
   feeds that deliver ranges (Spot, UTA) is not the first one: apply it as the range
   from the copy's next sequence number up to `ev.Sequence`, as below.

```go
mirror := orderbook.New(symbol)
valid := false
for ev := range book.C() {
	switch ev.Type {
	case orderbook.EventSnapshot:
		snap, ok := ev.Book.SnapshotIfReady(0)
		if valid = ok; ok {
			_ = mirror.Reset(snap)
		}
	case orderbook.EventStale:
		valid = false
	case orderbook.EventUpdate:
		if valid && ev.Sequence > mirror.Sequence() {
			_, _ = mirror.Apply(orderbook.Delta{Start: mirror.Sequence() + 1, End: ev.Sequence, Changes: ev.Changes})
		}
	}
}
```

A reader that only needs the current state never has to do any of this: read
`book.Book()` whenever it wants one.

A consumer that must not miss an update can ask for the opposite behaviour:
`SubscribeOrderBook(ctx, symbol, stream.WithOverflow(stream.FailSubscription))` ends the
subscription with `stream.ErrSlowConsumer` as soon as its queue overflows.

### What the managed book does not promise

It is exactly as correct as KuCoin's feed: the library verifies the sequence, not
the exchange. A book built from the feed matched KuCoin's own REST snapshot at the
same sequence number in every comparison made while developing it (see
[ADR 0004](adr/0004-typed-streaming-and-managed-order-books.md)), but a consumer that
needs a guarantee should compare `book.Book().Sequence()` against its own checks.

## Errors

Everything works with `errors.Is` / `errors.As`.

| Error | When |
|---|---|
| `*stream.ServerError` | KuCoin rejected a request. `Code`, `Message` and `ID` are exposed; it also matches the sentinels below. |
| `stream.ErrTopicNotFound` (404), `ErrTopicInvalid` (400), `ErrTopicRequired` (406) | A subscribe was rejected for the topic. |
| `stream.ErrLoginRequired` (403) | A private topic was requested on a public connection. |
| `stream.ErrTokenInvalid` (401), `ErrAuthFailed` | The token or API key was refused. |
| `stream.ErrSubscriptionLimit`, `ErrSessionLimit`, `ErrRateLimited`, `ErrServiceBusy` | The different 509 / gateway "too many" and "busy" replies, told apart by their message. |
| `stream.ErrIncompleteCredentials` | A private session without a complete key set. |
| `stream.ErrAlreadySubscribed` | The same channel (or an overlapping one) is already subscribed on the session. |
| `stream.ErrSlowConsumer` | A `FailSubscription` consumer overflowed (a managed book: its event queue). |
| `stream.ErrResyncFailed` | A managed book could not be rebuilt. |
| `stream.ErrPingTimeout`, `ErrWelcomeTimeout`, `ErrAckTimeout` | Timeouts of the connection, welcome and request/ack steps. |
| `stream.ErrNotConnected`, `ErrClosed`, `ErrAlreadyConnected`, `ErrReconnecting` | Misuse of the connection state. |
| `stream.ErrTokenUnavailable` | The token endpoint failed; the cause (an HTTP or KuCoin error from the REST layer) is wrapped. |
| `*stream.DecodeError` | A push could not be decoded. It never ends a subscription; it is counted and reported through `EventDecodeError`. |

```go
sub, err := session.SubscribeTickerV2(ctx, []string{"NOSUCHM"})
var se *stream.ServerError
switch {
case errors.Is(err, stream.ErrTopicNotFound):
	// the symbol does not exist
case errors.As(err, &se):
	log.Printf("KuCoin said %d: %s", se.Code, se.Message)
}
_ = sub
```

Subscription failures after the subscription was established (a resubscribe that
KuCoin rejects, a slow consumer, a failed resynchronisation) end the subscription;
read `Err()` after the channel closes.

## Exact numbers

KuCoin mixes JSON strings, bare numbers (`84486.0`) and exponent forms (`1.0E-4`)
for the same kind of field, sometimes between the REST and WebSocket variants of a
single value. The payload structs therefore use:

- `types.Decimal` for prices, sizes, rates and amounts: it accepts a string, a number
  or `null`, keeps the exact text (never a `float64`), and offers `Canonical`, `Cmp`,
  `Sign`, `IsZero`, `Add`, `Sub`, `Mul`, `Rat` and `Float64`;
- `types.Int64` for integers KuCoin sometimes quotes;
- `types.ID` for identifiers that arrive as numbers or strings.

Use `Cmp` rather than `==` to compare prices: `84486.0` and `84486` are equal as
numbers but different as text.

## Observability

- `session.State()`, `session.Stats()` (state, generation, reconnects, subscriptions,
  frames received, pushes dropped, decode errors, connected-since, last frame).
- `session.Events()` — lifecycle events (`Connected`, `Disconnected`, `Reconnecting`,
  `Reconnected`, `SubscriptionFailed`, `Overflow`, `DecodeError`, `ServerError`,
  `Closed`) on a buffered channel from which the oldest event is dropped when you do
  not read it, so ignoring it is safe — or `stream.WithEventHandler(fn)` for a
  callback on a dedicated goroutine.
- `kucoin.WithLogger` / `stream.WithLogger` for diagnostics. Credentials, tokens and
  signatures are never logged.

## Protocol notes

### Classic Futures

Topics are per symbol (`/contractMarket/tickerV2:XBTUSDTM`). Connection tokens come
from `POST /api/v1/bullet-public` / `bullet-private` on the Futures host and the
socket is `wss://ws-api-futures.kucoin.com`. Ten public channels (two tickers, depth
5/50, level-2 increments, klines, trades, instrument — mark/index price and funding
rate —, funding settlement, 24-hour snapshot) and six private ones (orders, stop
orders, balance, positions, margin mode, cross leverage); private channels need
`DialPrivate`. Sizes are in contracts (lots). A Futures connection takes any number of
topics (Spot and Margin: 400), at most 100 symbols per request, and 100 client messages per
10 seconds, which the library paces itself to (see *Request pacing*).

The level-2 feed is strictly sequential (`sequence` increases by one per message) and
the first update after a REST snapshot at sequence *S* is *S+1*, which is what makes the
managed book exact. The REST full snapshot holds up to 1000 levels per side. The
`/contract/announcement` funding-settlement topic is global (it carries no symbol).

### Classic Spot

Topics are per symbol or per market (`/market/ticker:BTC-USDT`, `/spotMarket/level2Depth5:BTC-USDT`,
`/market/level2:BTC-USDT`, `/market/candles:BTC-USDT_1min`, ...); tokens come from the
`bullet-public` / `bullet-private` endpoints of the Spot host. Twelve public channels
(ticker, all tickers, symbol and market snapshots, level 1, depth 5 and 50, level-2
incremental, call-auction depth and data, klines, trades) and four private ones
(orders V2 and V1, balance, stop orders). The depth pushes carry no sequence number.

KuCoin acknowledges a subscription to a symbol or candle interval that does not exist
and then sends nothing, so intervals are validated locally; take symbol names from the
REST symbol list.

The incremental feed delivers *ranges* (`sequenceStart`..`sequenceEnd`); the managed
book applies the sequence rule above and skips rows with a zero price while the sequence
still advances, as KuCoin prescribes. **The managed Spot book is the one stream that
needs credentials:** KuCoin serves the full Spot order-book snapshot
(`GET /api/v3/market/orderbook/level2`) to signed requests only. Without credentials the
book ends at once, without a single retry, with an error that matches both
`transport.ErrCredentialsRequired` and `stream.ErrResyncFailed`; the session and its other
subscriptions are unaffected.

### Classic Margin

Margin shares its WebSocket host, tokens and several channels with Spot, so a Margin
session embeds the Spot session and has all of its methods. The Margin-specific channels
are the index price and mark price (public) and the cross- and isolated-margin position
(private); the Margin documentation reuses the Spot specifications for its order, balance
and stop-order channels, which a stop order tells apart by its trade type and a balance
update by its relation event.

### UTA WebSocket v2

A session is bound to one market host: `DialSpot`, `DialFutures` (public) or `DialPrivate`
(account data). Private sessions authenticate with a signed message after the welcome frame
and again after every reconnect (a rejected key is a permanent failure); the signature uses
the client's `Clock`. A channel that does not exist on the session's market, or a private
channel on a public session, is refused locally.

KuCoin accepts a `symbols` list for the ticker and funding-fee channels only; for the other
public channels the library sends one request per symbol, a few at a time and spaced apart,
and merges the results into the single subscription it returns — all or nothing: if one
symbol is rejected the others are unsubscribed again. A connection holds 600 topics
(public) or 400 (private) and KuCoin may disconnect one that sends more than 300 (public)
or 100 (private) messages per 10 seconds.

The managed book follows `increment@10ms`: the server pushes a snapshot first (one to five
seconds after the subscription) and deltas afterwards, so no REST call or credentials are
needed; after a gap, an undecodable update or dropped updates the book subscribes again to
obtain a fresh snapshot. Depths 1, 5 and 50 push complete snapshots and need no
maintenance; the deprecated `increment` depth is offered with a REST snapshot you provide
(the UTA order-book endpoint is private).

## KuCoin documentation versus live behaviour

Where the documentation and the live service disagree, the library follows the live
service and says so in the Godoc of the affected type:

- Futures `GET /api/v1/trade-statistics` is documented as public but answers
  `400001` without authentication; the client signs it when credentials are present
  (the transport's *optional authentication* mode).
- Futures candles on the WebSocket are `[start, open, close, high, low, turnover,
  volume]`: close comes before high/low, unlike REST, and the last two elements are
  the other way round from the documented "volume, turnover" — element 5 is the quote
  currency turnover and element 6 the number of contracts. Checked against the REST
  candle of the same minute (for example XBTUSDTM, minute 1790993280: WS
  `["…","1080991.7836","12785"]`, REST volume `12785` and turnover `1080991.7836`).
  `Kline.Turnover` and `Kline.Volume` follow the live order. The UTA futures candle
  names its fields and needs no such care: its `v` equals the REST volume and its `a` the
  REST turnover of the same minute.
- Futures `/contractMarket/snapshot` carries a `fundingRate`, and the `funding.rate`
  push of `/contract/instrument` a `period`, which the documentation does not list;
  both are decoded (`SymbolSnapshot.FundingRate`, `InstrumentEvent.Period`). The
  snapshot's `volume` is in the base currency (3938.189 BTC), not in contracts.
- Futures `GET /api/v1/kline/query` documents the granularities 1, 5, 15, 30, 60,
  120, 240, 480, 720, 1440 and 10080; the live endpoint also accepts 3 and 43200 (the
  3min and 1month candles of the WebSocket channel) and rejects every other value with
  `300000 Unsupported granularity`. `market.Granularity3Min` and `Granularity1Month`
  exist for them.
- UTA `GET /api/ua/v2/market/orderbook` requires `limit` (20, 100 or `FULL`);
  `OrderBookOptions.Full` asks for the whole book.
- Futures funding-rate REST responses use exponent notation (`1.0E-4`) and add
  `dailyInterestRate` / `lastTimeFundingRate` while omitting a documented
  `predictedValue`; the field names follow the live shape.
- Futures REST order-book levels are bare JSON numbers (`[84491.3, 1220]`), the
  WebSocket levels are strings; both decode into `orderbook.Level`.
- The balance-event subjects `orderMargin.change`, `availableBalance.change` and
  `withdrawHold.change` are deprecated by KuCoin; they are still decoded
  (`BalanceEvent.IsDeprecatedSubject` identifies them) and `walletBalance.change` is
  the current subject.
- The UTA `increment` depth is documented as deprecated from 2026-07-15 but still
  answers; `increment@10ms` is the supported replacement. The library offers both and
  flags the deprecated one in its Godoc.
- Classic Spot: the documentation's ticker example spells the time key `Time` where its
  schema says `time` (both are decoded); its candle-interval table leaves out `5min`
  although REST and the live feed have it; the push timestamp of a candle is described as
  microseconds in an example comment but is nanoseconds in the schema and on the wire.
- UTA: the mark-price gateway timestamp is shown in milliseconds in the published example
  but arrives in nanoseconds (the library tells the unit by magnitude), and several
  documented string fields arrive as bare JSON numbers (`types.Decimal` takes both).

## What is not provided

- **WebSocket order entry** (Classic add/cancel order, UTA add/cancel/amend order) is
  trading, not market data, and is not implemented. Use the REST order endpoints.
- **Replay of missed updates** — KuCoin has none. Sequenced streams resynchronise;
  other streams continue from the next push.
- **Multiplexing many sessions** — a session is one connection. Open more sessions for
  more than 300 topics; the library does not shard automatically.
