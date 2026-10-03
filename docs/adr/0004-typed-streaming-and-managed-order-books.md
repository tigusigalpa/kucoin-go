# ADR-0004: Typed streaming packages and managed order books

- Date: 2026-10-03
- Status: accepted

## Context

The goal is for an external Go application to use this module as a standalone
KuCoin market-data source: no raw JSON parsing and no protocol logic on the
consumer's side. The previous WebSocket clients delivered `json.RawMessage`
(except for one UTA ticker), had no order-book semantics, and could not even
subscribe to several UTA channels (no interval/depth parameters).

KuCoin documents order-book maintenance as: subscribe to the incremental feed,
buffer it, fetch a REST snapshot, drop updates already contained in it, replay the
rest, and resynchronise when the sequence breaks. Verified live (Futures, 3 800
consecutive updates): `sequence` increases by exactly one, `sn == sequence`, and
the first update after a REST snapshot of sequence S has sequence S+1.

## Options

1. Keep generic raw clients and document the payloads.
2. One typed package for everything.
3. One typed package per product family (Classic Futures, Classic Spot, Classic
   Margin, UTA v2), each returning typed subscriptions and managed order books.

## Decision

Option 3 — `classic/futures/streaming`, `classic/spot/streaming`,
`classic/margin/streaming`, `uta/v2/streaming`, reachable as
`Client.Classic.Futures.Stream`, `Client.Classic.Spot.Stream`,
`Client.Classic.Margin.Stream` and `Client.UTA.V2.Stream`.

- A `Service` dials a `Session` (`DialPublic`/`DialPrivate`, UTA: `DialFutures`/
  `DialSpot`/`DialPrivate`). A `Session` has one `Subscribe…` method per channel
  returning `*stream.Subscription[T]` with a fully typed payload; the payload
  struct names the unit of every timestamp and uses `types.Decimal` for numbers.
- Channels that carry several subjects on one topic (instrument, balance,
  positions, margin positions) decode into one flat struct that mirrors the
  documented schema (all fields optional) plus a `Subject` and `Is…()` helpers.
- Documentation inconsistencies are resolved in favour of the live feed and
  recorded in the field comments (for example `/contract/announcement` is global,
  `ep`/`eq` in call-auction data, the deprecated balance subjects, and the order of
  a Futures WebSocket candle, `[start, open, close, high, low, turnover, volume]`,
  whose last two elements are the other way round from the documentation — settled
  by comparing the live push with the REST candle of the same minute).
- Managed order books use `orderbook.Syncer` and `orderbook.Book`:
  - exact-decimal levels keyed by canonical price;
  - one sequence rule for every feed — a delta applies when
    `Start <= seq+1 && End > seq`, is ignored as stale when `End <= seq`, and is a
    gap otherwise;
  - REST-snapshot mode for Classic Futures and Spot, stream-snapshot mode for UTA's
    `increment@10ms` (the server pushes a snapshot first);
  - automatic resynchronisation on a sequence gap, a reconnect (reset marker),
    updates dropped before they reached the book (gap marker), an undecodable
    update or a buffer overflow, with a bounded retry and a typed terminal error
    (`stream.ErrResyncFailed`);
  - the live book is readable from any goroutine; events are delivered in order.
- The book and its consumer are decoupled. The first implementation delivered the
  events from the goroutine that feeds the book, while holding the Syncer's lock.
  A flake hunt found the consequence: a consumer that was slow, or that called
  `State()` from its own event loop, or an early frame that raced `Start()`, could
  deadlock the Syncer, and a program that only polled `Book()` froze the book
  because it never read `C()`. Events are therefore queued under the lock (which
  fixes their order) and delivered by a goroutine of the Syncer's own with no lock
  held. When a consumer falls more than `EventQueue` (default 1024) events behind,
  the queued updates are collapsed into one `EventSnapshot` *reload marker* — the
  consumer reloads from `Event.Book` and skips updates whose `Sequence` is not newer
  — while the book keeps following the stream and no REST call is made. (`Sequence` is
  the last number an update covers; a consumer that keeps its own copy applies an update
  as the range from its copy's next sequence number to that one, which is right for the
  feeds that deliver ranges as well.) A consumer
  that must not miss an update selects `stream.FailSubscription` and gets
  `stream.ErrSlowConsumer` instead. A failure ends the subscription with its cause
  even if nobody reads (`SyncerConfig.OnFail`, wired by `DeliverTo`).
- Out of scope, documented as such: WebSocket order entry (add/cancel/amend over
  the socket) is trading, not market data; the deprecated UTA `increment` depth is
  offered only as a raw typed stream (its REST snapshot is a private endpoint and
  KuCoin replaces it with `increment@10ms`); Classic Spot's REST snapshot needs API
  credentials, so its managed book fails fast with a clear permanent error without
  them.

## Consequences

- A consumer reads typed structs, and gets reconnects, token renewal, resubscription,
  back-pressure accounting and order-book resynchronisation without writing any
  protocol code.
- ~50 channel payloads must track KuCoin's documentation; `SubscribeTyped` on the
  low-level clients remains available for channels KuCoin adds later.
- The public surface grows by four packages; the root `Client` gains a `Stream`
  field in four service groups (additive).

## Verification

- Every channel decodes the official documentation examples (fixtures) with exact
  assertions; variants prove field mapping where the examples are degenerate.
- Order-book scenarios: documented worked example, gap, reconnect, quiet symbol,
  snapshot failure, a burst that overflows the socket-to-book queue, an absent
  consumer, a consumer calling back into the Syncer, a `FailSubscription` consumer,
  undecodable update, close, concurrent readers, and a randomised slow consumer
  whose mirror must equal the live book.
- A connection "storm" test kills the connection at random moments while a dozen
  subscriptions stream and others subscribe/unsubscribe concurrently; it found a
  window in which `Subscribe` could see a connected state without a live session,
  and it asserts the ordering contract (generations never go backwards, no frame of
  a superseded connection follows a reset marker, nothing after `OnClosed`).
- Live: the managed Futures book was compared with KuCoin's REST snapshot at the
  *same sequence number* eleven times during a 70-second run over 12 431 updates:
  zero mismatches, zero resynchronisations.
