# KuCoin Golang SDK

![KuCoin Golang API SDK](https://i.postimg.cc/3xK6Brxs/kucoin-go-github.jpg)

[![CI](https://github.com/tigusigalpa/kucoin-go/actions/workflows/ci.yml/badge.svg)](https://github.com/tigusigalpa/kucoin-go/actions/workflows/ci.yml)
[![Tests](https://img.shields.io/badge/tests-go%20test%20--race-brightgreen)](https://github.com/tigusigalpa/kucoin-go/actions/workflows/ci.yml)
[![Go vet](https://img.shields.io/badge/code%20analysis-go%20vet-brightgreen)](https://github.com/tigusigalpa/kucoin-go/actions/workflows/ci.yml)
[![CodeQL](https://github.com/tigusigalpa/kucoin-go/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/tigusigalpa/kucoin-go/actions/workflows/codeql.yml)
[![codecov](https://codecov.io/gh/tigusigalpa/kucoin-go/graph/badge.svg)](https://codecov.io/gh/tigusigalpa/kucoin-go)
[![Go Reference](https://pkg.go.dev/badge/github.com/tigusigalpa/kucoin-go.svg)](https://pkg.go.dev/github.com/tigusigalpa/kucoin-go)
[![Go Version](https://img.shields.io/badge/go-%3E%3D1.22-blue)](go.mod)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**Live KuCoin market data in Go, as plain structs.** Point kucoin-go at a market
and just read: tickers, trades, candles, order books, mark prices and funding rates
arrive on ordinary Go channels, over a WebSocket connection that pings, reconnects
and resubscribes on its own.

```go
client := kucoin.NewClient()
session, err := client.Classic.Futures.Stream.DialPublic(ctx)
if err != nil {
	log.Fatal(err)
}
defer session.Close()

ticks, err := session.SubscribeTickerV2(ctx, []string{"XBTUSDTM", "ETHUSDTM"})
if err != nil {
	log.Fatal(err)
}
for tick := range ticks.C() {
	fmt.Println(tick.Symbol, tick.BestBidPrice, tick.BestAskPrice)
}
```

There is no JSON to parse, no ping loop to write, no reconnect logic to get wrong and
no API key to create. When the connection drops, the library fetches a fresh token,
reconnects, subscribes again and keeps feeding the same channel — and it tells you
that it happened.

kucoin-go is an independent, hand-written Go client for KuCoin's **UTA (Unified
Trading Account)** and **Classic (Spot, Margin, Futures)** REST and WebSocket APIs.
It is written from KuCoin's current documentation and checked against the live
service; it is not a wrapper around the official Universal SDK or any framework.

---

## Contents

- [Why this exists](#why-this-exists)
- [What you get](#what-you-get)
- [What is covered](#what-is-covered)
- [Install](#install)
- [Quick start](#quick-start)
- [Futures market data over REST](#futures-market-data-over-rest)
- [Streaming cookbook](#streaming-cookbook)
- [Working with exact numbers](#working-with-exact-numbers)
- [Credentials and security](#credentials-and-security)
- [UTA versus Classic](#uta-versus-classic)
- [Accounts and orders over REST](#accounts-and-orders-over-rest)
- [Configuration](#configuration)
- [FAQ](#faq)
- [Where to find what](#where-to-find-what)
- [A few practical tips](#a-few-practical-tips)
- [Testing and development](#testing-and-development)
- [Compatibility and migration](#compatibility-and-migration)
- [Security, risk and legal notice](#security-risk-and-legal-notice)

---

## Why this exists

KuCoin's official Universal SDK is generated for many languages and both account
models at once. That breadth has a cost: names vary from one area to the next,
fields are loosely typed, and errors look different depending on which endpoint
you called.

kucoin-go goes the other way. Every endpoint and every channel is written by hand,
tested, and shaped like the rest of the SDK, so once you have used one part you know
how the others behave. Coverage grows more slowly, but what is here does what it
says.

If you need every KuCoin endpoint today, use the official SDK. If you want a careful
Go client — above all for market data — this one is for you. And if something you
need is missing, an issue (or a pull request) is the fastest way to get it added.

---

## What you get

**Market data, typed from end to end**

- Every WebSocket channel of KuCoin's current documentation for Classic Futures,
  Classic Spot, Classic Margin and UTA has a `Subscribe…` method that returns a
  `*stream.Subscription[T]`: a channel of structs, an `Err()` that tells you why it
  ended, and a counter of anything it had to drop.
- The complete public Classic Futures market-data REST group (17 methods) and the
  complete UTA v2 market-data REST group (23 methods).
- Local order books that stay in step with the exchange: KuCoin's documented
  "snapshot plus updates" procedure, a sequence check on every update, and an
  automatic rebuild after a gap or a reconnect.

**A connection that looks after itself**

- Heartbeats at the rhythm KuCoin asks for; any incoming frame counts as a sign of life.
- Reconnects with jittered backoff and a *fresh* connection token every time (tokens
  expire after 24 hours, which is exactly when a long-running process needs one).
- Resubscribes everything after a reconnect, in order, and puts a marker into each
  stream so you know updates may be missing at that point.
- Paces its own requests so it stays inside KuCoin's limit on client messages — even
  while restoring hundreds of subscriptions at once.
- Gives every subscription its own bounded queue and an overflow policy you choose;
  a slow consumer never stalls the connection or the other subscriptions, and every
  dropped update is counted.
- `Close` ends every subscription and waits for the library's goroutines. Nothing leaks.

**Numbers you can trust**

- `types.Decimal` keeps prices and sizes exactly as KuCoin sent them — strings, bare
  numbers and `1.0E-4` alike — compares them numerically and never routes them
  through `float64`.
- Where KuCoin's documentation and the live service disagree, the library follows the
  live service and says so in the Godoc (see
  [documentation versus live behaviour](docs/STREAMING.md#kucoin-documentation-versus-live-behaviour)).

**Errors you can act on**

- Typed errors throughout: `errors.Is(err, stream.ErrTopicNotFound)`,
  `errors.As(err, &serverErr)`, `errors.Is(err, transport.ErrRateLimited)`.
- Invalid requests (an unknown candle interval, a malformed symbol, more than 100
  symbols in one subscription) are refused locally, before anything is sent.

**Safe by default**

- Only GET requests are retried automatically. Orders, cancellations, transfers and
  withdrawals are sent exactly once.
- `clientOid` is never generated for you, so retry-sensitive workflows stay under your
  control.
- Credentials, signatures and private request bodies are never logged.
- Nothing in this repository — tests, examples, CI — places a live order or moves funds.

<details>
<summary><b>Also in the box</b></summary>

- `context.Context` as the first parameter of every network call.
- An injectable `*http.Client`, `Clock` and `Logger`.
- An independent HMAC-SHA256 signer (`KC-API-SIGN` and the HMAC-signed
  `KC-API-PASSPHRASE`), checked against known-answer vectors.
- A shared `transport.Executor`: response-envelope decoding, `ResponseMeta` (HTTP
  status, KuCoin business code and message, request ID, `gw-ratelimit-*`,
  `x-in-time`/`x-out-time`) and a typed error hierarchy.
- A conservative GET-only retry policy: exponential backoff with jitter and a bounded
  total time.
- Local validation of obviously malformed order requests (a cancel without an ID, a
  batch over KuCoin's 20-item limit): `orders.ErrOrderIDOrClientOidRequired`,
  `orders.ErrTooManyBatchCancelItems`.
- Defensive decoding around confirmed documentation slips:
  `positions.Position.Value()` copes with the `positionValue`/`positionMargin` naming
  conflict, and `leverage.LeverageEntry.MarginMode` keeps KuCoin's literal `ISOLATE`
  instead of quietly "correcting" it.
- Manifest-driven coverage maps ([docs/ENDPOINTS.md](docs/ENDPOINTS.md),
  [docs/CHANNELS.md](docs/CHANNELS.md)): the build fails when a manifest row does not
  match a real method and a real test.

</details>

---

## What is covered

The short version: **every WebSocket channel, and the complete Futures and UTA
market-data REST; Spot and Margin REST and the trading side only in part.**

| Market | REST market data | WebSocket, public | WebSocket, private |
|---|---|---|---|
| Classic Futures | **Complete** — 17 methods | **Complete** — 10 channels + a managed local order book | **Complete** — 6 channels |
| Classic Spot | Partial — 8 of 20 methods | **Complete** — 12 channels + a managed local order book¹ | **Complete** — 4 channels |
| Classic Margin | Partial — 5 of 7 methods | **Complete** — 2 channels | **Complete** — 6 channels |
| UTA v2 | **Complete** — 23 methods | **Complete** — 8 channels + a managed local order book | **Complete** — 7 channels |

¹ KuCoin serves the full Spot order-book snapshot to signed requests only, so the
managed Spot book needs API credentials.

Not provided: WebSocket order entry (placing, cancelling or amending orders over the
socket is trading, not market data — use the REST order endpoints), most UTA v2
account and trading REST, the broader Classic account and funding REST, and the
specialty domains (Earn, VIP Lending, Convert, Broker, Affiliate, Copy Trading).

The exact boundary — method by method, channel by channel, with links to KuCoin's
pages — is in the [coverage matrix](docs/API_COVERAGE.md), the
[REST method map](docs/ENDPOINTS.md) and the [WebSocket channel map](docs/CHANNELS.md).
We would rather ship a small, correct surface than a large, half-tested one; if your
use case needs something that is not there yet, please
[open an issue](https://github.com/tigusigalpa/kucoin-go/issues) — real demand decides
what gets built next.

---

## Install

```bash
go get github.com/tigusigalpa/kucoin-go
```

Go 1.22 or newer. The only dependencies are `gorilla/websocket` and `yaml.v3` (the
latter only for the documentation generator and the manifest tests).

---

## Quick start

Everything in this section works without an account or API keys.

### 1. One REST call

```go
package main

import (
	"context"
	"fmt"
	"log"

	kucoin "github.com/tigusigalpa/kucoin-go"
)

func main() {
	ctx := context.Background()
	client := kucoin.NewClient() // no credentials: public market data only

	ticker, err := client.Classic.Futures.Market.GetTicker(ctx, "XBTUSDTM")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("XBTUSDTM last %s, best bid %s, best ask %s\n",
		ticker.Price, ticker.BestBidPrice, ticker.BestAskPrice)
}
```

```text
XBTUSDTM last 84587.6, best bid 84592.5, best ask 84592.6
```

Prefer the UTA side? The same call through the current UTA v2 surface:

```go
tickers, err := client.UTA.V2.Market.GetTickers(context.Background(), utav2market.TradeTypeSpot, "BTC-USDT")
if err != nil {
	log.Fatal(err)
}
fmt.Println(tickers.List[0].LastPrice)
```

### 2. A live stream

Trades of two contracts as they happen, until you press Ctrl+C:

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
	// Ctrl+C cancels ctx, and everything below winds down from there.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	client := kucoin.NewClient()
	session, err := client.Classic.Futures.Stream.DialPublic(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close() // ends every subscription and waits for the library's goroutines

	trades, err := session.SubscribeTrades(ctx, []string{"XBTUSDTM", "ETHUSDTM"})
	if err != nil {
		log.Fatal(err)
	}
	for {
		select {
		case t, ok := <-trades.C():
			if !ok { // the subscription ended; Err says why (nil when you closed it yourself)
				log.Printf("stream ended: %v", trades.Err())
				return
			}
			fmt.Printf("%s %-4s %d @ %s\n", t.Symbol, t.Side, t.Size, t.Price)
		case <-ctx.Done():
			return
		}
	}
}
```

```text
XBTUSDTM buy  2 @ 84600
XBTUSDTM buy  4 @ 84600
ETHUSDTM buy  1 @ 2679.81
ETHUSDTM sell 77 @ 2680.02
```

A *session* is one WebSocket connection, and it carries any number of subscriptions
within KuCoin's per-connection limits. Sizes on Futures are in contracts — one XBTUSDTM
contract is 0.001 BTC.

### 3. A local order book

A full order book that the library keeps in step with the exchange, printed once a
second:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	kucoin "github.com/tigusigalpa/kucoin-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	session, err := kucoin.NewClient().Classic.Futures.Stream.DialPublic(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// The library subscribes to the level-2 feed, fetches the REST snapshot,
	// replays what arrived in between and checks every sequence number from then on.
	book, err := session.SubscribeOrderBook(ctx, "XBTUSDTM")
	if err != nil {
		log.Fatal(err)
	}
	<-book.Ready() // the first snapshot is in

	every := time.NewTicker(time.Second)
	defer every.Stop()
	for {
		select {
		case <-every.C:
			snap, ok := book.Book().SnapshotIfReady(3) // a consistent copy of the best 3 levels
			if !ok || len(snap.Bids) == 0 || len(snap.Asks) == 0 {
				fmt.Println("resynchronising…")
				continue
			}
			spread, _ := snap.Asks[0].Price.Sub(snap.Bids[0].Price)
			fmt.Printf("seq %d  bid %s x %s  ask %s x %s  spread %s\n", snap.Sequence,
				snap.Bids[0].Price, snap.Bids[0].Size, snap.Asks[0].Price, snap.Asks[0].Size, spread)
		case <-book.Done():
			log.Printf("order book ended: %v", book.Err())
			return
		case <-ctx.Done():
			return
		}
	}
}
```

```text
seq 1748103244444  bid 84611.4 x 3469  ask 84611.5 x 3  spread 0.1
seq 1748103244567  bid 84611.4 x 3469  ask 84611.5 x 3  spread 0.1
seq 1748103245236  bid 84613.4 x 800  ask 84613.5 x 1  spread 0.1
```

If the stream ever skips a sequence number, or the connection drops, the book clears
itself, fetches a new snapshot and carries on. `SnapshotIfReady` reports `false`
during those few moments instead of handing you half a book.

**Runnable examples** with flags, signal handling and more output:
[futures_market](examples/futures_market/main.go) (a tour of the Futures REST data),
[futures_stream](examples/futures_stream/main.go),
[futures_orderbook](examples/futures_orderbook/main.go),
[uta_stream](examples/uta_stream/main.go),
[spot_stream](examples/spot_stream/main.go) and
[uta_v2_market](examples/uta_v2_market/main.go).

```bash
go run ./examples/futures_stream -symbols XBTUSDTM,ETHUSDTM -duration 30s
```

---

## Futures market data over REST

`client.Classic.Futures.Market` covers every public market-data endpoint of Classic
Futures. A few of them together:

```go
m := client.Classic.Futures.Market

// The contract: what one lot is worth, tick size, leverage, fees, live prices.
spec, err := m.GetSymbol(ctx, "XBTUSDTM")
if err != nil {
	return err
}
fmt.Println("one contract is", spec.Multiplier, spec.BaseCurrency, "- tick", spec.TickSize, "- max leverage", spec.MaxLeverage)

// Six hours of 15-minute candles. Volume is in contracts, Turnover in USDT.
candles, err := m.GetKlines(ctx, market.KlineOptions{
	Symbol:      "XBTUSDTM",
	Granularity: market.Granularity15Min,
	From:        time.Now().Add(-6 * time.Hour).UnixMilli(),
	To:          time.Now().UnixMilli(),
})
if err != nil {
	return err
}
for _, c := range candles {
	fmt.Println(c.Timestamp().Format("15:04"), c.Open, c.High, c.Low, c.Close, c.Volume)
}

// The best 20 levels (or 100). GetFullOrderBook returns the whole book.
book, err := m.GetPartOrderBook(ctx, "XBTUSDTM", 20)
if err != nil {
	return err
}
fmt.Println("best bid", book.Bids[0].Price, "best ask", book.Asks[0].Price, "at sequence", book.Sequence)

// Mark price, the current funding rate and three days of settled rates.
mark, err := m.GetMarkPrice(ctx, "XBTUSDTM")
if err != nil {
	return err
}
funding, err := m.GetCurrentFundingRate(ctx, "XBTUSDTM")
if err != nil {
	return err
}
fmt.Println("mark", mark.Value, "funding", funding.Value, "next settlement", funding.NextFundingTime())

history, err := m.GetPublicFundingHistory(ctx, market.FundingHistoryOptions{
	Symbol: "XBTUSDTM",
	From:   time.Now().Add(-72 * time.Hour).UnixMilli(),
	To:     time.Now().UnixMilli(),
})
if err != nil {
	return err
}
for _, p := range history {
	fmt.Println(p.Time().Format(time.DateTime), p.FundingRate)
}
```

(`market` is `github.com/tigusigalpa/kucoin-go/classic/futures/market`.)

Also there: all contracts (`GetAllSymbols`), all tickers (`GetAllTickers`), the last
100 trades (`GetTradeHistory`), the spot index, interest-rate and premium indices,
server time and service status. Candle granularities run from `Granularity1Min` to
`Granularity1Month`, including the 3-minute and one-month candles that the live API
serves although KuCoin's documentation leaves them out.

One quirk worth knowing: KuCoin documents `Get24hStats` (platform-wide 24-hour
turnover) as public, but only answers it for signed requests. The client signs it
when credentials are configured.

---

## Streaming cookbook

Short, complete recipes for the things people usually build first. Each one assumes
a `ctx`, a `client := kucoin.NewClient()` and, where it says `session`, a public
Futures session:

```go
session, err := client.Classic.Futures.Stream.DialPublic(ctx)
if err != nil {
	log.Fatal(err)
}
defer session.Close()
```

The packages used below are `streaming` (`classic/futures/streaming`), `stream`,
`orderbook` and `utastream` (`uta/v2/streaming`), all under
`github.com/tigusigalpa/kucoin-go/`.

### Several channels at once

Give every subscription its own goroutine and start reading **right after**
subscribing. A subscription's queue begins to fill the moment KuCoin acknowledges
it; if you create five subscriptions first and only then start reading, the busy
ones may overflow in the meantime.

```go
// consume reads one subscription on a goroutine of its own, starting right away, so
// that its queue never waits for the next subscription to be created.
func consume[T any](wg *sync.WaitGroup, sub *stream.Subscription[T], handle func(T)) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range sub.C() {
			handle(v)
		}
		if err := sub.Err(); err != nil {
			log.Printf("%s ended: %v", sub.Key(), err)
		}
	}()
}

func severalChannels(ctx context.Context, client *kucoin.Client) error {
	symbols := []string{"XBTUSDTM", "ETHUSDTM", "SOLUSDTM"}

	session, err := client.Classic.Futures.Stream.DialPublic(ctx)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	defer wg.Wait()       // runs second: waits until every consumer has drained
	defer session.Close() // runs first: ends the subscriptions, which closes their channels

	ticks, err := session.SubscribeTickerV2(ctx, symbols)
	if err != nil {
		return err
	}
	consume(&wg, ticks, func(t streaming.TickerV2) {
		fmt.Println(t.Symbol, "bid", t.BestBidPrice, "ask", t.BestAskPrice)
	})

	trades, err := session.SubscribeTrades(ctx, symbols)
	if err != nil {
		return err
	}
	consume(&wg, trades, func(t streaming.Trade) {
		fmt.Println(t.Symbol, t.Side, t.Size, "@", t.Price)
	})

	<-ctx.Done()
	return nil
}
```

One subscription can carry up to 100 symbols; updates for all of them arrive on the
same channel, each with its own `Symbol`.

### Only closed candles

KuCoin pushes the candle that is still open about once a second. When you only want
finished candles — to store them, say — keep the latest push per symbol and emit it
when the next period begins:

```go
candles, err := session.SubscribeKlines(ctx, streaming.Interval1Min, symbols)
if err != nil {
	log.Fatal(err)
}
// KuCoin pushes the open candle about once a second. A candle is final when
// the next one starts, so remember the latest push per symbol and emit it then.
open := map[string]streaming.Kline{}
for k := range candles.C() {
	if prev, ok := open[k.Symbol]; ok && prev.StartTime != k.StartTime {
		fmt.Printf("%s %s  O %s  H %s  L %s  C %s  %s contracts, %s USDT\n",
			prev.Symbol, prev.Start().UTC().Format("15:04"),
			prev.Open, prev.High, prev.Low, prev.Close, prev.Volume, prev.Turnover)
	}
	open[k.Symbol] = k
}
```

```text
ETHUSDTM 03:47  O 2680.21  H 2680.42  L 2680.21  C 2680.33  323 contracts, 8657.1499 USDT
XBTUSDTM 03:47  O 84614.6  H 84617.3  L 84614.6  C 84617.3  181 contracts, 15315.4598 USDT
```

A quiet contract may not trade at all in a minute, and then no candle is pushed for
it. On UTA, `Kline.First` marks the first push of every period, which makes the same
job a little easier.

### The top of the book without a local book

Often five levels are all you need. `SubscribeDepth5` (and `SubscribeDepth50`)
deliver a complete snapshot every 100 ms, with nothing to maintain:

```go
depth, err := session.SubscribeDepth5(ctx, []string{"XBTUSDTM"})
if err != nil {
	log.Fatal(err)
}
for d := range depth.C() { // every push is a complete top-5 snapshot, every 100 ms
	if len(d.Bids) == 0 || len(d.Asks) == 0 {
		continue
	}
	spread, _ := d.Asks[0].Price.Sub(d.Bids[0].Price)
	fmt.Println(d.Symbol, d.Bids[0].Price, d.Asks[0].Price, "spread", spread)
}
```

For the whole book use `SubscribeOrderBook` (see the [quick start](#3-a-local-order-book)).
Its live book is safe to read from any goroutine: `BestBid`, `BestAsk`, `Spread`, `Mid`,
`Top(n)`, `Snapshot(depth)` and `Sequence` are all there, and `Spread` and `Mid` always
pair a bid and an ask from the same moment.

### Keeping your own copy of the book

Most programs simply read `book.Book()` whenever they need it. If you keep your own
structure in step with the updates instead, follow three rules — they are what makes
the copy exact, even when your code falls behind:

```go
mirror := orderbook.New("XBTUSDTM")
valid := false
for ev := range book.C() {
	switch ev.Type {
	case orderbook.EventSnapshot: // first sync, a rebuild, or "you fell behind"
		snap, ok := ev.Book.SnapshotIfReady(0)
		if valid = ok; ok {
			_ = mirror.Reset(snap)
		}
	case orderbook.EventStale: // the stream broke; a new snapshot follows
		valid = false
	case orderbook.EventUpdate:
		if valid && ev.Sequence > mirror.Sequence() {
			_, _ = mirror.Apply(orderbook.Delta{Start: mirror.Sequence() + 1, End: ev.Sequence, Changes: ev.Changes})
		}
	}
}
```

1. On `EventSnapshot`, reload from `ev.Book`. A consumer that falls more than the
   queue behind gets one of these instead of thousands of updates.
2. On `EventStale`, treat your copy as invalid until the next snapshot.
3. On `EventUpdate`, apply only what is newer than your copy.

### Mark price, index price and funding

`/contract/instrument` carries two kinds of update; the event tells you which one
you have:

```go
instruments, err := session.SubscribeInstrument(ctx, []string{"XBTUSDTM", "ETHUSDTM"})
if err != nil {
	log.Fatal(err)
}
for ev := range instruments.C() {
	switch {
	case ev.IsMarkIndexPrice(): // about once a second
		fmt.Println(ev.Symbol, "mark", ev.MarkPrice, "index", ev.IndexPrice)
	case ev.IsFundingRate(): // about once a minute
		fmt.Println(ev.Symbol, "funding rate", ev.FundingRate)
	}
}
```

And the settlements themselves, for every contract at once:

```go
settled, err := session.SubscribeFundingSettlement(ctx) // every contract, every settlement
if err != nil {
	log.Fatal(err)
}
for s := range settled.C() {
	if s.Subject == streaming.SubjectFundingEnd {
		fmt.Println(s.Symbol, "settled at", s.FundingRate)
	}
}
```

`SubscribeSnapshot` adds 24-hour statistics (last price, high, low, change, volume,
turnover and the current funding rate) every five seconds.

### Knowing what the connection is doing

You don't have to watch the connection — but you can. Pass an event handler when
dialling, and read the counters whenever you like:

```go
session, err := client.Classic.Futures.Stream.DialPublic(ctx, stream.WithEventHandler(func(ev stream.Event) {
	switch ev.Type {
	case stream.EventDisconnected:
		log.Printf("connection lost: %v", ev.Err)
	case stream.EventReconnecting:
		log.Printf("reconnecting: attempt %d after %v", ev.Attempt, ev.Backoff)
	case stream.EventReconnected:
		log.Printf("back online (connection #%d)", ev.Generation)
	case stream.EventOverflow:
		log.Printf("%s is read too slowly: %d updates dropped so far", ev.Subscription, ev.Dropped)
	}
}))
if err != nil {
	log.Fatal(err)
}
defer session.Close()

st := session.Stats()
fmt.Printf("state %v, reconnects %d, frames %d, dropped %d, decode errors %d\n",
	st.State, st.Reconnects, st.FramesReceived, st.PushesDropped, st.DecodeErrors)
```

Without a handler, the same events are available on `session.Events()` — a buffered
channel that quietly discards the oldest event when nobody reads it, so ignoring it
is safe.

### When you must not lose a single update

By default a subscription that falls behind drops its *oldest* queued updates and
keeps going: you always see the newest data. If you would rather stop than miss
anything:

```go
trades, err := session.SubscribeTrades(ctx, symbols,
	stream.WithBuffer(10_000),                    // a deeper queue for this subscription only
	stream.WithOverflow(stream.FailSubscription), // never drop quietly: end the subscription instead
)
if err != nil {
	log.Fatal(err)
}
for t := range trades.C() {
	_ = t // store the trade
}
if errors.Is(trades.Err(), stream.ErrSlowConsumer) {
	// We fell behind and the stream was ended. Subscribe again and
	// reconcile the gap from REST (GetTradeHistory).
}
```

### Shutting down

`session.Close()` ends every subscription (their channels close and `Err()` returns
nil), sends a WebSocket close frame and waits up to ten seconds for the library's
goroutines. With your own deadline:

```go
ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
defer cancel()
if err := session.Shutdown(ctx); err != nil {
	log.Printf("shutdown did not finish in time: %v", err)
}
```

The context you pass to `Dial…` only bounds the dial itself — it does not tie the
session's lifetime to it — so always close the session explicitly.

### UTA v2 streams

UTA sessions are bound to a market: `DialFutures`, `DialSpot`, or `DialPrivate` for
account data. The channels look the same as on Classic:

```go
session, err := client.UTA.V2.Stream.DialFutures(ctx) // DialSpot for "BTC-USDT"-style symbols
if err != nil {
	return err
}
defer session.Close()

tickers, err := session.SubscribeTicker(ctx, []string{"XBTUSDTM", "ETHUSDTM"})
if err != nil {
	return err
}
top5, err := session.SubscribeOrderBookUpdates(ctx, []string{"XBTUSDTM"}, utastream.Depth5)
if err != nil {
	return err
}
book, err := session.SubscribeOrderBook(ctx, "XBTUSDTM") // from increment@10ms: no REST call, no keys
if err != nil {
	return err
}
```

UTA's managed book needs neither a REST call nor credentials: the server sends a
snapshot first and the changes after it. Trades, candles, mark price, funding rates
(one symbol or all of them) and call-auction data are there as well.

### Spot and Margin

Classic Spot works the same way:

```go
session, err := client.Classic.Spot.Stream.DialPublic(ctx)
if err != nil {
	return err
}
defer session.Close()

tickers, err := session.SubscribeTicker(ctx, []string{"BTC-USDT", "ETH-USDT"})
if err != nil {
	return err
}
for t := range tickers.C() {
	fmt.Println(t.Symbol, "bid", t.BestBid, "ask", t.BestAsk, "last", t.Price)
}
```

A Margin session (`client.Classic.Margin.Stream`) has every Spot method plus the
margin index and mark prices — and, on a private session, cross and isolated margin
positions.

### Private channels

Orders, fills, balances and positions need credentials on the client:

```go
client := kucoin.NewClient(kucoin.WithCredentials(kucoin.Credentials{
	APIKey:        os.Getenv("KUCOIN_API_KEY"),
	APISecret:     os.Getenv("KUCOIN_API_SECRET"),
	APIPassphrase: os.Getenv("KUCOIN_API_PASSPHRASE"),
	APIKeyVersion: os.Getenv("KUCOIN_API_KEY_VERSION"),
}))
session, err := client.Classic.Futures.Stream.DialPrivate(ctx)
if err != nil {
	log.Fatal(err) // transport.ErrCredentialsRequired when they are missing, before any network access
}
defer session.Close()

orders, err := session.SubscribeOrders(ctx, "") // "" = every symbol
if err != nil {
	log.Fatal(err)
}
for ev := range orders.C() {
	fmt.Println(ev.Symbol, ev.Type, ev.Status, ev.OrderID, ev.Price, ev.FilledSize)
}
```

Private channels are checked against KuCoin's documented examples and fake servers;
nobody's real account was used to test them.

### Errors you can act on

```go
_, err := session.SubscribeTrades(ctx, []string{"NOSUCHM"})
var rejected *stream.ServerError
switch {
case errors.Is(err, stream.ErrTopicNotFound):
	// KuCoin does not know the symbol
case errors.As(err, &rejected):
	log.Printf("KuCoin said %d: %s", rejected.Code, rejected.Message)
}

book, err := session.SubscribeOrderBook(ctx, "XBTUSDTM")
if err != nil {
	log.Fatal(err)
}
<-book.Done()
if errors.Is(book.Err(), stream.ErrResyncFailed) {
	// the REST snapshot could not be fetched, several times in a row
}
if errors.Is(book.Err(), transport.ErrCredentialsRequired) {
	// a Spot book: KuCoin serves its snapshot to signed requests only
}
```

The full list — rate limits, session limits, login required, timeouts — is in
[docs/STREAMING.md](docs/STREAMING.md#errors).

### A channel this release does not know yet

KuCoin adds channels from time to time. You don't have to wait for a new release:
the low-level clients decode whatever you tell them to, and you still get the managed
connection, reconnects and resubscription.

```go
// A channel KuCoin added after this release: decode it yourself and still get the
// managed connection, reconnects and resubscription.
type newChannelUpdate struct {
	Symbol string        `json:"symbol"`
	Value  types.Decimal `json:"value"`
}

func lowLevel(ctx context.Context, session *streaming.Session) {
	updates, err := classic.SubscribeTyped(ctx, session.Client(), "/contractMarket/someNewTopic:XBTUSDTM", false,
		func(m *classic.Message) (newChannelUpdate, bool, error) {
			var u newChannelUpdate
			err := json.Unmarshal(m.Data, &u)
			return u, err == nil, err
		})
	if err != nil {
		log.Fatal(err)
	}
	for u := range updates.C() {
		fmt.Println(u.Symbol, u.Value)
	}
}
```

(`classic` is `websocket/classic`; for UTA use `websocket/uta` and `uta.SubscribeTyped`.)

---

## Working with exact numbers

Prices and sizes in the new packages are `types.Decimal`: the exact text KuCoin
sent, with arithmetic that never goes through a float.

```go
price := tick.BestBidPrice // types.Decimal: exactly the text KuCoin sent

if c, err := price.Cmp("84500"); err == nil && c > 0 { // numeric: "84500.0" equals "84500"
	fmt.Println("above 84.5k")
}
notional, err := price.Mul("0.001") // exact Add, Sub and Mul
if err != nil {
	return err
}
f, _ := price.Float64() // for charts and statistics, where a float is fine
r, _ := price.Rat()     // a *big.Rat when you need division
fmt.Println(notional, f, r)
```

Compare prices with `Cmp`, not `==`: `"84486.0"` and `"84486"` are the same price
written two ways, and KuCoin uses both.

---

## Credentials and security

Read `KC-API-KEY`, `KC-API-SECRET` and `KC-API-PASSPHRASE` from environment variables
or your own secret store — never hardcode them:

```go
client := kucoin.NewClient(kucoin.WithCredentials(kucoin.Credentials{
	APIKey:        os.Getenv("KUCOIN_API_KEY"),
	APISecret:     os.Getenv("KUCOIN_API_SECRET"),
	APIPassphrase: os.Getenv("KUCOIN_API_PASSPHRASE"),
	APIKeyVersion: os.Getenv("KUCOIN_API_KEY_VERSION"), // per-key metadata, not a constant
}))
```

- Create keys with the minimum permission you need (read-only for market and account data).
- Restrict the key to specific IP addresses where KuCoin allows it.
- The library never logs `KC-API-KEY`, `KC-API-SIGN`, `KC-API-PASSPHRASE` or full private
  request bodies.
- It never places live trading, transfer or withdrawal calls in its own tests, examples
  or CI.

See [SECURITY.md](SECURITY.md) to report a vulnerability.

---

## UTA versus Classic

KuCoin runs two account models with different permissions, hosts and data shapes.
**UTA** is the newer unified account (spot, margin and futures together); **Classic**
is the older setup with separately funded Spot, Margin and Futures accounts. They are
not interchangeable at the API level, so this SDK keeps them as separate roots —
`client.UTA` and `client.Classic.Spot`, `.Margin`, `.Futures` — instead of pretending
they are one thing.

```go
// UTA
utaTickers, err := client.UTA.Market.GetTickers(ctx, market.TradeTypeSpot, "BTC-USDT")
// Classic Spot
spotTicker, err := client.Classic.Spot.Market.GetTicker(ctx, "BTC-USDT")
```

Don't assume a UTA and a Classic response for "the same" thing share a shape. Candles
are the classic trap: UTA and Classic Spot both return arrays, but in different field
orders (see the docblocks on `uta/market.Kline` and `classic/spot/market.Kline`).

Not sure which model your account uses? Check your KuCoin account settings.

---

## Accounts and orders over REST

**Public market data** (no credentials):

```go
trades, err := client.UTA.V2.Market.GetTrades(ctx, utav2market.TradeTypeSpot, "BTC-USDT")
if err != nil {
	log.Fatal(err)
}
for _, t := range trades.List {
	fmt.Println(t.Side, t.Size, "@", t.Price)
}
```

Note that KuCoin serves the UTA REST order book (`UTA.V2.Market.GetOrderBook`, and the
older `UTA.Market.GetOrderBook`) to signed requests only, although it is market data —
the client tells you so with `transport.ErrCredentialsRequired` before sending anything.

**Authenticated account data:**

```go
overview, err := client.UTA.Account.GetOverview(ctx)
if err != nil {
	log.Fatal(err)
}
fmt.Println("available margin:", overview.AvailableMargin)
```

**A test-safe order flow with an explicit `clientOid`** (never generated for you — read
the [security, risk and legal notice](#security-risk-and-legal-notice) before running this
against a funded account):

```go
ref, err := client.UTA.Orders.PlaceOrder(ctx, orders.PlaceOrderRequest{
	TradeType: "SPOT",
	Symbol:    "BTC-USDT",
	Side:      "BUY",
	OrderType: "LIMIT",
	Size:      "0.001",
	SizeUnit:  "BASECCY",
	Price:     "10000", // deliberately far below market so it won't fill
	ClientOid: "my-app-order-0001",
})
if err != nil {
	log.Fatal(err)
}

_, err = client.UTA.Orders.CancelOrder(ctx, orders.CancelOrderRequest{
	TradeType: "SPOT", Symbol: "BTC-USDT", OrderID: ref.OrderID,
})
```

**Classic Spot** (a separate, differently shaped account model — see
[UTA versus Classic](#uta-versus-classic)):

```go
spotRef, err := client.Classic.Spot.Orders.PlaceOrder(ctx, classicspotorders.PlaceOrderRequest{
	Type:      "limit",
	Symbol:    "BTC-USDT",
	Side:      "buy",
	Price:     "10000", // deliberately far below market so it won't fill
	Size:      "0.001",
	ClientOid: "my-app-classic-order-0001",
})
if err != nil {
	log.Fatal(err)
}
_, err = client.Classic.Spot.Orders.CancelOrderByID(ctx, spotRef.OrderID, "BTC-USDT")
```

**Classic Spot stop order** (an older order family than the HF orders above —
`/api/v1/stop-order`, not `/api/v1/hf/orders`):

```go
stopRef, err := client.Classic.Spot.Orders.AddStopOrder(ctx, classicspotorders.StopOrderRequest{
	Symbol:    "BTC-USDT",
	Side:      "sell",
	Type:      "limit",
	StopPrice: "45000", // triggers once the last trade price crosses this
	Price:     "44900",
	Size:      "0.001",
	Stop:      "loss", // "loss" (<=) or "entry" (>=)
})
if err != nil {
	log.Fatal(err)
}
_, err = client.Classic.Spot.Orders.CancelStopOrderByID(ctx, stopRef.OrderID)
```

**Classic Spot OCO order** (a limit leg paired with a stop-limit leg —
`/api/v3/oco/order`; here `ClientOid` is required, unlike `AddStopOrder`):

```go
ocoRef, err := client.Classic.Spot.Orders.AddOCOOrder(ctx, classicspotorders.OCOOrderRequest{
	Symbol:     "BTC-USDT",
	Side:       "sell",
	ClientOid:  "my-app-oco-order-0001",
	Price:      "50000", // the limit-order leg
	Size:       "0.001",
	StopPrice:  "45000", // triggers the stop-limit leg
	LimitPrice: "44900",
})
if err != nil {
	log.Fatal(err)
}
// GetOCOOrderByID returns a flat summary; GetOCOOrderDetails returns the two legs.
_, err = client.Classic.Spot.Orders.CancelOCOOrderByID(ctx, ocoRef.OrderID)
```

**Classic Futures** (yet another host and schema; a seed set of order and position
calls — there is no cancel endpoint yet):

```go
futuresRef, err := client.Classic.Futures.Orders.PlaceOrder(ctx, classicfuturesorders.PlaceOrderRequest{
	ClientOid: "my-app-futures-order-0001",
	Side:      "buy",
	Symbol:    "XBTUSDTM",
	Price:     "10000", // deliberately far below market so it won't fill
	Size:      1,
})
if err != nil {
	log.Fatal(err)
}
positions, err := client.Classic.Futures.Positions.GetPositionDetails(ctx, "XBTUSDTM")
```

**Classic Margin** (shares the Classic Spot host; `tradeType` tells cross from isolated
on the order-listing calls, `isIsolated` does it everywhere else):

```go
marginRef, err := client.Classic.Margin.Orders.PlaceOrder(ctx, classicmarginorders.PlaceOrderRequest{
	ClientOid:  "my-app-margin-order-0001",
	Symbol:     "BTC-USDT",
	Side:       "buy",
	Price:      "10000", // deliberately far below market so it won't fill
	Size:       "0.001",
	IsIsolated: true,
})
if err != nil {
	log.Fatal(err)
}
_, err = client.Classic.Margin.Orders.CancelOrderByID(ctx, marginRef.OrderID, "BTC-USDT")

borrowRef, err := client.Classic.Margin.Debit.Borrow(ctx, classicmargindebit.BorrowRequest{
	Currency: "USDT", Size: "100", TimeInForce: "FOK",
})
```

Margin has its own stop-order and OCO-order families under
`/api/v3/hf/margin/stop-order` and `/api/v3/hf/margin/oco-order` — distinct from Classic
Spot's endpoints, not shared with them. The margin mode is chosen with `IsIsolated` on
these two Add calls (not `TradeType`, which belongs to the listing endpoints):

```go
marginStopRef, err := client.Classic.Margin.Orders.AddStopOrder(ctx, classicmarginorders.StopOrderRequest{
	Symbol:     "BTC-USDT",
	Side:       "sell",
	Type:       "limit",
	StopPrice:  "45000",
	Price:      "44900",
	Size:       "0.001",
	IsIsolated: true,
})
```

**Cursor pagination:**

```go
page, err := client.UTA.Orders.GetOrderHistory(ctx, "SPOT", orders.GetOrderHistoryOptions{PageSize: 50})
for err == nil && page.LastID != 0 {
	// process page.Items...
	page, err = client.UTA.Orders.GetOrderHistory(ctx, "SPOT", orders.GetOrderHistoryOptions{LastID: page.LastID, PageSize: 50})
}
```

**Errors and rate limits:**

```go
_, err := client.UTA.Market.GetTickers(ctx, market.TradeTypeSpot, "BTC-USDT")
if errors.Is(err, transport.ErrRateLimited) {
	// back off
}
var kucoinErr *transport.KucoinError
if errors.As(err, &kucoinErr) {
	fmt.Println(kucoinErr.HTTPStatus, kucoinErr.Code, kucoinErr.Message)
}
```

Package aliases used above: `orders` (`uta/orders`), `market` (`uta/market`),
`utav2market` (`uta/v2/market`), `classicspotorders` (`classic/spot/orders`),
`classicfuturesorders` (`classic/futures/orders`), `classicmarginorders`
(`classic/margin/orders`), `classicmargindebit` (`classic/margin/debit`).

---

## Configuration

### Client options

| Option | What it does | Default |
|---|---|---|
| `WithCredentials(Credentials)` | API key, secret, passphrase and key version | none (public data only) |
| `WithUTABaseURL(string)` / `WithClassicBaseURL(string)` / `WithClassicFuturesBaseURL(string)` | Override the REST hosts | `https://api.kucoin.com` / `https://api.kucoin.com` / `https://api-futures.kucoin.com` |
| `WithHTTPClient(*http.Client)` | Your own HTTP client (proxy, TLS, transport) | `&http.Client{Timeout: 15s}` |
| `WithTimeout(time.Duration)` | Timeout of the internally built HTTP client | `15s` |
| `WithSiteType(SiteType)` | `X-SITE-TYPE` header (`global` / `australia`) | `global` |
| `WithEnableNS(bool)` | Ask for nanosecond response timing (`kc-enable-ns: true`) | `false` |
| `WithClock(Clock)` | Your own clock (tests, clock-skew correction) | system clock |
| `WithLogger(Logger)` | Structured logger; never sees credentials | no-op |
| `WithRetryPolicy(*RetryPolicy)` | Replace the GET-only retry policy | `NewDefaultRetryPolicy()` |
| `WithStreamOptions(...stream.Option)` | Default options of every WebSocket session (below) | see below |
| `WithUTAWebSocketHosts(Hosts)` | Point UTA sessions at other hosts (a proxy, a test server) | KuCoin's hosts |

### Connection options

Pass them to `kucoin.WithStreamOptions(...)` for every session, or to a single
`Dial…(ctx, ...)` call:

| Option | What it does | Default |
|---|---|---|
| `stream.WithReconnect(ReconnectPolicy)` | Backoff between reconnect attempts and when to give up | 0.5 s doubling to 60 s, 50 % jitter, forever; starts over after 30 s of stable connection |
| `stream.WithAutoReconnect(false)` | No reconnects: a lost connection ends the session | reconnect on |
| `stream.WithInitialConnectAttempts(n)` | Retry the very first connection too | 1 attempt |
| `stream.WithConnectTimeout(d)` | Token, dial, welcome and authentication of one attempt | 15 s |
| `stream.WithAckTimeout(d)` | Wait for KuCoin to confirm a subscribe or unsubscribe | 10 s |
| `stream.WithWriteTimeout(d)` / `stream.WithReadLimit(n)` | One frame write / the largest accepted frame | 10 s / 8 MiB |
| `stream.WithPingInterval(d)` / `stream.WithPingTimeout(d)` | Override KuCoin's heartbeat | KuCoin's (18 s / 10 s); pings every 9 s |
| `stream.WithMessageLimit(n, window)` / `stream.WithoutMessagePacing()` | How fast requests may go out | 90 % of KuCoin's limit (Classic 100 per 10 s, UTA 300 public / 100 private) |
| `stream.WithBufferSize(n)` | Queue per subscription | 1024 updates |
| `stream.WithOverflowPolicy(p)` | `DropOldest`, `DropNewest` or `FailSubscription` | `DropOldest` |
| `stream.WithEventHandler(fn)` / `stream.WithEventBuffer(n)` | Lifecycle events as a callback / the size of `Events()` | channel of 128 |
| `stream.WithDialer(d)` / `stream.WithHeader(h)` | WebSocket proxy, TLS, handshake headers | gorilla's default dialer (proxy from the environment) |
| `stream.WithLogger(l)` | Connection diagnostics | the client's logger |

Per subscription, `stream.WithBuffer(n)` and `stream.WithOverflow(p)` override the
queue size and the overflow policy.

For example, deeper queues, a calmer reconnect and gentler pacing:

```go
client := kucoin.NewClient(kucoin.WithStreamOptions(
	stream.WithBufferSize(4096),                  // default queue per subscription
	stream.WithOverflowPolicy(stream.DropOldest), // the default: keep the newest data
	stream.WithReconnect(stream.ReconnectPolicy{ // retry forever, at most once a minute
		MinDelay: time.Second,
		MaxDelay: time.Minute,
	}),
	stream.WithMessageLimit(50, 10*time.Second), // be gentler than KuCoin's 100 per 10 s
))
```

Behind a corporate proxy — REST and WebSocket each take their own transport:

```go
proxyURL, _ := url.Parse("http://proxy.internal:3128")
client := kucoin.NewClient(
	kucoin.WithHTTPClient(&http.Client{ // REST
		Timeout:   15 * time.Second,
		Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)},
	}),
	kucoin.WithStreamOptions(stream.WithDialer(&websocket.Dialer{ // WebSocket
		Proxy:            http.ProxyURL(proxyURL),
		HandshakeTimeout: 15 * time.Second,
	})),
)
```

(`websocket` here is `github.com/gorilla/websocket`.)

---

## FAQ

**Do I need API keys?**
Not for public market data, REST or WebSocket. You need them for private channels and
for account and trading calls — and in three market-data corners where KuCoin itself
insists on a signature: the managed Spot order book (its REST snapshot is signed), the
Futures platform-wide 24-hour statistics (`Get24hStats`), and the UTA REST order book.

**What happens when my connection drops?**
The session notices (a socket error, or no answer to a ping), waits a short, jittered
moment, fetches a fresh token, reconnects and subscribes everything again; your
channels simply keep delivering. Updates published while you were offline are gone —
KuCoin offers no replay — so the library says so: `EventDisconnected` and
`EventReconnected` on the event stream, a reset marker in every subscription, and
managed order books rebuild themselves from a new snapshot. Usually your code does
nothing at all.

**`Dropped()` keeps growing. What does that mean?**
Your code reads that subscription more slowly than KuCoin writes it. Read every
subscription on its own goroutine, keep the per-update work small, or give it a deeper
queue with `stream.WithBuffer(n)`. If you can't afford to lose anything, use
`stream.WithOverflow(stream.FailSubscription)` and handle `stream.ErrSlowConsumer`.

**Why `stream.ErrAlreadySubscribed`?**
A session holds each topic once. KuCoin identifies subscriptions by topic, so two of
them on the same topic (or on overlapping symbols) would let one unsubscribe silently
stop the other. Reuse the first subscription, or open a second session.

**A reconnect with hundreds of subscriptions takes a while. Why?**
KuCoin allows a Classic connection 100 client messages per 10 seconds (UTA: 300 public,
100 private) and may drop a connection that sends more. The library paces itself to 90 %
of that, so restoring 200 single-symbol subscriptions takes about 20 seconds. Put many
symbols into one subscription instead — one message carries up to 100 of them.

**How do I know the local order book is right?**
Every update carries a sequence number and must continue the book exactly; a gap or a
reconnect triggers a rebuild from a fresh snapshot (`EventStale`, then
`EventSnapshot`). While this release was being prepared, the managed Futures book was
compared with KuCoin's own REST snapshot at the same sequence number again and again —
every comparison matched. `book.Resyncs()` and `book.State()` show how it is doing.

**Is it safe to use from many goroutines?**
Yes. Sessions, subscriptions and books are safe for concurrent use, and `book.Book()`
can be read from anywhere while updates are applied.

**The Futures candle's volume and turnover look swapped compared with KuCoin's docs.**
It is the documentation that has them swapped. The live feed sends
`[…, turnover, volume]`, and `Kline.Volume` (contracts) and `Kline.Turnover` (quote
currency) follow the live feed — checked against the REST candle of the same minute.

**Does it work behind a proxy?** Yes — see [Configuration](#configuration).

---

## Where to find what

- [docs/STREAMING.md](docs/STREAMING.md) — the streaming guide: lifecycle, back-pressure,
  order books, errors, protocol notes, and where KuCoin's documentation and the live
  service disagree.
- [docs/CHANNELS.md](docs/CHANNELS.md) — every WebSocket subscription: topic, payload
  type, test and KuCoin page.
- [docs/ENDPOINTS.md](docs/ENDPOINTS.md) — every REST method, with its KuCoin page.
- [docs/API_COVERAGE.md](docs/API_COVERAGE.md) — complete / partial / absent, per market.
- [docs/STREAMING_READINESS_AUDIT.md](docs/STREAMING_READINESS_AUDIT.md) — what was
  checked before calling the streaming stack ready, and how.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and the [ADRs](docs/adr) — how it is built
  and why.

The coverage maps are generated from [internal/endpoints.yaml](internal/endpoints.yaml)
and [internal/channels.yaml](internal/channels.yaml); the build fails if a row does not
match a real method. KuCoin's own documentation map:
[`llms.txt`](https://www.kucoin.com/docs-new/llms.txt).

Before wiring up a new integration, skim the maps — they tell you at a glance whether
what you need already exists, or whether you would be the first to ask for it.

---

## A few practical tips

1. **Keep money exact.** Numeric fields are `string` (older REST packages) or
   `types.Decimal` (Futures market data and every streaming payload). Compare prices
   with `Cmp`, not `==`, and avoid `strconv.ParseFloat` where precision matters.
2. **Check the maps before assuming coverage.** KuCoin documenting an endpoint does not
   mean this SDK has it yet — [docs/ENDPOINTS.md](docs/ENDPOINTS.md) and
   [docs/CHANNELS.md](docs/CHANNELS.md) say exactly what exists.
3. **Pass contexts with deadlines** to REST calls; a slow request can outlive its
   usefulness. For streams, the context of `Dial…` only bounds the dial — close the
   session explicitly.
4. **Check errors by type**, with `errors.Is` and `errors.As`, never by matching strings.
5. **Read every subscription as soon as you create it**, each on its own goroutine (see
   [several channels at once](#several-channels-at-once)).
6. **Mind the retry policy.** Only GET requests are retried. `PlaceOrder`, `CancelOrder`
   and every other POST fire once — if you need retry safety on writes, build it
   yourself (a stable `clientOid` plus a query-before-retry check).

---

## Testing and development

```bash
go test ./...              # unit tests: offline, against httptest REST servers and scriptable fake WebSocket servers
go test -race ./...        # the same under the race detector (CI always runs it)
go vet ./...               # also fails when a standard-library API newer than go.mod's Go 1.22 is used
go run ./internal/gendocs  # regenerate docs/ENDPOINTS.md and docs/CHANNELS.md after editing a manifest
```

The WebSocket stack is tested against fake KuCoin servers — every lifecycle edge case, a
randomised connection-kill storm and a goroutine-leak check after every test — and the
regression tests of this release were checked by putting each fixed defect back and
watching its test fail. Public market data was compared with the live service; see
[docs/STREAMING_READINESS_AUDIT.md](docs/STREAMING_READINESS_AUDIT.md). No test, example
or CI job places a live trading, transfer or withdrawal call, and the examples touch
public, read-only endpoints only.

---

## Compatibility and migration

This project follows [SemVer](https://semver.org/); [CHANGELOG.md](CHANGELOG.md) lists what
changed in every release.

Upgrading to v1.3.0 is a `go get github.com/tigusigalpa/kucoin-go@v1.3.0` away: no exported
signature changed. The low-level `websocket/classic` and `websocket/uta` clients keep
their API (their `Logger` is now an alias of `stream.Logger`, with the same methods) and
pick up behaviour fixes you may notice: UTA subscriptions deliver only their own symbol,
channels that were never routed before are delivered, a rejected subscription fails at
once with a typed error instead of after a timeout, `Close` waits for the client's
goroutines, and a slow consumer now loses its *oldest* queued update rather than the
newest. The changelog lists every change.

kucoin-go is not a drop-in replacement for the official
[KuCoin Universal SDK](https://github.com/Kucoin/kucoin-universal-sdk) — method names,
types and error handling are intentionally different.

---

## Security, risk and legal notice

This is an unofficial, community-maintained client, provided **as is**, with no warranty.
Nothing here is financial advice. You alone are responsible for complying with the laws
and regulations of your jurisdiction and for the safety of your API credentials and
funds. Always try new code with read-only permissions or a small amount before trusting
it with a funded account.

---

## Contributing and roadmap

See [CONTRIBUTING.md](CONTRIBUTING.md) for the workflow and the roadmap. Contributions
that add a missing endpoint, tighten test coverage or make the documentation more
accurate are especially welcome — a good first contribution is usually a single REST
endpoint with its fixtures and a manifest row.

---

## Getting help

- Found a bug, or missing an endpoint you need?
  [Open an issue](https://github.com/tigusigalpa/kucoin-go/issues).
- Found a security issue? Please follow [SECURITY.md](SECURITY.md) instead of opening a
  public issue.
- Questions about scope or the roadmap are welcome as issues too — the project is small
  enough that direct feedback shapes what gets built next.

---

## License

MIT. See [LICENSE](LICENSE).

## Author

Igor Sazonov — [@tigusigalpa](https://github.com/tigusigalpa) — sovletig@gmail.com

## Links

- [KuCoin API documentation map](https://www.kucoin.com/docs-new/llms.txt)
- [Repository](https://github.com/tigusigalpa/kucoin-go)
- [Issues](https://github.com/tigusigalpa/kucoin-go/issues)

---

*Not affiliated with KuCoin. Check [docs/API_COVERAGE.md](docs/API_COVERAGE.md) before relying
on any endpoint.*
