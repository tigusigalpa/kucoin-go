// futures_stream streams KuCoin Classic Futures market data as typed Go values:
// best bid/offer, trades, one-minute candles and mark/index price, for any number
// of symbols over one connection. It shows the whole lifecycle — one goroutine per
// subscription, lifecycle events, reconnects handled by the library, graceful
// shutdown on Ctrl-C — and needs no credentials.
//
// Run: go run ./examples/futures_stream -symbols XBTUSDTM,ETHUSDTM
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kucoin "github.com/tigusigalpa/kucoin-go"
	"github.com/tigusigalpa/kucoin-go/classic/futures/streaming"
	"github.com/tigusigalpa/kucoin-go/stream"
)

func main() {
	symbolsFlag := flag.String("symbols", "XBTUSDTM,ETHUSDTM", "comma-separated contract symbols")
	duration := flag.Duration("duration", 0, "stop after this long (0 = until interrupted)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	if err := run(ctx, strings.Split(*symbolsFlag, ",")); err != nil {
		stop()
		log.Fatal(err)
	}
}

// consume reads a subscription from the moment it exists, on a goroutine of its
// own: the queue behind a subscription starts filling as soon as KuCoin
// acknowledges it, so reading must not wait until every channel is subscribed.
// The goroutine ends when the subscription does.
func consume[T any](wg *sync.WaitGroup, sub *stream.Subscription[T], handle func(T)) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range sub.C() {
			handle(v)
		}
		// C is closed: the subscription ended. Err is nil after a requested end (the
		// session was closed) and holds the cause otherwise.
		if err := sub.Err(); err != nil {
			log.Printf("%s ended: %v", sub.Key(), err)
		}
	}()
}

// run returns instead of calling log.Fatal so that the deferred session.Close
// always runs: it ends every subscription, closes the socket and waits for the
// library's goroutines.
func run(ctx context.Context, symbols []string) error {
	client := kucoin.NewClient()

	// The library reconnects on its own; the handler only reports what happened.
	session, err := client.Classic.Futures.Stream.DialPublic(ctx, stream.WithEventHandler(func(ev stream.Event) {
		switch ev.Type {
		case stream.EventDisconnected:
			log.Printf("connection lost: %v", ev.Err)
		case stream.EventReconnecting:
			log.Printf("reconnect attempt %d in %s", ev.Attempt, ev.Backoff.Round(time.Millisecond))
		case stream.EventReconnected:
			log.Printf("reconnected (connection #%d); updates during the outage are not replayed", ev.Generation)
		case stream.EventOverflow:
			log.Printf("consumer too slow on %s: %d updates dropped", ev.Subscription, ev.Dropped)
		case stream.EventDecodeError:
			log.Printf("undecodable push on %s: %v", ev.Subscription, ev.Err)
		}
	}))
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	var wg sync.WaitGroup
	defer func() {
		_ = session.Close() // ends every subscription and closes their channels...
		wg.Wait()           // ...which lets the consumers finish
	}()

	var nTicks, nTrades, nCandles, nMarks atomic.Int64

	ticks, err := session.SubscribeTickerV2(ctx, symbols)
	if err != nil {
		return fmt.Errorf("ticker: %w", err)
	}
	consume(&wg, ticks, func(t streaming.TickerV2) {
		nTicks.Add(1)
		// Prices are exact decimals; Cmp, Add and friends never touch a float.
		fmt.Printf("%-9s bid %s x %d  ask %s x %d\n", t.Symbol, t.BestBidPrice, t.BestBidSize, t.BestAskPrice, t.BestAskSize)
	})

	trades, err := session.SubscribeTrades(ctx, symbols)
	if err != nil {
		return fmt.Errorf("trades: %w", err)
	}
	consume(&wg, trades, func(t streaming.Trade) {
		nTrades.Add(1)
		fmt.Printf("%-9s %-4s %d @ %s\n", t.Symbol, t.Side, t.Size, t.Price)
	})

	candles, err := session.SubscribeKlines(ctx, streaming.Interval1Min, symbols)
	if err != nil {
		return fmt.Errorf("klines: %w", err)
	}
	consume(&wg, candles, func(k streaming.Kline) {
		nCandles.Add(1)
		fmt.Printf("%-9s %s candle %s open %s high %s low %s close %s volume %s contracts turnover %s\n",
			k.Symbol, k.Interval, k.Start().UTC().Format("15:04"), k.Open, k.High, k.Low, k.Close, k.Volume, k.Turnover)
	})

	marks, err := session.SubscribeInstrument(ctx, symbols)
	if err != nil {
		return fmt.Errorf("instrument: %w", err)
	}
	consume(&wg, marks, func(m streaming.InstrumentEvent) {
		nMarks.Add(1)
		switch {
		case m.IsMarkIndexPrice():
			fmt.Printf("%-9s mark %s index %s\n", m.Symbol, m.MarkPrice, m.IndexPrice)
		case m.IsFundingRate():
			fmt.Printf("%-9s funding rate %s\n", m.Symbol, m.FundingRate)
		}
	})

	log.Printf("streaming %s; press Ctrl-C to stop", strings.Join(symbols, ", "))
	select {
	case <-ctx.Done():
		st := session.Stats()
		log.Printf("stopping: %d tickers, %d trades, %d candles, %d mark/funding updates; %d frames, %d reconnects, %d dropped",
			nTicks.Load(), nTrades.Load(), nCandles.Load(), nMarks.Load(), st.FramesReceived, st.Reconnects, st.PushesDropped)
		return nil
	case <-session.Done():
		// Reconnecting is automatic; the session only ends for good when
		// reconnecting is disabled or exhausted, or after a fatal error.
		if err := session.Err(); err != nil {
			return fmt.Errorf("session ended: %w", err)
		}
		return nil
	}
}
