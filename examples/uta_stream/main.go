// uta_stream streams KuCoin's current UTA WebSocket v2 futures market data as
// typed Go values: ticker, trades, one-minute candles, mark price and funding
// rate for any number of symbols, plus a managed local order book built from the
// 10ms incremental feed (which pushes its own snapshot first, so no REST call or
// credentials are needed). Everything runs over public connections.
//
// Run: go run ./examples/uta_stream -symbols XBTUSDTM,ETHUSDTM
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
	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/uta/v2/streaming"
)

func main() {
	symbolsFlag := flag.String("symbols", "XBTUSDTM,ETHUSDTM", "comma-separated futures symbols")
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
// own, so that its queue never waits for the other subscriptions to be created.
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

func run(ctx context.Context, symbols []string) error {
	client := kucoin.NewClient()
	session, err := client.UTA.V2.Stream.DialFutures(ctx, stream.WithEventHandler(func(ev stream.Event) {
		switch ev.Type {
		case stream.EventDisconnected:
			log.Printf("connection lost: %v", ev.Err)
		case stream.EventReconnected:
			log.Printf("reconnected (connection #%d)", ev.Generation)
		case stream.EventDecodeError:
			log.Printf("undecodable push on %s: %v", ev.Subscription, ev.Err)
		}
	}))
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	var wg sync.WaitGroup
	defer func() {
		_ = session.Close()
		wg.Wait()
	}()

	var nTicks, nTrades, nCandles, nMarks, nFunding atomic.Int64

	ticks, err := session.SubscribeTicker(ctx, symbols)
	if err != nil {
		return fmt.Errorf("ticker: %w", err)
	}
	consume(&wg, ticks, func(t streaming.Ticker) {
		nTicks.Add(1)
		fmt.Printf("%-9s bid %s x %s  ask %s x %s  last %s\n", t.Symbol, t.BestBidPrice, t.BestBidSize, t.BestAskPrice, t.BestAskSize, t.LastPrice)
	})

	trades, err := session.SubscribeTrades(ctx, symbols)
	if err != nil {
		return fmt.Errorf("trades: %w", err)
	}
	consume(&wg, trades, func(t streaming.Trade) {
		nTrades.Add(1)
		fmt.Printf("%-9s %-4s %s @ %s\n", t.Symbol, t.Side, t.Size, t.Price)
	})

	candles, err := session.SubscribeKlines(ctx, streaming.Interval1Min, symbols)
	if err != nil {
		return fmt.Errorf("klines: %w", err)
	}
	consume(&wg, candles, func(k streaming.Kline) {
		nCandles.Add(1)
		fmt.Printf("%-9s %s candle %s open %s high %s low %s close %s\n", k.Symbol, k.Interval, k.Start().UTC().Format("15:04"), k.Open, k.High, k.Low, k.Close)
	})

	marks, err := session.SubscribeMarkPrice(ctx, symbols)
	if err != nil {
		return fmt.Errorf("mark price: %w", err)
	}
	consume(&wg, marks, func(m streaming.MarkPrice) {
		nMarks.Add(1)
		fmt.Printf("%-9s mark %s index %s open interest %s\n", m.Symbol, m.MarkPrice, m.IndexPrice, m.OpenInterest)
	})

	funding, err := session.SubscribeFundingRate(ctx, symbols)
	if err != nil {
		return fmt.Errorf("funding rate: %w", err)
	}
	consume(&wg, funding, func(f streaming.FundingRate) {
		nFunding.Add(1)
		fmt.Printf("%-9s funding rate %s, next settlement %s\n", f.Symbol, f.Rate, f.NextSettlement().UTC().Format(time.RFC3339))
	})

	// A managed local order book of the first symbol. KuCoin delays the first
	// snapshot of this feed by a few seconds after the subscription.
	book, err := session.SubscribeOrderBook(ctx, symbols[0])
	if err != nil {
		return fmt.Errorf("order book: %w", err)
	}
	consume(&wg, book.Subscription, func(ev orderbook.Event) {
		if ev.Type == orderbook.EventStale {
			log.Printf("%s order book out of sync (%v); rebuilding", symbols[0], ev.Err)
		}
	})
	go func() {
		select {
		case <-book.Ready():
		case <-ctx.Done():
			return
		}
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if snap, ok := book.Book().SnapshotIfReady(3); ok {
					fmt.Printf("%s book at sequence %d: best bids %v, best asks %v\n", symbols[0], snap.Sequence, snap.Bids, snap.Asks)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	log.Printf("streaming %s; press Ctrl-C to stop", strings.Join(symbols, ", "))
	select {
	case <-ctx.Done():
		st := session.Stats()
		log.Printf("stopping: %d tickers, %d trades, %d candles, %d mark prices, %d funding rates; %d frames, %d reconnects, %d dropped",
			nTicks.Load(), nTrades.Load(), nCandles.Load(), nMarks.Load(), nFunding.Load(), st.FramesReceived, st.Reconnects, st.PushesDropped)
		return nil
	case <-session.Done():
		if err := session.Err(); err != nil {
			return fmt.Errorf("session ended: %w", err)
		}
		return nil
	}
}
