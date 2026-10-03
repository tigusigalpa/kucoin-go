// spot_stream streams KuCoin Classic Spot market data as typed Go values: best
// bid/offer, the best five levels of the order book, trades and one-minute
// candles for any number of symbols over one connection. It needs no credentials.
// (The managed local order book of Classic Spot is the one exception: KuCoin
// serves the Spot full order-book snapshot it is built from to signed requests
// only; see session.SubscribeOrderBook.)
//
// Run: go run ./examples/spot_stream -symbols BTC-USDT,ETH-USDT
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

	kucoin "github.com/tigusigalpa/kucoin-go"
	"github.com/tigusigalpa/kucoin-go/classic/spot/streaming"
	"github.com/tigusigalpa/kucoin-go/stream"
)

func main() {
	symbolsFlag := flag.String("symbols", "BTC-USDT,ETH-USDT", "comma-separated spot symbols")
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
	session, err := client.Classic.Spot.Stream.DialPublic(ctx, stream.WithEventHandler(func(ev stream.Event) {
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

	var nTicks, nDepth, nTrades, nCandles atomic.Int64

	ticks, err := session.SubscribeTicker(ctx, symbols)
	if err != nil {
		return fmt.Errorf("ticker: %w", err)
	}
	consume(&wg, ticks, func(t streaming.Ticker) {
		nTicks.Add(1)
		fmt.Printf("%-10s bid %s x %s  ask %s x %s  last %s\n", t.Symbol, t.BestBid, t.BestBidSize, t.BestAsk, t.BestAskSize, t.Price)
	})

	depth, err := session.SubscribeDepth5(ctx, symbols)
	if err != nil {
		return fmt.Errorf("depth: %w", err)
	}
	consume(&wg, depth, func(d streaming.OrderBookDepth) {
		nDepth.Add(1)
		if len(d.Bids) > 0 && len(d.Asks) > 0 {
			fmt.Printf("%-10s top of book: %s x %s / %s x %s (%d+%d levels)\n", d.Symbol, d.Bids[0].Price, d.Bids[0].Size, d.Asks[0].Price, d.Asks[0].Size, len(d.Bids), len(d.Asks))
		}
	})

	trades, err := session.SubscribeTrades(ctx, symbols)
	if err != nil {
		return fmt.Errorf("trades: %w", err)
	}
	consume(&wg, trades, func(t streaming.Trade) {
		nTrades.Add(1)
		fmt.Printf("%-10s %-4s %s @ %s\n", t.Symbol, t.Side, t.Size, t.Price)
	})

	candles, err := session.SubscribeKlines(ctx, streaming.Interval1Min, symbols)
	if err != nil {
		return fmt.Errorf("klines: %w", err)
	}
	consume(&wg, candles, func(k streaming.Kline) {
		nCandles.Add(1)
		fmt.Printf("%-10s %s candle open %s high %s low %s close %s volume %s\n", k.Symbol, k.Interval, k.Open, k.High, k.Low, k.Close, k.Volume)
	})

	log.Printf("streaming %s; press Ctrl-C to stop", strings.Join(symbols, ", "))
	select {
	case <-ctx.Done():
		st := session.Stats()
		log.Printf("stopping: %d tickers, %d depth pushes, %d trades, %d candles; %d frames, %d reconnects, %d dropped",
			nTicks.Load(), nDepth.Load(), nTrades.Load(), nCandles.Load(), st.FramesReceived, st.Reconnects, st.PushesDropped)
		return nil
	case <-session.Done():
		if err := session.Err(); err != nil {
			return fmt.Errorf("session ended: %w", err)
		}
		return nil
	}
}
