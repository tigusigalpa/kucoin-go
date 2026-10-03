// futures_orderbook keeps an exact local order book of a Classic Futures contract
// and prints its best levels once a second. The library builds the book from the
// REST snapshot and the level-2 incremental stream, checks the sequence of every
// update and rebuilds the book on its own after a gap, a reconnect or a slow
// consumer; this program only reads it and reports when that happens. No
// credentials are needed.
//
// Run: go run ./examples/futures_orderbook -symbol XBTUSDTM -depth 5
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	kucoin "github.com/tigusigalpa/kucoin-go"
	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
)

func main() {
	symbol := flag.String("symbol", "XBTUSDTM", "contract symbol")
	depth := flag.Int("depth", 5, "levels to print per side")
	duration := flag.Duration("duration", 0, "stop after this long (0 = until interrupted)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	if err := run(ctx, *symbol, *depth); err != nil {
		stop()
		log.Fatal(err)
	}
}

func run(ctx context.Context, symbol string, depth int) error {
	client := kucoin.NewClient()
	session, err := client.Classic.Futures.Stream.DialPublic(ctx)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer session.Close()

	// SubscribeOrderBook subscribes to the incremental feed, fetches the snapshot,
	// replays what arrived meanwhile and keeps the book in step with the sequence.
	book, err := session.SubscribeOrderBook(ctx, symbol)
	if err != nil {
		return fmt.Errorf("order book: %w", err)
	}
	defer book.Close()

	select {
	case <-book.Ready():
	case <-book.Done():
		return fmt.Errorf("order book ended before it was ready: %w", book.Err())
	case <-ctx.Done():
		return nil
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var updates uint64
	for {
		select {
		case ev, ok := <-book.C():
			if !ok {
				if err := book.Err(); err != nil {
					if errors.Is(err, stream.ErrResyncFailed) {
						return fmt.Errorf("could not rebuild the book (snapshot unavailable): %w", err)
					}
					return fmt.Errorf("order book ended: %w", err)
				}
				return nil
			}
			switch ev.Type {
			case orderbook.EventUpdate:
				updates++
			case orderbook.EventSnapshot:
				// First synchronisation, a rebuild after a stale event, or a reload
				// marker for a consumer that fell behind.
				log.Printf("book (re)loaded at sequence %d", ev.Sequence)
			case orderbook.EventStale:
				// Not an error: the library is already rebuilding the book.
				log.Printf("book out of sync (%v); rebuilding", ev.Err)
			}
		case <-ticker.C:
			printBook(book.Book(), depth, updates, book.Resyncs())
		case <-ctx.Done():
			return nil
		}
	}
}

func printBook(b *orderbook.Book, depth int, updates, resyncs uint64) {
	if !b.Ready() {
		fmt.Println("-- resynchronising --")
		return
	}
	// A consistent copy: bids and asks belong to the same sequence number.
	snap := b.Snapshot(depth)
	fmt.Printf("-- %s sequence %d, %d updates applied, %d resyncs --\n", snap.Symbol, snap.Sequence, updates, resyncs)
	for i := len(snap.Asks) - 1; i >= 0; i-- {
		fmt.Printf("  ask %12s  %8s\n", snap.Asks[i].Price, snap.Asks[i].Size)
	}
	// The spread comes from the same snapshot, so it fits the levels around it.
	if len(snap.Asks) > 0 && len(snap.Bids) > 0 {
		if spread, err := snap.Asks[0].Price.Sub(snap.Bids[0].Price); err == nil {
			fmt.Printf("  spread %s\n", spread)
		}
	}
	for _, l := range snap.Bids {
		fmt.Printf("  bid %12s  %8s\n", l.Price, l.Size)
	}
}
