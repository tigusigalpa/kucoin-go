package kucoin

import (
	"context"
	"fmt"
	"strconv"

	classicfuturesmarket "github.com/tigusigalpa/kucoin-go/classic/futures/market"
	classicfuturesstreaming "github.com/tigusigalpa/kucoin-go/classic/futures/streaming"
	classicspotmarket "github.com/tigusigalpa/kucoin-go/classic/spot/market"
	classicspotstreaming "github.com/tigusigalpa/kucoin-go/classic/spot/streaming"
	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/types"
	utav2market "github.com/tigusigalpa/kucoin-go/uta/v2/market"
	utav2streaming "github.com/tigusigalpa/kucoin-go/uta/v2/streaming"
)

// The adapters below connect the REST market-data clients to the snapshot sources
// of the managed order books of the streaming services: a book is built from a REST
// snapshot and the incremental WebSocket feed, and the streaming packages only know
// the neutral orderbook.Snapshot.

// futuresBookSnapshot adapts the public full-order-book call of Classic Futures.
func futuresBookSnapshot(m *classicfuturesmarket.Client) classicfuturesstreaming.SnapshotFunc {
	return func(ctx context.Context, symbol string) (orderbook.Snapshot, error) {
		book, err := m.GetFullOrderBook(ctx, symbol)
		if err != nil {
			return orderbook.Snapshot{}, err
		}
		return orderbook.Snapshot{
			Symbol:   symbol,
			Sequence: book.Sequence.Value(),
			Bids:     book.Bids,
			Asks:     book.Asks,
		}, nil
	}
}

// spotBookSnapshot adapts the full-order-book call of Classic Spot. KuCoin serves
// it to signed requests only, so without credentials the call fails with
// transport.ErrCredentialsRequired; the streaming package turns that into a
// permanent, clearly worded error of the order book.
func spotBookSnapshot(m *classicspotmarket.Client) classicspotstreaming.SnapshotFunc {
	return func(ctx context.Context, symbol string) (orderbook.Snapshot, error) {
		book, err := m.GetFullOrderBook(ctx, symbol)
		if err != nil {
			return orderbook.Snapshot{}, err
		}
		seq, err := strconv.ParseInt(book.Sequence, 10, 64)
		if err != nil {
			return orderbook.Snapshot{}, fmt.Errorf("kucoin: spot order book %s: sequence %q is not an integer: %w", symbol, book.Sequence, err)
		}
		return orderbook.Snapshot{
			Symbol:   symbol,
			Sequence: seq,
			Bids:     spotLevels(book.Bids),
			Asks:     spotLevels(book.Asks),
		}, nil
	}
}

func spotLevels(levels []classicspotmarket.OrderBookLevel) []orderbook.Level {
	out := make([]orderbook.Level, len(levels))
	for i, l := range levels {
		out[i] = orderbook.Level{Price: types.Decimal(l[0]), Size: types.Decimal(l[1])}
	}
	return out
}

// utaBookSnapshot adapts the authenticated full-order-book call of UTA v2. It is
// only used by the deprecated REST-synchronised book (SubscribeOrderBookIncrement);
// the supported increment@10ms book pushes its own snapshot.
func utaBookSnapshot(m *utav2market.Client) utav2streaming.SnapshotFunc {
	return func(ctx context.Context, tradeType, symbol string) (orderbook.Snapshot, error) {
		// A book kept in step with the increment feed needs every level: a
		// truncated snapshot would leave the levels beyond the cut-off missing.
		book, err := m.GetOrderBook(ctx, utav2market.OrderBookOptions{TradeType: utav2market.TradeType(tradeType), Symbol: symbol, Full: true})
		if err != nil {
			return orderbook.Snapshot{}, err
		}
		bids, err := utaLevels(book.Bids)
		if err != nil {
			return orderbook.Snapshot{}, fmt.Errorf("kucoin: uta order book %s bids: %w", symbol, err)
		}
		asks, err := utaLevels(book.Asks)
		if err != nil {
			return orderbook.Snapshot{}, fmt.Errorf("kucoin: uta order book %s asks: %w", symbol, err)
		}
		return orderbook.Snapshot{Symbol: symbol, Sequence: book.Sequence, Bids: bids, Asks: asks}, nil
	}
}

// utaLevels converts [price, size] (or [price, size, rpiSize]) levels.
func utaLevels(levels [][]string) ([]orderbook.Level, error) {
	out := make([]orderbook.Level, len(levels))
	for i, l := range levels {
		if len(l) < 2 {
			return nil, fmt.Errorf("level %d has %d elements, want at least 2", i, len(l))
		}
		out[i] = orderbook.Level{Price: types.Decimal(l[0]), Size: types.Decimal(l[1])}
		if len(l) > 2 {
			out[i].RPISize = types.Decimal(l[2])
		}
	}
	return out, nil
}
