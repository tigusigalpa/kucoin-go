// futures_market is a tour of KuCoin Classic Futures' public market-data REST
// endpoints: contract specification, ticker, order books, trades, candles, mark
// price and funding rates. It needs no credentials. The 24-hour platform
// statistics call is the one endpoint KuCoin documents as public but serves only
// to signed callers, so it runs only when KUCOIN_API_KEY, KUCOIN_API_SECRET and
// KUCOIN_API_PASSPHRASE are set.
//
// Run: go run ./examples/futures_market -symbol XBTUSDTM
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	kucoin "github.com/tigusigalpa/kucoin-go"
	"github.com/tigusigalpa/kucoin-go/classic/futures/market"
	"github.com/tigusigalpa/kucoin-go/transport"
)

func main() {
	symbol := flag.String("symbol", "XBTUSDTM", "contract symbol")
	flag.Parse()
	if err := run(context.Background(), *symbol); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, symbol string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	opts := []kucoin.Option{}
	if key := os.Getenv("KUCOIN_API_KEY"); key != "" {
		opts = append(opts, kucoin.WithCredentials(kucoin.Credentials{
			APIKey:        key,
			APISecret:     os.Getenv("KUCOIN_API_SECRET"),
			APIPassphrase: os.Getenv("KUCOIN_API_PASSPHRASE"),
			APIKeyVersion: os.Getenv("KUCOIN_API_KEY_VERSION"),
		}))
	}
	m := kucoin.NewClient(opts...).Classic.Futures.Market

	serverTime, err := m.GetServerTime(ctx)
	if err != nil {
		return fmt.Errorf("server time: %w", err)
	}
	status, err := m.GetServiceStatus(ctx)
	if err != nil {
		return fmt.Errorf("service status: %w", err)
	}
	fmt.Printf("server time %s, futures service %s\n", time.UnixMilli(serverTime).UTC().Format(time.RFC3339), status.Status)

	// The contract specification and a live statistics snapshot. Every price and
	// rate is a types.Decimal: the exact text KuCoin sent, never a float.
	spec, err := m.GetSymbol(ctx, symbol)
	if err != nil {
		return fmt.Errorf("symbol: %w", err)
	}
	fmt.Printf("\n%s (%s/%s, settles in %s)\n", spec.Symbol, spec.BaseCurrency, spec.QuoteCurrency, spec.SettleCurrency)
	fmt.Printf("  tick %s, lot %d, multiplier %s, max leverage %d, maker/taker fee %s/%s\n",
		spec.TickSize, spec.LotSize, spec.Multiplier, spec.MaxLeverage, spec.MakerFeeRate, spec.TakerFeeRate)
	fmt.Printf("  mark %s, index %s, last %s, open interest %s contracts, funding rate %s\n",
		spec.MarkPrice, spec.IndexPrice, spec.LastTradePrice, spec.OpenInterest, spec.FundingFeeRate)

	all, err := m.GetAllSymbols(ctx)
	if err != nil {
		return fmt.Errorf("all symbols: %w", err)
	}
	fmt.Printf("\n%d contracts are tradable\n", len(all))

	ticker, err := m.GetTicker(ctx, symbol)
	if err != nil {
		return fmt.Errorf("ticker: %w", err)
	}
	fmt.Printf("\nticker: last %s (%s %d), bid %s x %d, ask %s x %d at %s\n", ticker.Price, ticker.Side, ticker.Size,
		ticker.BestBidPrice, ticker.BestBidSize, ticker.BestAskPrice, ticker.BestAskSize, ticker.Time().Format("15:04:05.000"))

	// Order books: the part book is the light call; the full book is what a
	// local order book is seeded with (see examples/futures_orderbook).
	part, err := m.GetPartOrderBook(ctx, symbol, 20)
	if err != nil {
		return fmt.Errorf("part order book: %w", err)
	}
	fmt.Printf("\nbest 3 of %d bid and %d ask levels (sequence %d):\n", len(part.Bids), len(part.Asks), part.Sequence)
	for i := 0; i < 3 && i < len(part.Bids) && i < len(part.Asks); i++ {
		fmt.Printf("  bid %12s x %-8s ask %12s x %s\n", part.Bids[i].Price, part.Bids[i].Size, part.Asks[i].Price, part.Asks[i].Size)
	}
	full, err := m.GetFullOrderBook(ctx, symbol)
	if err != nil {
		return fmt.Errorf("full order book: %w", err)
	}
	fmt.Printf("full book: %d bids, %d asks, sequence %d\n", len(full.Bids), len(full.Asks), full.Sequence)

	trades, err := m.GetTradeHistory(ctx, symbol)
	if err != nil {
		return fmt.Errorf("trade history: %w", err)
	}
	fmt.Printf("\nlast %d trades, newest:\n", len(trades))
	for i := 0; i < 3 && i < len(trades); i++ {
		fmt.Printf("  %s %-4s %d @ %s\n", trades[i].Time().Format("15:04:05.000"), trades[i].Side, trades[i].Size, trades[i].Price)
	}

	candles, err := m.GetKlines(ctx, market.KlineOptions{Symbol: symbol, Granularity: market.Granularity1Hour})
	if err != nil {
		return fmt.Errorf("klines: %w", err)
	}
	fmt.Printf("\n%d one-hour candles, latest:\n", len(candles))
	for i := len(candles) - 1; i >= 0 && i >= len(candles)-3; i-- {
		c := candles[i]
		fmt.Printf("  %s open %s high %s low %s close %s turnover %s\n", c.Timestamp().Format("01-02 15:04"), c.Open, c.High, c.Low, c.Close, c.Turnover)
	}

	mark, err := m.GetMarkPrice(ctx, symbol)
	if err != nil {
		return fmt.Errorf("mark price: %w", err)
	}
	fmt.Printf("\nmark price %s, index price %s\n", mark.Value, mark.IndexPrice)

	funding, err := m.GetCurrentFundingRate(ctx, symbol)
	if err != nil {
		return fmt.Errorf("funding rate: %w", err)
	}
	fmt.Printf("funding rate %s, next settlement %s\n", funding.Value, funding.NextFundingTime().UTC().Format(time.RFC3339))
	now := time.Now()
	history, err := m.GetPublicFundingHistory(ctx, market.FundingHistoryOptions{
		Symbol: symbol, From: now.Add(-72 * time.Hour).UnixMilli(), To: now.UnixMilli(),
	})
	if err != nil {
		return fmt.Errorf("funding history: %w", err)
	}
	for _, p := range history {
		fmt.Printf("  settled %s at %s\n", p.Time().UTC().Format("01-02 15:04"), p.FundingRate)
	}

	stats, err := m.Get24hStats(ctx)
	switch {
	case err == nil:
		fmt.Printf("\nplatform 24h futures turnover: %s USD\n", stats.TurnoverOf24h)
	case errors.Is(err, transport.ErrBadRequest) && os.Getenv("KUCOIN_API_KEY") == "":
		fmt.Println("\nplatform 24h statistics need API credentials (KuCoin documents the endpoint as public, but serves it signed only)")
	default:
		return fmt.Errorf("24h stats: %w", err)
	}
	return nil
}
