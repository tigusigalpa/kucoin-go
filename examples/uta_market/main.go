// Command uta_market demonstrates KuCoin UTA public market data. Most of
// it runs with no credentials.
//
// Run:
//
//	go run ./examples/uta_market
//
// GetOrderBook is the one method on this service that requires
// credentials (see its docblock) — set KUCOIN_API_KEY/KUCOIN_API_SECRET/
// KUCOIN_API_PASSPHRASE to also exercise it.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	kucoin "github.com/tigusigalpa/kucoin-go"
	"github.com/tigusigalpa/kucoin-go/uta/market"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := kucoin.NewClient(kucoin.WithCredentials(kucoin.Credentials{
		APIKey:        os.Getenv("KUCOIN_API_KEY"),
		APISecret:     os.Getenv("KUCOIN_API_SECRET"),
		APIPassphrase: os.Getenv("KUCOIN_API_PASSPHRASE"),
		APIKeyVersion: os.Getenv("KUCOIN_API_KEY_VERSION"),
	}))

	tickers, err := client.UTA.Market.GetTickers(ctx, market.TradeTypeSpot, "BTC-USDT")
	if err != nil {
		log.Fatalf("get tickers: %v", err)
	}
	for _, t := range tickers.List {
		fmt.Printf("%s last=%s bid=%s ask=%s\n", t.Symbol, t.LastPrice, t.BestBidPrice, t.BestAskPrice)
	}

	status, err := client.UTA.Market.GetServiceStatus(ctx, market.TradeTypeSpot)
	if err != nil {
		log.Fatalf("get service status: %v", err)
	}
	fmt.Printf("spot trading status: %s\n", status.ServerStatus)

	if os.Getenv("KUCOIN_API_KEY") == "" {
		fmt.Println("set KUCOIN_API_KEY/KUCOIN_API_SECRET/KUCOIN_API_PASSPHRASE to also fetch the order book (that one endpoint requires credentials)")
		return
	}
	book, err := client.UTA.Market.GetOrderBook(ctx, market.TradeTypeSpot, "BTC-USDT", market.GetOrderBookOptions{Limit: 5})
	if err != nil {
		log.Fatalf("get order book: %v", err)
	}
	fmt.Printf("best bid=%v best ask=%v\n", book.Bids[0], book.Asks[0])
}
