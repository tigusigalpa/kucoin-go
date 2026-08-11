// Command uta_trading demonstrates authenticated UTA account/order/
// position endpoints. Read-only calls (account overview, open orders,
// positions) run whenever credentials are present. Placing and
// cancelling an order additionally requires an explicit
// KUCOIN_ENABLE_TRADING_EXAMPLE=true opt-in, since it's the one call in
// this example with a side effect (even though the order is placed far
// below market price and immediately cancelled).
//
// Run:
//
//	KUCOIN_API_KEY=... KUCOIN_API_SECRET=... KUCOIN_API_PASSPHRASE=... go run ./examples/uta_trading
//
// Minimum API key permission: "General" for the read-only calls,
// "Unified" additionally for PlaceOrder/CancelOrder.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	kucoin "github.com/tigusigalpa/kucoin-go"
	"github.com/tigusigalpa/kucoin-go/uta/orders"
	"github.com/tigusigalpa/kucoin-go/uta/positions"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	apiKey := os.Getenv("KUCOIN_API_KEY")
	if apiKey == "" {
		fmt.Println("set KUCOIN_API_KEY/KUCOIN_API_SECRET/KUCOIN_API_PASSPHRASE to run this example")
		return
	}

	client := kucoin.NewClient(kucoin.WithCredentials(kucoin.Credentials{
		APIKey:        apiKey,
		APISecret:     os.Getenv("KUCOIN_API_SECRET"),
		APIPassphrase: os.Getenv("KUCOIN_API_PASSPHRASE"),
		APIKeyVersion: os.Getenv("KUCOIN_API_KEY_VERSION"),
	}))

	overview, err := client.UTA.Account.GetOverview(ctx)
	if err != nil {
		log.Fatalf("get account overview: %v", err)
	}
	fmt.Printf("equity=%s availableMargin=%s\n", overview.Equity, overview.AvailableMargin)

	openOrders, err := client.UTA.Orders.GetOpenOrderList(ctx, "SPOT", orders.GetOpenOrderListOptions{})
	if err != nil {
		log.Fatalf("get open orders: %v", err)
	}
	fmt.Printf("%d open SPOT orders\n", len(openOrders.Items))

	openPositions, err := client.UTA.Positions.GetPositions(ctx, positions.GetPositionsOptions{})
	if err != nil {
		log.Fatalf("get positions: %v", err)
	}
	fmt.Printf("%d open futures positions\n", len(openPositions))

	if os.Getenv("KUCOIN_ENABLE_TRADING_EXAMPLE") != "true" {
		fmt.Println("set KUCOIN_ENABLE_TRADING_EXAMPLE=true to also place+cancel a demo SPOT order")
		return
	}

	ref, err := client.UTA.Orders.PlaceOrder(ctx, orders.PlaceOrderRequest{
		TradeType: "SPOT",
		Symbol:    "BTC-USDT",
		Side:      "BUY",
		OrderType: "LIMIT",
		Size:      "0.0001",
		SizeUnit:  "BASECCY",
		Price:     "1000", // deliberately far below market so it won't fill
		ClientOid: "kucoin-go-example-" + time.Now().UTC().Format("20060102150405"),
	})
	if err != nil {
		log.Fatalf("place order: %v", err)
	}
	fmt.Printf("placed order %s, cancelling it now\n", ref.OrderID)

	if _, err := client.UTA.Orders.CancelOrder(ctx, orders.CancelOrderRequest{
		TradeType: "SPOT", Symbol: "BTC-USDT", OrderID: ref.OrderID,
	}); err != nil {
		log.Fatalf("cancel order: %v", err)
	}
	fmt.Println("cancelled")
}
