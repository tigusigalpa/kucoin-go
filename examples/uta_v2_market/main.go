// uta_v2_market demonstrates current UTA REST v2 public market data.
package main

import (
	"context"
	"fmt"
	"log"

	kucoin "github.com/tigusigalpa/kucoin-go"
	utav2market "github.com/tigusigalpa/kucoin-go/uta/v2/market"
)

func main() {
	client := kucoin.NewClient()
	ctx := context.Background()

	tickers, err := client.UTA.V2.Market.GetTickers(ctx, utav2market.TradeTypeSpot, "BTC-USDT")
	if err != nil {
		log.Fatal(err)
	}
	if len(tickers.List) == 0 {
		log.Fatal("KuCoin returned no BTC-USDT ticker")
	}
	fmt.Println("last price:", tickers.List[0].LastPrice)

	funding, err := client.UTA.V2.Market.GetFundingRates(ctx, utav2market.FundingRatesOptions{Symbol: "XBTUSDTM"})
	if err != nil {
		log.Fatal(err)
	}
	if len(funding) > 0 {
		fmt.Println("next funding rate:", funding[0].NextFundingRate)
	}
}
