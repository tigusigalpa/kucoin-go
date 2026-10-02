package market

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/tigusigalpa/kucoin-go/transport"
)

func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return NewClient(transport.NewExecutor(transport.ExecutorConfig{BaseURL: server.URL}))
}

func newAuthenticatedTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return NewClient(transport.NewExecutor(transport.ExecutorConfig{
		BaseURL: server.URL,
		Credentials: transport.Credentials{
			APIKey: "key", APISecret: "secret", APIPassphrase: "pass",
		},
	}))
}

func writeOK(t *testing.T, w http.ResponseWriter, data string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write([]byte(`{"code":"200000","data":` + data + `}`)); err != nil {
		t.Errorf("write response: %v", err)
	}
}

func TestGetIndexPrices(t *testing.T) {
	var query url.Values
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/ua/v2/market/index-price" {
			t.Errorf("path = %q", r.URL.Path)
		}
		query = r.URL.Query()
		writeOK(t, w, `{"items":[{"symbol":"XBTUSDTM","indexPrice":"78975.97","decompositionList":[{"exchange":"binance","weight":"0.4115"}]}]}`)
	})

	result, err := client.GetIndexPrices(context.Background(), "XBTUSDTM")
	if err != nil {
		t.Fatalf("GetIndexPrices: %v", err)
	}
	if query.Get("symbol") != "XBTUSDTM" || result.Items[0].DecompositionList[0].Exchange != "binance" {
		t.Fatalf("unexpected result: query=%v result=%+v", query, result)
	}
}

func TestGetPositionTiersAndCollateralRatios(t *testing.T) {
	t.Run("position tiers", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/position-tiers" {
				t.Errorf("path = %q", r.URL.Path)
			}
			query := r.URL.Query()
			if query.Get("tradeType") != "FUTURES" || query.Get("data") != "RISK_LIMIT" || query.Get("symbol") != "KCSUSDTM" {
				t.Errorf("query = %v", query)
			}
			writeOK(t, w, `[{"currency":"USDT","tier":1,"maxLeverage":"5"}]`)
		})
		result, err := client.GetPositionTiers(context.Background(), PositionTiersOptions{TradeType: "FUTURES", Data: "RISK_LIMIT", Symbol: "KCSUSDTM"})
		if err != nil || len(result) != 1 || result[0].MaxLeverage != "5" {
			t.Fatalf("GetPositionTiers = %+v, %v", result, err)
		}
	})

	t.Run("collateral ratios", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/collateral-discount-ratio" {
				t.Errorf("path = %q", r.URL.Path)
			}
			writeOK(t, w, `[{"currency":"USDT","cdrConfigs":[{"tier":1,"cdr":"1"}]}]`)
		})
		result, err := client.GetCollateralRatios(context.Background())
		if err != nil || len(result) != 1 || result[0].CDRConfigs[0].CDR != "1" {
			t.Fatalf("GetCollateralRatios = %+v, %v", result, err)
		}
	})
}

func TestGetBorrowableCurrenciesAndFundingRates(t *testing.T) {
	t.Run("borrowable currencies", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/borrowable-currency" {
				t.Errorf("path = %q", r.URL.Path)
			}
			writeOK(t, w, `[{"currency":"USDT"}]`)
		})
		result, err := client.GetBorrowableCurrencies(context.Background())
		if err != nil || len(result) != 1 || result[0].Currency != "USDT" {
			t.Fatalf("GetBorrowableCurrencies = %+v, %v", result, err)
		}
	})

	t.Run("funding rates", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/funding-rate" {
				t.Errorf("path = %q", r.URL.Path)
			}
			query := r.URL.Query()
			if query.Get("symbol") != "XBTUSDTM" || query.Get("productType") != "COIN-FUTURES" {
				t.Errorf("query = %v", query)
			}
			writeOK(t, w, `[{"symbol":"XBTUSDTM","nextFundingRate":"0.000071","fundingTime":1787760000000}]`)
		})
		result, err := client.GetFundingRates(context.Background(), FundingRatesOptions{Symbol: "XBTUSDTM", ProductType: "COIN-FUTURES"})
		if err != nil || len(result) != 1 || result[0].NextFundingRate != "0.000071" {
			t.Fatalf("GetFundingRates = %+v, %v", result, err)
		}
	})
}

func TestGetFundingRateHistoryAndOpenInterest(t *testing.T) {
	t.Run("funding history", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/funding-rate-history" {
				t.Errorf("path = %q", r.URL.Path)
			}
			query := r.URL.Query()
			if query.Get("symbol") != "XBTUSDTM" || query.Get("startAt") != "1700310700000" || query.Get("endAt") != "1702310700000" {
				t.Errorf("query = %v", query)
			}
			writeOK(t, w, `{"symbol":"XBTUSDTM","list":[{"fundingRate":"0.00021","ts":1702296000000}]}`)
		})
		result, err := client.GetFundingRateHistory(context.Background(), "XBTUSDTM", 1700310700000, 1702310700000)
		if err != nil || len(result.List) != 1 || result.List[0].FundingRate != "0.00021" {
			t.Fatalf("GetFundingRateHistory = %+v, %v", result, err)
		}
	})

	t.Run("historical open interest", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/open-interest" {
				t.Errorf("path = %q", r.URL.Path)
			}
			query := r.URL.Query()
			if query.Get("symbol") != "XBTUSDTM" || query.Get("interval") != "1hour" || query.Get("pageSize") != "50" {
				t.Errorf("query = %v", query)
			}
			writeOK(t, w, `[{"openInterest":"4615535","ts":1767004200000}]`)
		})
		result, err := client.GetOpenInterest(context.Background(), OpenInterestOptions{Symbol: "XBTUSDTM", Interval: "1hour", PageSize: 50})
		if err != nil || len(result) != 1 || result[0].OpenInterest != "4615535" {
			t.Fatalf("GetOpenInterest = %+v, %v", result, err)
		}
	})
}

func TestGetInterestRateIndexAndTradeStatistics(t *testing.T) {
	t.Run("interest rate index", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/interest-rate-index" {
				t.Errorf("path = %q", r.URL.Path)
			}
			query := r.URL.Query()
			if query.Get("symbol") != "XBTUSDTM" || query.Get("lastId") != "17" || query.Get("pageSize") != "100" {
				t.Errorf("query = %v", query)
			}
			writeOK(t, w, `{"lastId":18,"items":[{"symbol":"XBTUSDTM","interestRate":"0.0003","ts":1787734560000}]}`)
		})
		result, err := client.GetInterestRateIndex(context.Background(), InterestRateIndexOptions{Symbol: "XBTUSDTM", LastID: 17, PageSize: 100})
		if err != nil || result.LastID != 18 || result.Items[0].InterestRate != "0.0003" {
			t.Fatalf("GetInterestRateIndex = %+v, %v", result, err)
		}
	})

	t.Run("trade statistics", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/trade-statistics" {
				t.Errorf("path = %q", r.URL.Path)
			}
			writeOK(t, w, `{"spot":{"turnoverOf24h":"1"},"futures":{"turnoverOf24h":"2"}}`)
		})
		result, err := client.GetTradeStatistics(context.Background())
		if err != nil || result.Spot.TurnoverOf24h != "1" || result.Futures.TurnoverOf24h != "2" {
			t.Fatalf("GetTradeStatistics = %+v, %v", result, err)
		}
	})
}

func TestGetCallAuctionInfo(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/ua/v2/market/call-auction-info" || r.URL.Query().Get("symbol") != "GROVE-USDT" {
			t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
		}
		writeOK(t, w, `{"symbol":"GROVE-USDT","estimatedPrice":"0.08388","estimatedSize":"2406.3","time":1783434135273}`)
	})

	result, err := client.GetCallAuctionInfo(context.Background(), "GROVE-USDT")
	if err != nil || result.EstimatedPrice != "0.08388" || result.Time != 1783434135273 {
		t.Fatalf("GetCallAuctionInfo = %+v, %v", result, err)
	}
}

func TestGetAnnouncementsAndCurrencies(t *testing.T) {
	t.Run("announcements", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/announcement" || r.URL.Query().Get("pageSize") != "20" {
				t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
			}
			writeOK(t, w, `{"totalNumber":1,"pageSize":20,"list":[{"id":129275,"title":"Update"}]}`)
		})
		result, err := client.GetAnnouncements(context.Background(), AnnouncementOptions{PageSize: 20})
		if err != nil || result.List[0].ID != 129275 {
			t.Fatalf("GetAnnouncements = %+v, %v", result, err)
		}
	})

	t.Run("currency", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/currency" || r.URL.Query().Get("currency") != "USDT" || r.URL.Query().Get("chain") != "trx" {
				t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
			}
			writeOK(t, w, `{"currency":"USDT","list":[{"chain":"trx","chainName":"TRC20","isMemoRequired":false}]}`)
		})
		result, err := client.GetCurrency(context.Background(), "USDT", "trx")
		if err != nil || result.Chains[0].Chain != "trx" {
			t.Fatalf("GetCurrency = %+v, %v", result, err)
		}
	})

	t.Run("currencies keeps repeated query keys", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/asset/currencies" || r.URL.Query().Get("chain") != "trx" {
				t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
			}
			currencies := r.URL.Query()["currencyList"]
			if len(currencies) != 2 || currencies[0] != "USDT" || currencies[1] != "BTC" {
				t.Errorf("currencyList = %v", currencies)
			}
			writeOK(t, w, `[{"currency":"USDT","list":[{"chain":"trx"}]}]`)
		})
		result, err := client.GetCurrencies(context.Background(), []string{"USDT", "BTC"}, "trx")
		if err != nil || len(result) != 1 || result[0].Currency != "USDT" {
			t.Fatalf("GetCurrencies = %+v, %v", result, err)
		}
	})
}

func TestGetV2MarketCore(t *testing.T) {
	t.Run("tickers", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/ticker" || r.URL.Query().Get("tradeType") != "SPOT" || r.URL.Query().Get("symbol") != "BTC-USDT" {
				t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
			}
			writeOK(t, w, `{"tradeType":"SPOT","ts":1,"list":[{"symbol":"BTC-USDT","lastPrice":"100"}]}`)
		})
		result, err := client.GetTickers(context.Background(), TradeTypeSpot, "BTC-USDT")
		if err != nil || result.List[0].LastPrice != "100" {
			t.Fatalf("GetTickers = %+v, %v", result, err)
		}
	})

	t.Run("instruments", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/instrument" || r.URL.Query().Get("tradeType") != "FUTURES" {
				t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
			}
			writeOK(t, w, `{"tradeType":"FUTURES","list":[{"symbol":"XBTUSDTM","tickSize":"0.1"}]}`)
		})
		result, err := client.GetInstruments(context.Background(), TradeTypeFutures, "XBTUSDTM")
		if err != nil || result.List[0].TickSize != "0.1" {
			t.Fatalf("GetInstruments = %+v, %v", result, err)
		}
	})

	t.Run("klines", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/kline" || r.URL.Query().Get("klineType") != "TRADE" || r.URL.Query().Get("startAt") != "1700000000" {
				t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
			}
			writeOK(t, w, `{"tradeType":"SPOT","symbol":"BTC-USDT","list":[[1700000000,"1","3","0","2","4","5"]]}`)
		})
		result, err := client.GetKlines(context.Background(), KlineOptions{TradeType: TradeTypeSpot, Symbol: "BTC-USDT", KlineType: "TRADE", Interval: "1hour", StartAt: 1700000000})
		if err != nil || result.List[0].High != "3" || result.List[0].Close != "2" {
			t.Fatalf("GetKlines = %+v, %v", result, err)
		}
	})

	t.Run("order book signs the documented private endpoint", func(t *testing.T) {
		client := newAuthenticatedTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/orderbook" || r.URL.Query().Get("limit") != "20" || r.Header.Get("KC-API-KEY") != "key" {
				t.Errorf("request = %s?%s key=%q", r.URL.Path, r.URL.RawQuery, r.Header.Get("KC-API-KEY"))
			}
			writeOK(t, w, `{"tradeType":"FUTURES","symbol":"XBTUSDTM","sequence":1,"bids":[["1","2"]],"asks":[["3","4"]]}`)
		})
		result, err := client.GetOrderBook(context.Background(), OrderBookOptions{TradeType: TradeTypeFutures, Symbol: "XBTUSDTM", Limit: 20})
		if err != nil || result.Bids[0][0] != "1" {
			t.Fatalf("GetOrderBook = %+v, %v", result, err)
		}
	})

	t.Run("trades", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/trade" || r.URL.Query().Get("symbol") != "BTC-USDT" {
				t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
			}
			writeOK(t, w, `{"tradeType":"SPOT","list":[{"sequence":1,"tradeId":"1","price":"2","size":"3","side":"BUY","ts":4}]}`)
		})
		result, err := client.GetTrades(context.Background(), TradeTypeSpot, "BTC-USDT")
		if err != nil || result.List[0].Price != "2" {
			t.Fatalf("GetTrades = %+v, %v", result, err)
		}
	})
}

func TestGetV2MarketSupplementaryEndpoints(t *testing.T) {
	t.Run("fiat prices", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/market/fiat-price" || r.URL.Query().Get("base") != "USD" {
				t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
			}
			if values := r.URL.Query()["currencies"]; len(values) != 2 || values[0] != "BTC" || values[1] != "ETH" {
				t.Errorf("currencies = %v", values)
			}
			writeOK(t, w, `{"BTC":"1","ETH":"2"}`)
		})
		result, err := client.GetFiatPrices(context.Background(), "USD", []string{"BTC", "ETH"})
		if err != nil || result["ETH"] != "2" {
			t.Fatalf("GetFiatPrices = %+v, %v", result, err)
		}
	})

	t.Run("custody currencies", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/oes/currency" || r.URL.Query().Get("custodian") != "BITGO_SG" {
				t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
			}
			writeOK(t, w, `[{"custodian":"BITGO_SG","currency":"BTC","precision":8}]`)
		})
		result, err := client.GetCustodyCurrencies(context.Background(), "BITGO_SG", "BTC")
		if err != nil || result[0].Precision != 8 {
			t.Fatalf("GetCustodyCurrencies = %+v, %v", result, err)
		}
	})

	t.Run("service status", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/server/status" || r.URL.Query().Get("tradeType") != "SPOT" {
				t.Errorf("request = %s?%s", r.URL.Path, r.URL.RawQuery)
			}
			writeOK(t, w, `{"tradeType":"SPOT","serverStatus":"open"}`)
		})
		result, err := client.GetServiceStatus(context.Background(), TradeTypeSpot)
		if err != nil || result.ServerStatus != "open" {
			t.Fatalf("GetServiceStatus = %+v, %v", result, err)
		}
	})

	t.Run("KYC regions", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/user/kyc-region" {
				t.Errorf("path = %s", r.URL.Path)
			}
			writeOK(t, w, `[{"code":"AD","enName":"Andorra"}]`)
		})
		result, err := client.GetKYCRegions(context.Background())
		if err != nil || result[0].ENName != "Andorra" {
			t.Fatalf("GetKYCRegions = %+v, %v", result, err)
		}
	})

	t.Run("client IP", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/ua/v2/user/my-ip" {
				t.Errorf("path = %s", r.URL.Path)
			}
			writeOK(t, w, `"160.30.121.224"`)
		})
		result, err := client.GetClientIPAddress(context.Background())
		if err != nil || result != "160.30.121.224" {
			t.Fatalf("GetClientIPAddress = %q, %v", result, err)
		}
	})
}
