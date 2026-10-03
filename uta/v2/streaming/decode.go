package streaming

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/tigusigalpa/kucoin-go/types"
	"github.com/tigusigalpa/kucoin-go/websocket/uta"
)

// errNoData is the decoding error of a push without a payload.
var errNoData = errors.New("push carries no data")

// tradeTypeOf returns the trade type of a push type of the form
// "channel.TRADETYPE" ("ticker.FUTURES", "obu.spot"); it is "" for pushes whose
// type has no such suffix.
func tradeTypeOf(pushType string) TradeType {
	if i := strings.LastIndexByte(pushType, '.'); i >= 0 {
		return TradeType(strings.ToUpper(pushType[i+1:]))
	}
	return ""
}

// accountTypeOf returns the account type of a balance push type
// ("balance.FUNDING").
func accountTypeOf(pushType string) AccountType {
	return AccountType(tradeTypeOf(pushType))
}

// decodeData unmarshals the payload of a push into v.
func decodeData(p *uta.Push, v any) error {
	if len(p.Data) == 0 || string(p.Data) == "null" {
		return errNoData
	}
	return json.Unmarshal(p.Data, v)
}

// decodeObject builds a decoder that unmarshals the push payload into T and then
// lets fill add the fields that live outside the payload (push type, push time).
func decodeObject[T any](fill func(v *T, p *uta.Push)) uta.DecodeFunc[T] {
	return func(p *uta.Push) (T, bool, error) {
		var v T
		if err := decodeData(p, &v); err != nil {
			return v, false, err
		}
		if fill != nil {
			fill(&v, p)
		}
		return v, true, nil
	}
}

var (
	decodeTicker = decodeObject(func(v *Ticker, p *uta.Push) {
		v.TradeType = tradeTypeOf(p.T)
		v.GatewayTimestamp = types.Int64(p.P)
	})
	decodeTrade = decodeObject(func(v *Trade, p *uta.Push) {
		v.TradeType = tradeTypeOf(p.T)
		v.GatewayTimestamp = types.Int64(p.P)
	})
	decodeKline = decodeObject(func(v *Kline, p *uta.Push) {
		v.TradeType = tradeTypeOf(p.T)
		v.GatewayTimestamp = types.Int64(p.P)
	})
	decodeOrderBookUpdate = decodeObject(func(v *OrderBookUpdate, p *uta.Push) {
		v.TradeType = tradeTypeOf(p.T)
		v.Depth = Depth(p.Depth)
		v.Kind = UpdateKind(strings.ToLower(p.Kind))
		v.GatewayTimestamp = types.Int64(p.P)
	})
	decodeMarkPrice = decodeObject(func(v *MarkPrice, p *uta.Push) {
		v.GatewayTimestamp = types.Int64(p.P)
	})
	decodeFundingRate = decodeObject(func(v *FundingRate, p *uta.Push) {
		v.GatewayTimestamp = types.Int64(p.P)
	})
	decodeOrder = decodeObject(func(v *OrderUpdate, p *uta.Push) {
		v.PushType = p.T
		v.GatewayTimestamp = types.Int64(p.P)
	})
	decodeExecution = decodeObject(func(v *Execution, p *uta.Push) {
		v.GatewayTimestamp = types.Int64(p.P)
	})
	decodeExecutionLite = decodeObject(func(v *ExecutionLite, p *uta.Push) {
		v.GatewayTimestamp = types.Int64(p.P)
	})
	decodeBalance = decodeObject(func(v *BalanceUpdate, p *uta.Push) {
		v.AccountType = accountTypeOf(p.T)
		v.GatewayTimestamp = types.Int64(p.P)
	})
	decodePosition = decodeObject(func(v *PositionUpdate, p *uta.Push) {
		v.PushType = p.T
		v.GatewayTimestamp = types.Int64(p.P)
	})
	decodeLiquidationWarning = decodeObject(func(v *LiquidationWarning, p *uta.Push) {
		v.PushType = p.T
		v.GatewayTimestamp = types.Int64(p.P)
	})
	decodeLeverage = decodeObject(func(v *LeverageUpdate, p *uta.Push) {
		v.GatewayTimestamp = types.Int64(p.P)
	})
)

// decodeAllFundingRates decodes the array payload of the funding-fee-all-symbols
// channel.
func decodeAllFundingRates(p *uta.Push) (AllFundingRates, bool, error) {
	var out AllFundingRates
	if err := decodeData(p, &out.Rates); err != nil {
		return out, false, err
	}
	out.GatewayTimestamp = types.Int64(p.P)
	for i := range out.Rates {
		out.Rates[i].GatewayTimestamp = out.GatewayTimestamp
	}
	return out, true, nil
}

// decodeCallAuction decodes the call-auction payload, taking the estimated price
// from "ep" (schema) or "eq" (example), whichever is present.
func decodeCallAuction(p *uta.Push) (CallAuctionInfo, bool, error) {
	var w callAuctionWire
	if err := decodeData(p, &w); err != nil {
		return CallAuctionInfo{}, false, err
	}
	price := w.EstimatedPx
	if price.IsEmpty() {
		price = w.EstimatedEq
	}
	return CallAuctionInfo{
		Symbol:           w.Symbol,
		Kind:             UpdateKind(strings.ToLower(p.Kind)),
		EstimatedPrice:   price,
		EstimatedSize:    w.EstimatedSize,
		SellLowPrice:     w.SellLow,
		SellHighPrice:    w.SellHigh,
		BuyLowPrice:      w.BuyLow,
		BuyHighPrice:     w.BuyHigh,
		Timestamp:        w.Timestamp,
		GatewayTimestamp: types.Int64(p.P),
	}, true, nil
}

// decodeBookUpdate decodes an obu push for a managed order book and rejects what
// a book cannot use: an unknown kind or a sequence range that runs backwards.
func decodeBookUpdate(p *uta.Push) (OrderBookUpdate, error) {
	u, _, err := decodeOrderBookUpdate(p)
	if err != nil {
		return u, err
	}
	switch u.Kind {
	case UpdateSnapshot, UpdateDelta:
	default:
		return u, fmt.Errorf("order-book push has kind %q, want %q or %q", p.Kind, UpdateSnapshot, UpdateDelta)
	}
	if u.EndSequence < u.StartSequence {
		return u, fmt.Errorf("order-book push covers sequences %d..%d, which runs backwards", u.StartSequence, u.EndSequence)
	}
	return u, nil
}
