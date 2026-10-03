package streaming

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/types"
	"github.com/tigusigalpa/kucoin-go/websocket/classic"
)

// symbolFromTopic returns the symbol part of a topic such as
// "/contractMarket/tickerV2:XBTUSDTM"; "" when the topic has none.
func symbolFromTopic(topic string) string {
	if i := strings.LastIndexByte(topic, ':'); i >= 0 {
		return topic[i+1:]
	}
	return ""
}

// decodePlain builds a decoder that unmarshals the push payload into T and then
// lets fill add the fields that live outside the payload (topic, subject, ...).
func decodePlain[T any](fill func(v *T, m *classic.Message)) classic.DecodeFunc[T] {
	return func(m *classic.Message) (T, bool, error) {
		var v T
		if len(m.Data) == 0 {
			return v, false, errors.New("push carries no data")
		}
		if err := json.Unmarshal(m.Data, &v); err != nil {
			return v, false, err
		}
		if fill != nil {
			fill(&v, m)
		}
		return v, true, nil
	}
}

func envelope(m *classic.Message) PrivateEnvelope {
	return PrivateEnvelope{Subject: m.Subject, UserID: m.UserID, ChannelType: m.ChannelType}
}

var (
	decodeTickerV2 = decodePlain(func(v *TickerV2, m *classic.Message) {
		if v.Symbol == "" {
			v.Symbol = symbolFromTopic(m.Topic)
		}
	})
	decodeTickerV1 = decodePlain(func(v *TickerV1, m *classic.Message) {
		if v.Symbol == "" {
			v.Symbol = symbolFromTopic(m.Topic)
		}
	})
	decodeDepth = decodePlain(func(v *OrderBookDepth, m *classic.Message) { v.Symbol = symbolFromTopic(m.Topic) })
	decodeTrade = decodePlain(func(v *Trade, m *classic.Message) {
		if v.Symbol == "" {
			v.Symbol = symbolFromTopic(m.Topic)
		}
	})
	decodeInstrument = decodePlain(func(v *InstrumentEvent, m *classic.Message) {
		v.Symbol = symbolFromTopic(m.Topic)
		v.Subject = m.Subject
	})
	decodeFundingSettlement = decodePlain(func(v *FundingSettlement, m *classic.Message) { v.Subject = m.Subject })
	decodeSnapshot          = decodePlain(func(v *SymbolSnapshot, m *classic.Message) {
		if v.Symbol == "" {
			v.Symbol = symbolFromTopic(m.Topic)
		}
	})
	decodeOrderChange = decodePlain(func(v *OrderChange, m *classic.Message) { v.PrivateEnvelope = envelope(m) })
	decodeStopOrder   = decodePlain(func(v *StopOrderEvent, m *classic.Message) {
		v.PrivateEnvelope = envelope(m)
		v.ID = string(m.ID)
	})
	decodeBalance = decodePlain(func(v *BalanceEvent, m *classic.Message) {
		v.PrivateEnvelope = envelope(m)
		v.ID = string(m.ID)
	})
	decodePosition = decodePlain(func(v *PositionEvent, m *classic.Message) {
		v.PrivateEnvelope = envelope(m)
		// A funding settlement carries no symbol of its own; the per-symbol topic does.
		if v.Symbol == "" {
			v.Symbol = symbolFromTopic(m.Topic)
		}
	})
)

// parseChange splits the level-2 wire string "price,side,size".
func parseChange(change string) (orderbook.Change, error) {
	parts := strings.Split(change, ",")
	if len(parts) != 3 {
		return orderbook.Change{}, fmt.Errorf("level-2 change %q is not \"price,side,size\"", change)
	}
	side, err := orderbook.ParseSide(parts[1])
	if err != nil {
		return orderbook.Change{}, err
	}
	c := orderbook.Change{Side: side, Price: types.Decimal(parts[0]), Size: types.Decimal(parts[2])}
	if !c.Price.Valid() || !c.Size.Valid() {
		return orderbook.Change{}, fmt.Errorf("level-2 change %q has a malformed price or size", change)
	}
	return c, nil
}

func decodeOrderBookChange(m *classic.Message) (OrderBookChange, bool, error) {
	var v OrderBookChange
	if len(m.Data) == 0 {
		return v, false, errors.New("push carries no data")
	}
	if err := json.Unmarshal(m.Data, &v); err != nil {
		return v, false, err
	}
	c, err := parseChange(v.Change)
	if err != nil {
		return v, false, err
	}
	v.Symbol = symbolFromTopic(m.Topic)
	v.Side, v.Price, v.Size = c.Side, c.Price, c.Size
	return v, true, nil
}

func decodeKline(m *classic.Message) (Kline, bool, error) {
	var v Kline
	if len(m.Data) == 0 {
		return v, false, errors.New("push carries no data")
	}
	if err := json.Unmarshal(m.Data, &v); err != nil {
		return v, false, err
	}
	if len(v.Candles) < 7 {
		return v, false, fmt.Errorf("candle has %d elements, want 7 [start, open, close, high, low, turnover, volume]", len(v.Candles))
	}
	start, err := v.Candles[0].Int64()
	if err != nil {
		return v, false, fmt.Errorf("candle start time: %w", err)
	}
	v.StartTime = start
	v.Open, v.Close, v.High, v.Low = v.Candles[1], v.Candles[2], v.Candles[3], v.Candles[4]
	// KuCoin's documentation lists the last two elements as volume, turnover. The
	// live feed sends them the other way round, and both match the REST kline of
	// the same minute: element 5 is the turnover in the quote currency, element 6
	// the traded size in contracts.
	v.Turnover, v.Volume = v.Candles[5], v.Candles[6]
	// The topic suffix is "{symbol}_{interval}".
	suffix := symbolFromTopic(m.Topic)
	if i := strings.LastIndexByte(suffix, '_'); i >= 0 {
		v.Interval = Interval(suffix[i+1:])
		if v.Symbol == "" {
			v.Symbol = suffix[:i]
		}
	}
	return v, true, nil
}

func decodeMarginMode(m *classic.Message) (MarginModeEvent, bool, error) {
	ev := MarginModeEvent{PrivateEnvelope: envelope(m)}
	if err := json.Unmarshal(m.Data, &ev.Modes); err != nil {
		return ev, false, fmt.Errorf("margin-mode payload: %w", err)
	}
	return ev, true, nil
}

func decodeCrossLeverage(m *classic.Message) (CrossLeverageEvent, bool, error) {
	ev := CrossLeverageEvent{PrivateEnvelope: envelope(m)}
	var raw map[string]struct {
		Leverage types.Decimal `json:"leverage"`
	}
	if err := json.Unmarshal(m.Data, &raw); err != nil {
		return ev, false, fmt.Errorf("cross-leverage payload: %w", err)
	}
	ev.Leverages = make(map[string]types.Decimal, len(raw))
	for symbol, entry := range raw {
		ev.Leverages[symbol] = entry.Leverage
	}
	return ev, true, nil
}
