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
// "/market/ticker:BTC-USDT"; "" when the topic has none.
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
	decodeTicker = decodePlain(func(v *Ticker, m *classic.Message) { v.Symbol = symbolFromTopic(m.Topic) })
	// The all-tickers topic is the same for every symbol; the symbol is the
	// subject.
	decodeAllTicker = decodePlain(func(v *AllTickerUpdate, m *classic.Message) { v.Symbol = m.Subject })
	decodeDepth     = decodePlain(func(v *OrderBookDepth, m *classic.Message) { v.Symbol = symbolFromTopic(m.Topic) })
	decodeTrade     = decodePlain(func(v *Trade, m *classic.Message) {
		if v.Symbol == "" {
			v.Symbol = symbolFromTopic(m.Topic)
		}
	})
	decodeOrderUpdate = decodePlain(func(v *OrderUpdate, m *classic.Message) { v.PrivateEnvelope = envelope(m) })
	decodeStopOrder   = decodePlain(func(v *StopOrderUpdate, m *classic.Message) { v.PrivateEnvelope = envelope(m) })
	decodeBalance     = decodePlain(func(v *BalanceUpdate, m *classic.Message) {
		v.PrivateEnvelope = envelope(m)
		v.ID = string(m.ID)
	})
)

// snapshotWire is the nested shape of the snapshot channels.
type snapshotWire struct {
	Sequence types.Int64     `json:"sequence"`
	Data     *SymbolSnapshot `json:"data"`
}

// decodeSnapshot reads a snapshot push. The symbol of a symbol snapshot may fall
// back to the topic; the market channel's topic names a market, so its symbol
// must come from the payload.
func decodeSnapshot(m *classic.Message, topicNamesSymbol bool) (SymbolSnapshot, bool, error) {
	var w snapshotWire
	if len(m.Data) == 0 {
		return SymbolSnapshot{}, false, errors.New("push carries no data")
	}
	if err := json.Unmarshal(m.Data, &w); err != nil {
		return SymbolSnapshot{}, false, err
	}
	if w.Data == nil {
		return SymbolSnapshot{}, false, errors.New("snapshot push carries no snapshot object")
	}
	v := *w.Data
	v.Sequence = w.Sequence
	if v.Symbol == "" && topicNamesSymbol {
		v.Symbol = symbolFromTopic(m.Topic)
	}
	return v, true, nil
}

func decodeSymbolSnapshot(m *classic.Message) (SymbolSnapshot, bool, error) {
	return decodeSnapshot(m, true)
}

func decodeMarketSnapshot(m *classic.Message) (SymbolSnapshot, bool, error) {
	return decodeSnapshot(m, false)
}

// decodeBest reads one side of a level-1 push: a [price, size] pair, or an empty
// array (or nothing) when that side of the book is empty.
func decodeBest(raw json.RawMessage) (orderbook.Level, error) {
	if len(raw) == 0 {
		return orderbook.Level{}, nil
	}
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return orderbook.Level{}, err
	}
	if len(parts) == 0 { // [] and null
		return orderbook.Level{}, nil
	}
	var l orderbook.Level
	if err := json.Unmarshal(raw, &l); err != nil {
		return orderbook.Level{}, err
	}
	return l, nil
}

func decodeLevel1(m *classic.Message) (Level1, bool, error) {
	var w struct {
		Asks      json.RawMessage `json:"asks"`
		Bids      json.RawMessage `json:"bids"`
		Timestamp types.Int64     `json:"timestamp"`
	}
	if len(m.Data) == 0 {
		return Level1{}, false, errors.New("push carries no data")
	}
	if err := json.Unmarshal(m.Data, &w); err != nil {
		return Level1{}, false, err
	}
	v := Level1{Symbol: symbolFromTopic(m.Topic), Timestamp: w.Timestamp}
	var err error
	if v.Ask, err = decodeBest(w.Asks); err != nil {
		return Level1{}, false, fmt.Errorf("level-1 asks: %w", err)
	}
	if v.Bid, err = decodeBest(w.Bids); err != nil {
		return Level1{}, false, fmt.Errorf("level-1 bids: %w", err)
	}
	return v, true, nil
}

// UnmarshalJSON decodes a [price, size, sequence] row of a level-2 increment.
func (c *LevelChange) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("level-2 row: %w", err)
	}
	if len(raw) < 2 {
		return fmt.Errorf("level-2 row has %d element(s), want [price, size, sequence]", len(raw))
	}
	var out LevelChange
	if err := json.Unmarshal(raw[0], &out.Price); err != nil {
		return fmt.Errorf("level-2 row price: %w", err)
	}
	if err := json.Unmarshal(raw[1], &out.Size); err != nil {
		return fmt.Errorf("level-2 row size: %w", err)
	}
	if len(raw) > 2 {
		if err := json.Unmarshal(raw[2], &out.Sequence); err != nil {
			return fmt.Errorf("level-2 row sequence: %w", err)
		}
	}
	if !out.Price.Valid() || !out.Size.Valid() {
		return fmt.Errorf("level-2 row %s has a malformed price or size", data)
	}
	*c = out
	return nil
}

func decodeOrderBookChange(m *classic.Message) (OrderBookChange, bool, error) {
	var w struct {
		Changes struct {
			Asks []LevelChange `json:"asks"`
			Bids []LevelChange `json:"bids"`
		} `json:"changes"`
		SequenceStart types.Int64 `json:"sequenceStart"`
		SequenceEnd   types.Int64 `json:"sequenceEnd"`
		Symbol        string      `json:"symbol"`
		Time          types.Int64 `json:"time"`
	}
	if len(m.Data) == 0 {
		return OrderBookChange{}, false, errors.New("push carries no data")
	}
	if err := json.Unmarshal(m.Data, &w); err != nil {
		return OrderBookChange{}, false, err
	}
	// A book cannot follow an update without a usable sequence range: it would
	// either skip the gap check or apply nothing.
	if w.SequenceStart < 1 || w.SequenceEnd < w.SequenceStart {
		return OrderBookChange{}, false, fmt.Errorf("level-2 push has an invalid sequence range %d..%d", w.SequenceStart, w.SequenceEnd)
	}
	v := OrderBookChange{
		Symbol:        w.Symbol,
		SequenceStart: w.SequenceStart,
		SequenceEnd:   w.SequenceEnd,
		Asks:          w.Changes.Asks,
		Bids:          w.Changes.Bids,
		Timestamp:     w.Time,
	}
	if v.Symbol == "" {
		v.Symbol = symbolFromTopic(m.Topic)
	}
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
		return v, false, fmt.Errorf("candle has %d elements, want 7 [start, open, close, high, low, volume, turnover]", len(v.Candles))
	}
	start, err := v.Candles[0].Int64()
	if err != nil {
		return v, false, fmt.Errorf("candle start time: %w", err)
	}
	v.StartTime = start
	v.Open, v.Close, v.High, v.Low = v.Candles[1], v.Candles[2], v.Candles[3], v.Candles[4]
	v.Volume, v.Turnover = v.Candles[5], v.Candles[6]
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

// decodeCallAuctionData reads a call-auction push in either of the two spellings
// the documentation shows: the full names of its example and the abbreviated
// names of its schema.
func decodeCallAuctionData(m *classic.Message) (CallAuctionData, bool, error) {
	var w struct {
		Symbol                  string        `json:"symbol"`
		EstimatedPrice          types.Decimal `json:"estimatedPrice"`
		EstimatedSize           types.Decimal `json:"estimatedSize"`
		SellOrderRangeLowPrice  types.Decimal `json:"sellOrderRangeLowPrice"`
		SellOrderRangeHighPrice types.Decimal `json:"sellOrderRangeHighPrice"`
		BuyOrderRangeLowPrice   types.Decimal `json:"buyOrderRangeLowPrice"`
		BuyOrderRangeHighPrice  types.Decimal `json:"buyOrderRangeHighPrice"`
		Time                    types.Int64   `json:"time"`

		S   string        `json:"s"`
		EP  types.Decimal `json:"ep"`
		ES  types.Decimal `json:"es"`
		SLP types.Decimal `json:"slp"`
		SHP types.Decimal `json:"shp"`
		BLP types.Decimal `json:"blp"`
		BHP types.Decimal `json:"bhp"`
		TS  types.Int64   `json:"ts"`
	}
	if len(m.Data) == 0 {
		return CallAuctionData{}, false, errors.New("push carries no data")
	}
	if err := json.Unmarshal(m.Data, &w); err != nil {
		return CallAuctionData{}, false, err
	}
	pick := func(full, short types.Decimal) types.Decimal {
		if !full.IsEmpty() {
			return full
		}
		return short
	}
	v := CallAuctionData{
		Symbol:                  w.Symbol,
		EstimatedPrice:          pick(w.EstimatedPrice, w.EP),
		EstimatedSize:           pick(w.EstimatedSize, w.ES),
		SellOrderRangeLowPrice:  pick(w.SellOrderRangeLowPrice, w.SLP),
		SellOrderRangeHighPrice: pick(w.SellOrderRangeHighPrice, w.SHP),
		BuyOrderRangeLowPrice:   pick(w.BuyOrderRangeLowPrice, w.BLP),
		BuyOrderRangeHighPrice:  pick(w.BuyOrderRangeHighPrice, w.BHP),
		Timestamp:               w.Time,
	}
	if v.Symbol == "" {
		v.Symbol = w.S
	}
	if v.Symbol == "" {
		v.Symbol = symbolFromTopic(m.Topic)
	}
	if v.Timestamp == 0 {
		v.Timestamp = w.TS
	}
	return v, true, nil
}
