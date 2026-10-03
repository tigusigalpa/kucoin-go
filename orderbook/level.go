// Package orderbook maintains exact-decimal local order books and the typed
// price levels, snapshots and deltas shared by every order-book source of this
// module (Classic Futures, Classic Spot and UTA, over REST and WebSocket).
package orderbook

import (
	"encoding/json"
	"fmt"

	"github.com/tigusigalpa/kucoin-go/types"
)

// Level is one price level of an order book: the price and the aggregated size
// resting at it. KuCoin sends levels as two-element arrays whose elements are
// strings in some feeds and bare JSON numbers in others (the Futures REST book
// uses numbers such as 84486.0); Level accepts both and keeps the exact text.
type Level struct {
	Price types.Decimal
	Size  types.Decimal
	// RPISize is the size of Retail Price Improvement orders. It is only set on
	// UTA books requested with rpiFilter=1, whose levels have a third element
	// (price, non-RPI size, RPI size); Size then holds the non-RPI part.
	RPISize types.Decimal
}

// UnmarshalJSON decodes a [price, size] or [price, size, rpiSize] array.
func (l *Level) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("kucoin: decode order-book level: %w", err)
	}
	if len(raw) < 2 {
		return fmt.Errorf("kucoin: decode order-book level: want at least [price, size], got %d element(s)", len(raw))
	}
	var out Level
	if err := json.Unmarshal(raw[0], &out.Price); err != nil {
		return fmt.Errorf("kucoin: decode order-book level price: %w", err)
	}
	if err := json.Unmarshal(raw[1], &out.Size); err != nil {
		return fmt.Errorf("kucoin: decode order-book level size: %w", err)
	}
	if len(raw) > 2 {
		if err := json.Unmarshal(raw[2], &out.RPISize); err != nil {
			return fmt.Errorf("kucoin: decode order-book level RPI size: %w", err)
		}
	}
	*l = out
	return nil
}

// MarshalJSON encodes the level as a [price, size] string array (with the RPI
// size appended when present).
func (l Level) MarshalJSON() ([]byte, error) {
	if l.RPISize != "" {
		return json.Marshal([]string{string(l.Price), string(l.Size), string(l.RPISize)})
	}
	return json.Marshal([]string{string(l.Price), string(l.Size)})
}
