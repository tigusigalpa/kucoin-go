package types

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// ID is an identifier (order ID, trade ID, sequence) that KuCoin sends as a
// JSON string in some messages and as a bare JSON number in others. It keeps
// the exact text, so a 64-bit trade ID such as 22075878662488064 is never
// rounded through float64.
type ID string

// String returns the identifier text.
func (i ID) String() string { return string(i) }

// UnmarshalJSON accepts a JSON string, a JSON number or null.
func (i *ID) UnmarshalJSON(data []byte) error {
	text, isNull, err := flexText(data)
	if err != nil {
		return fmt.Errorf("kucoin: decode id: %w", err)
	}
	if !isNull {
		*i = ID(text)
	}
	return nil
}

// MarshalJSON encodes the identifier as a JSON string.
func (i ID) MarshalJSON() ([]byte, error) { return json.Marshal(string(i)) }

// Int64 is an integer (timestamp, sequence, count) that KuCoin sends as a JSON
// number in the documentation but occasionally as a numeric string on the
// wire. An absent, null or empty value decodes to 0.
type Int64 int64

// Value returns the plain int64.
func (n Int64) Value() int64 { return int64(n) }

// UnmarshalJSON accepts a JSON number, a numeric JSON string, an empty string
// or null.
func (n *Int64) UnmarshalJSON(data []byte) error {
	text, isNull, err := flexText(data)
	if err != nil {
		return fmt.Errorf("kucoin: decode int64: %w", err)
	}
	if isNull {
		return nil
	}
	if text == "" {
		*n = 0
		return nil
	}
	// Fast path for the overwhelmingly common plain integer.
	if v, err := strconv.ParseInt(text, 10, 64); err == nil {
		*n = Int64(v)
		return nil
	}
	// Integral values written with a fraction or exponent ("1.7E18", "12.0").
	v, err := Decimal(text).Int64()
	if err != nil {
		return fmt.Errorf("kucoin: decode int64 %q: %w", text, err)
	}
	*n = Int64(v)
	return nil
}

// MarshalJSON encodes the integer as a JSON number.
func (n Int64) MarshalJSON() ([]byte, error) { return strconv.AppendInt(nil, int64(n), 10), nil }
