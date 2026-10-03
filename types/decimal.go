// Package types holds the wire-level value types shared by every typed REST
// model and WebSocket payload in this module.
//
// KuCoin documents most prices, sizes and rates as JSON strings, but the live
// API is not consistent: the same logical field is a quoted string in one
// endpoint, a bare JSON number in another (including exponent notation such as
// 1.0E-4), and sometimes an empty string. Decoding such values into float64
// loses precision, and decoding them into string fails on a bare number.
// Decimal, ID and Int64 accept both forms and keep the exact text KuCoin sent,
// so a consumer never has to parse raw JSON and never loses a digit.
package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// maxExponent bounds the decimal exponent accepted by Canonical so a hostile
// value such as "1e999999999" cannot make normalisation allocate gigabytes.
const maxExponent = 400

// ErrInvalidDecimal is returned by the numeric helpers when a Decimal does not
// hold a well-formed decimal number (for example an empty string).
var ErrInvalidDecimal = errors.New("kucoin: invalid decimal")

// Decimal is an exact decimal number kept as the text KuCoin transmitted.
//
// It unmarshals from a JSON string ("84486.5"), from a bare JSON number
// (84486.5, 1.0E-4) or from null, and marshals back as a JSON string. The
// zero value is the empty Decimal, which is what an absent or empty field
// decodes to; use IsEmpty to tell it apart from the number zero.
//
// Do arithmetic with Rat (exact) or Float64 (approximate), and compare with
// Cmp, never by comparing the strings: "84486.0" and "84486" are the same
// price.
type Decimal string

// String returns the exact text KuCoin sent.
func (d Decimal) String() string { return string(d) }

// IsEmpty reports whether the field was absent, null or an empty string.
func (d Decimal) IsEmpty() bool { return d == "" }

// UnmarshalJSON accepts a JSON string, a JSON number or null and preserves the
// literal text.
func (d *Decimal) UnmarshalJSON(data []byte) error {
	text, isNull, err := flexText(data)
	if err != nil {
		return fmt.Errorf("kucoin: decode decimal: %w", err)
	}
	if !isNull { // null is a no-op, matching encoding/json's convention
		*d = Decimal(text)
	}
	return nil
}

// MarshalJSON encodes the decimal as a JSON string.
func (d Decimal) MarshalJSON() ([]byte, error) { return json.Marshal(string(d)) }

// Valid reports whether d holds a well-formed decimal number.
func (d Decimal) Valid() bool {
	_, ok := parseDecimal(string(d))
	return ok
}

// IsZero reports whether d is the number zero. An empty or malformed Decimal
// is not zero; check IsEmpty or Valid first when that matters.
func (d Decimal) IsZero() bool {
	p, ok := parseDecimal(string(d))
	return ok && p.digits == "0"
}

// Sign returns -1, 0 or +1 for a negative, zero or positive number and an
// error when d is not a well-formed decimal.
func (d Decimal) Sign() (int, error) {
	p, ok := parseDecimal(string(d))
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrInvalidDecimal, string(d))
	}
	switch {
	case p.digits == "0":
		return 0, nil
	case p.neg:
		return -1, nil
	default:
		return 1, nil
	}
}

// Float64 returns the nearest float64. It is approximate by nature; prefer Rat
// when exactness matters.
func (d Decimal) Float64() (float64, error) {
	if !d.Valid() {
		return 0, fmt.Errorf("%w: %q", ErrInvalidDecimal, string(d))
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(string(d)), 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q", ErrInvalidDecimal, string(d))
	}
	return f, nil
}

// Rat returns the exact rational value of d.
func (d Decimal) Rat() (*big.Rat, error) {
	canonical, err := d.Canonical()
	if err != nil {
		return nil, err
	}
	r, ok := new(big.Rat).SetString(string(canonical))
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDecimal, string(d))
	}
	return r, nil
}

// Int64 returns d as an integer. It fails when d has a non-zero fractional
// part or does not fit in an int64.
func (d Decimal) Int64() (int64, error) {
	canonical, err := d.Canonical()
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(string(canonical), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q is not an int64", ErrInvalidDecimal, string(d))
	}
	return n, nil
}

// Canonical returns the normalised plain-notation form of d: no exponent, no
// leading or trailing zeros ("84486.0" -> "84486", "1.0E-4" -> "0.0001",
// "-0.00" -> "0"). Two Decimals denote the same number exactly when their
// Canonical forms are equal, which makes it a safe map key.
func (d Decimal) Canonical() (Decimal, error) {
	p, ok := parseDecimal(string(d))
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrInvalidDecimal, string(d))
	}
	return Decimal(p.String()), nil
}

// Cmp compares two decimals exactly and returns -1, 0 or +1.
func (d Decimal) Cmp(other Decimal) (int, error) {
	a, ok := parseDecimal(string(d))
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrInvalidDecimal, string(d))
	}
	b, ok := parseDecimal(string(other))
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrInvalidDecimal, string(other))
	}
	return a.cmp(b), nil
}

// CompareCanonical compares two strings that are already in Canonical form
// (as produced by Decimal.Canonical) without allocating. It is the hot-path
// comparison used by the order book; results for non-canonical input are
// undefined.
func CompareCanonical(a, b string) int {
	if a == b {
		return 0
	}
	an, bn := strings.HasPrefix(a, "-"), strings.HasPrefix(b, "-")
	switch {
	case an && !bn:
		return -1
	case !an && bn:
		return 1
	case an && bn:
		return -compareCanonicalPositive(a[1:], b[1:])
	}
	return compareCanonicalPositive(a, b)
}

func compareCanonicalPositive(a, b string) int {
	ai, af := splitPoint(a)
	bi, bf := splitPoint(b)
	if len(ai) != len(bi) {
		if len(ai) < len(bi) {
			return -1
		}
		return 1
	}
	if c := strings.Compare(ai, bi); c != 0 {
		return c
	}
	// Canonical fractions have no trailing zeros, so a plain lexicographic
	// comparison orders them correctly ("5" < "55" < "6").
	return strings.Compare(af, bf)
}

func splitPoint(s string) (intPart, frac string) {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return s[:i], s[i+1:]
	}
	return s, ""
}

// parsed is a decimal split into sign, significant digits and the position of
// the decimal point: value = (neg ? -1 : 1) * 0.DIGITS * 10^point... expressed
// instead as digits with an integer-part length, which is simpler to print.
type parsed struct {
	neg    bool
	digits string // significant digits without leading zeros ("0" for zero)
	intLen int    // number of leading digits that belong to the integer part; may be <= 0 or > len(digits)
}

func parseDecimal(s string) (parsed, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return parsed{}, false
	}
	var p parsed
	switch s[0] {
	case '-':
		p.neg = true
		s = s[1:]
	case '+':
		s = s[1:]
	}
	exp := 0
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.Atoi(s[i+1:])
		if err != nil || e > maxExponent || e < -maxExponent {
			return parsed{}, false
		}
		exp = e
		s = s[:i]
	}
	intPart, frac := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart, frac = s[:i], s[i+1:]
	}
	if intPart == "" && frac == "" {
		return parsed{}, false
	}
	for _, part := range [2]string{intPart, frac} {
		for i := 0; i < len(part); i++ {
			if part[i] < '0' || part[i] > '9' {
				return parsed{}, false
			}
		}
	}
	all := intPart + frac
	p.intLen = len(intPart) + exp
	// Strip leading zeros of the digit string, shifting the point with them.
	trimmed := strings.TrimLeft(all, "0")
	p.intLen -= len(all) - len(trimmed)
	trimmed = strings.TrimRight(trimmed, "0")
	if trimmed == "" {
		return parsed{neg: false, digits: "0", intLen: 1}, true
	}
	p.digits = trimmed
	return p, true
}

// String renders p in canonical plain notation.
func (p parsed) String() string {
	if p.digits == "0" {
		return "0"
	}
	var b strings.Builder
	if p.neg {
		b.WriteByte('-')
	}
	switch {
	case p.intLen <= 0:
		b.WriteString("0.")
		b.WriteString(strings.Repeat("0", -p.intLen))
		b.WriteString(p.digits)
	case p.intLen >= len(p.digits):
		b.WriteString(p.digits)
		b.WriteString(strings.Repeat("0", p.intLen-len(p.digits)))
	default:
		b.WriteString(p.digits[:p.intLen])
		b.WriteByte('.')
		b.WriteString(p.digits[p.intLen:])
	}
	return b.String()
}

func (p parsed) cmp(q parsed) int {
	switch {
	case p.digits == "0" && q.digits == "0":
		return 0
	case p.digits == "0":
		if q.neg {
			return 1
		}
		return -1
	case q.digits == "0":
		if p.neg {
			return -1
		}
		return 1
	case p.neg && !q.neg:
		return -1
	case !p.neg && q.neg:
		return 1
	}
	c := p.cmpMagnitude(q)
	if p.neg {
		return -c
	}
	return c
}

func (p parsed) cmpMagnitude(q parsed) int {
	if p.intLen != q.intLen {
		if p.intLen < q.intLen {
			return -1
		}
		return 1
	}
	// Same magnitude order: compare digit strings position by position;
	// digits have no trailing zeros so a proper prefix is smaller.
	return strings.Compare(p.digits, q.digits)
}

// flexText extracts the text of a JSON string or number. For null it reports
// isNull so callers can treat it as a no-op.
func flexText(data []byte) (text string, isNull bool, err error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return "", false, errors.New("empty JSON value")
	}
	switch data[0] {
	case '"':
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return "", false, err
		}
		return s, false, nil
	case 'n':
		if string(data) == "null" {
			return "", true, nil
		}
	case 't', 'f', '{', '[':
		return "", false, fmt.Errorf("unexpected JSON value %.20q, want string or number", data)
	default:
		// A bare number; json.Valid rejects anything that is not exactly one
		// well-formed JSON token.
		if json.Valid(data) {
			return string(data), false, nil
		}
	}
	return "", false, fmt.Errorf("invalid JSON value %.20q", data)
}
