package types

import (
	"fmt"
	"math/big"
	"strings"
)

// scaled is a decimal as an integer mantissa and the number of fractional
// digits: value = mantissa / 10^scale.
type scaled struct {
	mantissa *big.Int
	scale    int
}

func (d Decimal) scaled() (scaled, error) {
	p, ok := parseDecimal(string(d))
	if !ok {
		return scaled{}, fmt.Errorf("%w: %q", ErrInvalidDecimal, string(d))
	}
	if p.digits == "0" {
		return scaled{mantissa: new(big.Int)}, nil
	}
	digits := p.digits
	scale := len(digits) - p.intLen
	if scale < 0 { // an integer with trailing zeros that parse stripped
		digits += strings.Repeat("0", -scale)
		scale = 0
	}
	m, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return scaled{}, fmt.Errorf("%w: %q", ErrInvalidDecimal, string(d))
	}
	if p.neg {
		m.Neg(m)
	}
	return scaled{mantissa: m, scale: scale}, nil
}

func (s scaled) decimal() Decimal {
	if s.mantissa.Sign() == 0 {
		return "0"
	}
	neg := s.mantissa.Sign() < 0
	digits := new(big.Int).Abs(s.mantissa).String()
	var out string
	switch {
	case s.scale <= 0:
		out = digits + strings.Repeat("0", -s.scale)
	case len(digits) > s.scale:
		out = digits[:len(digits)-s.scale] + "." + digits[len(digits)-s.scale:]
	default:
		out = "0." + strings.Repeat("0", s.scale-len(digits)) + digits
	}
	if strings.Contains(out, ".") {
		out = strings.TrimRight(out, "0")
		out = strings.TrimSuffix(out, ".")
	}
	if neg {
		out = "-" + out
	}
	return Decimal(out)
}

func align(a, b scaled) (am, bm *big.Int, scale int) {
	scale = a.scale
	if b.scale > scale {
		scale = b.scale
	}
	am, bm = new(big.Int).Set(a.mantissa), new(big.Int).Set(b.mantissa)
	if a.scale < scale {
		am.Mul(am, pow10(scale-a.scale))
	}
	if b.scale < scale {
		bm.Mul(bm, pow10(scale-b.scale))
	}
	return am, bm, scale
}

func pow10(n int) *big.Int { return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil) }

func binary(a, b Decimal) (scaled, scaled, error) {
	x, err := a.scaled()
	if err != nil {
		return scaled{}, scaled{}, err
	}
	y, err := b.scaled()
	if err != nil {
		return scaled{}, scaled{}, err
	}
	return x, y, nil
}

// Add returns d + other exactly, in canonical plain notation.
func (d Decimal) Add(other Decimal) (Decimal, error) {
	x, y, err := binary(d, other)
	if err != nil {
		return "", err
	}
	xm, ym, scale := align(x, y)
	return scaled{mantissa: xm.Add(xm, ym), scale: scale}.decimal(), nil
}

// Sub returns d - other exactly, in canonical plain notation.
func (d Decimal) Sub(other Decimal) (Decimal, error) {
	x, y, err := binary(d, other)
	if err != nil {
		return "", err
	}
	xm, ym, scale := align(x, y)
	return scaled{mantissa: xm.Sub(xm, ym), scale: scale}.decimal(), nil
}

// Mul returns d * other exactly, in canonical plain notation.
func (d Decimal) Mul(other Decimal) (Decimal, error) {
	x, y, err := binary(d, other)
	if err != nil {
		return "", err
	}
	return scaled{mantissa: new(big.Int).Mul(x.mantissa, y.mantissa), scale: x.scale + y.scale}.decimal(), nil
}
