package types

import (
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func TestArithmeticTable(t *testing.T) {
	tests := []struct {
		a, b          string
		add, sub, mul string
	}{
		{"1", "2", "3", "-1", "2"},
		{"0.1", "0.2", "0.3", "-0.1", "0.02"},
		{"84486.0", "0.5", "84486.5", "84485.5", "42243"},
		{"1.0E-4", "2.0E-4", "0.0003", "-0.0001", "0.00000002"},
		{"-5", "5", "0", "-10", "-25"},
		{"0", "0", "0", "0", "0"},
		{"100", "0.001", "100.001", "99.999", "0.1"},
		{"1e3", "1e-3", "1000.001", "999.999", "1"},
		{"-2.5", "-2.5", "-5", "0", "6.25"},
		{"123456789012345678901234567890.123456789", "1", "123456789012345678901234567891.123456789", "123456789012345678901234567889.123456789", "123456789012345678901234567890.123456789"},
		{"0.000000000000000000001", "0.000000000000000000001", "0.000000000000000000002", "0", "0.000000000000000000000000000000000000000001"},
	}
	for _, tt := range tests {
		a, b := Decimal(tt.a), Decimal(tt.b)
		for name, got := range map[string]func() (Decimal, error){
			"add": func() (Decimal, error) { return a.Add(b) },
			"sub": func() (Decimal, error) { return a.Sub(b) },
			"mul": func() (Decimal, error) { return a.Mul(b) },
		} {
			want := map[string]string{"add": tt.add, "sub": tt.sub, "mul": tt.mul}[name]
			res, err := got()
			if err != nil {
				t.Errorf("%s(%s,%s): %v", name, tt.a, tt.b, err)
				continue
			}
			if string(res) != want {
				t.Errorf("%s(%s,%s) = %s, want %s", name, tt.a, tt.b, res, want)
			}
		}
	}
}

func TestArithmeticRejectsMalformed(t *testing.T) {
	for _, bad := range []string{"", "x", "1.2.3"} {
		if _, err := Decimal(bad).Add("1"); err == nil {
			t.Errorf("Add(%q, 1) must fail", bad)
		}
		if _, err := Decimal("1").Sub(Decimal(bad)); err == nil {
			t.Errorf("Sub(1, %q) must fail", bad)
		}
		if _, err := Decimal(bad).Mul("1"); err == nil {
			t.Errorf("Mul(%q, 1) must fail", bad)
		}
		if _, err := Decimal("1").Mul(Decimal(bad)); err == nil {
			t.Errorf("Mul(1, %q) must fail", bad)
		}
	}
}

func TestArithmeticMatchesBigRat(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	gen := func() string {
		var b strings.Builder
		if rng.Intn(3) == 0 {
			b.WriteByte('-')
		}
		b.WriteString(strconv.Itoa(rng.Intn(100000)))
		if rng.Intn(3) != 0 {
			b.WriteByte('.')
			for i, n := 0, 1+rng.Intn(9); i < n; i++ {
				b.WriteByte(byte('0' + rng.Intn(10)))
			}
		}
		return b.String()
	}
	for i := 0; i < 5000; i++ {
		as, bs := gen(), gen()
		ra, _ := new(big.Rat).SetString(as)
		rb, _ := new(big.Rat).SetString(bs)
		check := func(op string, got Decimal, err error, want *big.Rat) {
			t.Helper()
			if err != nil {
				t.Fatalf("%s(%s,%s): %v", op, as, bs, err)
			}
			rg, ok := new(big.Rat).SetString(string(got))
			if !ok || rg.Cmp(want) != 0 {
				t.Fatalf("%s(%s,%s) = %s, want %s", op, as, bs, got, want.FloatString(20))
			}
			canonical, _ := got.Canonical()
			if canonical != got {
				t.Fatalf("%s(%s,%s) = %s is not canonical (%s)", op, as, bs, got, canonical)
			}
		}
		sum, err := Decimal(as).Add(Decimal(bs))
		check("add", sum, err, new(big.Rat).Add(ra, rb))
		diff, err := Decimal(as).Sub(Decimal(bs))
		check("sub", diff, err, new(big.Rat).Sub(ra, rb))
		prod, err := Decimal(as).Mul(Decimal(bs))
		check("mul", prod, err, new(big.Rat).Mul(ra, rb))
	}
}
