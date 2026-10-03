package types

import (
	"encoding/json"
	"errors"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func TestDecimal_UnmarshalAcceptsStringNumberAndNull(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want Decimal
	}{
		{"quoted", `"84486.5"`, "84486.5"},
		{"number", `84486.5`, "84486.5"},
		{"trailing zero is preserved", `84486.0`, "84486.0"},
		{"java exponent", `1.0E-4`, "1.0E-4"},
		{"negative exponent lowercase", `-2.3e-05`, "-2.3e-05"},
		{"long fraction is not rounded", `1033552780.2532196044`, "1033552780.2532196044"},
		{"huge integer is not rounded", `22075878662488064`, "22075878662488064"},
		{"empty string", `""`, ""},
		{"escaped string", `"1.5"`, "1.5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Decimal
			if err := json.Unmarshal([]byte(tt.in), &got); err != nil {
				t.Fatalf("unmarshal %s: %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDecimal_UnmarshalNullIsNoOp(t *testing.T) {
	d := Decimal("5")
	if err := json.Unmarshal([]byte(`null`), &d); err != nil {
		t.Fatal(err)
	}
	if d != "5" {
		t.Fatalf("null must not clear the value, got %q", d)
	}
}

func TestDecimal_UnmarshalRejectsNonScalars(t *testing.T) {
	for _, in := range []string{`true`, `false`, `{}`, `[]`, `[1]`, `{"a":1}`} {
		var d Decimal
		if err := json.Unmarshal([]byte(in), &d); err == nil {
			t.Errorf("unmarshal %s: expected an error", in)
		}
	}
}

func TestDecimal_InStructWithMissingAndNullFields(t *testing.T) {
	type payload struct {
		Price Decimal `json:"price"`
		Size  Decimal `json:"size"`
		Fee   Decimal `json:"fee"`
	}
	var p payload
	if err := json.Unmarshal([]byte(`{"price":84486.5,"size":"3","fee":null}`), &p); err != nil {
		t.Fatal(err)
	}
	if p.Price != "84486.5" || p.Size != "3" || !p.Fee.IsEmpty() {
		t.Fatalf("unexpected payload: %+v", p)
	}
}

func TestDecimal_MarshalIsAlwaysAString(t *testing.T) {
	b, err := json.Marshal(struct {
		A Decimal `json:"a"`
		B Decimal `json:"b"`
	}{"1.0E-4", ""})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"a":"1.0E-4","b":""}` {
		t.Fatalf("unexpected JSON: %s", b)
	}
}

func TestDecimal_Canonical(t *testing.T) {
	tests := map[string]string{
		"84486":                   "84486",
		"84486.0":                 "84486",
		"84486.00":                "84486",
		"84486.50":                "84486.5",
		"0.0001":                  "0.0001",
		"1.0E-4":                  "0.0001",
		"1E-4":                    "0.0001",
		"0.00010":                 "0.0001",
		"-2.3e-05":                "-0.000023",
		"+5":                      "5",
		".5":                      "0.5",
		"5.":                      "5",
		"0":                       "0",
		"0.000":                   "0",
		"-0.00":                   "0",
		"-0":                      "0",
		"00012.3400":              "12.34",
		"1e3":                     "1000",
		"1E+3":                    "1000",
		"1.5e3":                   "1500",
		"12345678901234567890.12": "12345678901234567890.12",
		"  7.0 ":                  "7",
	}
	for in, want := range tests {
		got, err := Decimal(in).Canonical()
		if err != nil {
			t.Errorf("Canonical(%q): %v", in, err)
			continue
		}
		if string(got) != want {
			t.Errorf("Canonical(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDecimal_CanonicalRejectsMalformed(t *testing.T) {
	for _, in := range []string{"", " ", "-", "+", ".", "abc", "1.2.3", "1e", "1e+", "e5", "0x10", "inf", "NaN", "1 2", "1e99999", "--1", "1,5"} {
		if _, err := Decimal(in).Canonical(); !errors.Is(err, ErrInvalidDecimal) {
			t.Errorf("Canonical(%q): want ErrInvalidDecimal, got %v", in, err)
		}
		if Decimal(in).Valid() {
			t.Errorf("Valid(%q) = true", in)
		}
	}
}

func TestDecimal_CmpAndSign(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"1", "1.0", 0},
		{"84486.0", "84486", 0},
		{"1.0E-4", "0.0001", 0},
		{"2", "10", -1},
		{"10", "2", 1},
		{"0.5", "0.05", 1},
		{"0.05", "0.5", -1},
		{"0.5", "0.55", -1},
		{"-1", "1", -1},
		{"1", "-1", 1},
		{"-1", "-2", 1},
		{"-2", "-1", -1},
		{"0", "-0.0", 0},
		{"0", "0.0001", -1},
		{"-0.0001", "0", -1},
		{"1e3", "999.99", 1},
		{"123456789012345678901234567890", "123456789012345678901234567891", -1},
	}
	for _, tt := range tests {
		got, err := Decimal(tt.a).Cmp(Decimal(tt.b))
		if err != nil {
			t.Errorf("Cmp(%q,%q): %v", tt.a, tt.b, err)
			continue
		}
		if got != tt.want {
			t.Errorf("Cmp(%q,%q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
	if _, err := Decimal("x").Cmp("1"); err == nil {
		t.Error("Cmp with a malformed receiver must fail")
	}
	if _, err := Decimal("1").Cmp("x"); err == nil {
		t.Error("Cmp with a malformed argument must fail")
	}
	for in, want := range map[string]int{"5": 1, "-5": -1, "0": 0, "0.0": 0, "-0": 0, "1.0E-9": 1} {
		got, err := Decimal(in).Sign()
		if err != nil || got != want {
			t.Errorf("Sign(%q) = %d,%v want %d", in, got, err, want)
		}
	}
	if _, err := Decimal("").Sign(); err == nil {
		t.Error("Sign of an empty decimal must fail")
	}
}

func TestDecimal_IsZero(t *testing.T) {
	for in, want := range map[string]bool{"0": true, "0.00": true, "-0": true, "0E5": true, "": false, "1": false, "0.0001": false, "x": false} {
		if got := Decimal(in).IsZero(); got != want {
			t.Errorf("IsZero(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestDecimal_NumericConversions(t *testing.T) {
	if f, err := Decimal("1.0E-4").Float64(); err != nil || f != 0.0001 {
		t.Errorf("Float64 = %v, %v", f, err)
	}
	for _, bad := range []string{"", "x", "0x10", "inf", "NaN"} {
		if _, err := Decimal(bad).Float64(); err == nil {
			t.Errorf("Float64(%q) must fail", bad)
		}
	}
	r, err := Decimal("0.1").Rat()
	if err != nil || r.Cmp(big.NewRat(1, 10)) != 0 {
		t.Errorf("Rat(0.1) = %v, %v", r, err)
	}
	if _, err := Decimal("").Rat(); err == nil {
		t.Error("Rat of an empty decimal must fail")
	}
	if n, err := Decimal("1.0E3").Int64(); err != nil || n != 1000 {
		t.Errorf("Int64(1.0E3) = %d, %v", n, err)
	}
	if _, err := Decimal("12.5").Int64(); err == nil {
		t.Error("Int64 of a fractional value must fail")
	}
	if _, err := Decimal("9223372036854775808").Int64(); err == nil {
		t.Error("Int64 overflow must fail")
	}
}

func TestCompareCanonical_MatchesBigRat(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	gen := func() string {
		var b strings.Builder
		if rng.Intn(4) == 0 {
			b.WriteByte('-')
		}
		b.WriteString(strconv.Itoa(rng.Intn(1000)))
		if rng.Intn(3) != 0 {
			b.WriteByte('.')
			for i, n := 0, 1+rng.Intn(6); i < n; i++ {
				b.WriteByte(byte('0' + rng.Intn(10)))
			}
		}
		return b.String()
	}
	for i := 0; i < 20000; i++ {
		a, b := gen(), gen()
		ca, err := Decimal(a).Canonical()
		if err != nil {
			t.Fatal(err)
		}
		cb, err := Decimal(b).Canonical()
		if err != nil {
			t.Fatal(err)
		}
		ra, _ := new(big.Rat).SetString(a)
		rb, _ := new(big.Rat).SetString(b)
		want := ra.Cmp(rb)
		if got := CompareCanonical(string(ca), string(cb)); got != want {
			t.Fatalf("CompareCanonical(%q,%q) = %d, big.Rat says %d", ca, cb, got, want)
		}
		viaCmp, err := Decimal(a).Cmp(Decimal(b))
		if err != nil || viaCmp != want {
			t.Fatalf("Cmp(%q,%q) = %d,%v, big.Rat says %d", a, b, viaCmp, err, want)
		}
		if (ca == cb) != (want == 0) {
			t.Fatalf("canonical equality disagrees for %q vs %q", a, b)
		}
	}
}

func TestID_UnmarshalKeepsExactDigits(t *testing.T) {
	type payload struct {
		Spot ID `json:"spot"`
		Fut  ID `json:"fut"`
	}
	var p payload
	if err := json.Unmarshal([]byte(`{"spot":22075878662488064,"fut":"1928560862264"}`), &p); err != nil {
		t.Fatal(err)
	}
	if p.Spot != "22075878662488064" || p.Fut != "1928560862264" {
		t.Fatalf("unexpected ids: %+v", p)
	}
	b, _ := json.Marshal(p)
	if string(b) != `{"spot":"22075878662488064","fut":"1928560862264"}` {
		t.Fatalf("unexpected JSON: %s", b)
	}
	if err := json.Unmarshal([]byte(`{"spot":true}`), &p); err == nil {
		t.Fatal("a boolean id must be rejected")
	}
}

func TestInt64_UnmarshalForms(t *testing.T) {
	tests := []struct {
		in      string
		want    Int64
		wantErr bool
	}{
		{`1741164936624`, 1741164936624, false},
		{`"1741164936624"`, 1741164936624, false},
		{`1740641976241000000`, 1740641976241000000, false},
		{`"1729843222921000000"`, 1729843222921000000, false},
		{`""`, 0, false},
		{`null`, 7, false}, // no-op: keeps the pre-set value
		{`12.0`, 12, false},
		{`1.7E3`, 1700, false},
		{`"-5"`, -5, false},
		{`1.5`, 0, true},
		{`"abc"`, 0, true},
		{`true`, 0, true},
		{`9223372036854775808`, 0, true},
	}
	for _, tt := range tests {
		n := Int64(7)
		err := json.Unmarshal([]byte(tt.in), &n)
		if tt.wantErr {
			if err == nil {
				t.Errorf("%s: expected an error, got %d", tt.in, n)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tt.in, err)
			continue
		}
		if n != tt.want {
			t.Errorf("%s: got %d, want %d", tt.in, n, tt.want)
		}
	}
	b, _ := json.Marshal(Int64(42))
	if string(b) != "42" || Int64(42).Value() != 42 {
		t.Fatalf("Int64 marshals as a number, got %s", b)
	}
}
