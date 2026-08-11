package auth

import (
	"testing"
	"time"
)

// TestSign_KnownVectors verifies KC-API-SIGN against vectors computed
// independently via `openssl dgst -sha256 -hmac`, covering a GET without
// query params, a GET with query params, and a POST with a JSON body.
//
// Docs: https://www.kucoin.com/docs-new/authentication
func TestSign_KnownVectors(t *testing.T) {
	s := NewSigner("test-api-secret")

	tests := []struct {
		name      string
		timestamp string
		method    string
		endpoint  string
		body      string
		want      string
	}{
		{
			name:      "GET without query",
			timestamp: "1622185200000",
			method:    "GET",
			endpoint:  "/api/ua/v1/account/ledger",
			body:      "",
			want:      "A+drUvUhuK9B9lW6p+c9G0k0AbDvBaRADV4hinVgh9c=",
		},
		{
			name:      "GET with query",
			timestamp: "1622185200000",
			method:    "GET",
			endpoint:  "/api/ua/v1/market/ticker?tradeType=SPOT&symbol=BTC-USDT",
			body:      "",
			want:      "Y4CIw8vypGuYmE/JDB/Kr9UGyO9kzC31hIe/BZIIPlw=",
		},
		{
			name:      "POST with JSON body",
			timestamp: "1622185200000",
			method:    "POST",
			endpoint:  "/api/v1/hf/orders",
			body:      `{"symbol":"BTC-USDT","side":"buy","type":"limit","price":"10000","size":"0.001"}`,
			want:      "25TF2rDhMDPs6LTi4iqBthYb/vIw1i1rFsf+/o6ts5s=",
		},
		{
			// KuCoin uppercases the method internally, so a caller-supplied
			// lowercase method must still sign identically.
			name:      "lowercase method is uppercased",
			timestamp: "1622185200000",
			method:    "get",
			endpoint:  "/api/ua/v1/account/ledger",
			body:      "",
			want:      "A+drUvUhuK9B9lW6p+c9G0k0AbDvBaRADV4hinVgh9c=",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s.Sign(tt.timestamp, tt.method, tt.endpoint, tt.body)
			if got != tt.want {
				t.Errorf("Sign() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestSignPassphrase_KnownVector verifies KC-API-PASSPHRASE (the
// passphrase itself, HMAC-SHA256-signed with the API secret) against a
// vector computed independently via `openssl dgst -sha256 -hmac`.
//
// Docs: https://www.kucoin.com/docs-new/authentication
func TestSignPassphrase_KnownVector(t *testing.T) {
	s := NewSigner("test-api-secret")
	got := s.SignPassphrase("test-passphrase")
	want := "fqkeR28GaLc+yzydx1yFfD3Jc46eSFoOwfG2YgL/Qos="
	if got != want {
		t.Errorf("SignPassphrase() = %q, want %q", got, want)
	}
}

func TestTimestampMillis(t *testing.T) {
	tm := time.Date(2021, 5, 28, 6, 0, 0, 0, time.UTC)
	got := TimestampMillis(tm)
	want := "1622181600000"
	if got != want {
		t.Errorf("TimestampMillis() = %q, want %q", got, want)
	}
}
