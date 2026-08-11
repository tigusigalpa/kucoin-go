// Package auth implements KuCoin's HMAC-SHA256 request-signing scheme.
//
// Docs: https://www.kucoin.com/docs-new/authentication
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"time"
)

// Signer computes KuCoin's KC-API-SIGN and KC-API-PASSPHRASE header values
// from a single API secret.
//
// Docs: https://www.kucoin.com/docs-new/authentication
type Signer struct {
	secret string
}

// NewSigner creates a Signer bound to a single API key's secret.
func NewSigner(secret string) *Signer {
	return &Signer{secret: secret}
}

// Sign computes KC-API-SIGN: Base64(HMAC-SHA256(secret, prehash)), where
// prehash = timestamp + UPPERCASE(method) + endpoint (including an
// unencoded query string, if any) + body (compact JSON, or "" for
// GET/DELETE with no body).
//
// Docs: https://www.kucoin.com/docs-new/authentication
func (s *Signer) Sign(timestamp, method, endpoint, body string) string {
	prehash := timestamp + strings.ToUpper(method) + endpoint + body
	return hmacBase64(s.secret, prehash)
}

// SignPassphrase computes KC-API-PASSPHRASE: the API passphrase, itself
// HMAC-SHA256-signed with the API secret and Base64-encoded. KuCoin never
// transmits the raw passphrase.
//
// Docs: https://www.kucoin.com/docs-new/authentication
func (s *Signer) SignPassphrase(passphrase string) string {
	return hmacBase64(s.secret, passphrase)
}

func hmacBase64(secret, message string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(message))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// TimestampMillis returns t as a Unix-millisecond string, the format
// KC-API-TIMESTAMP requires.
func TimestampMillis(t time.Time) string {
	return strconv.FormatInt(t.UnixMilli(), 10)
}
