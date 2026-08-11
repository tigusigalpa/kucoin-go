package transport

import (
	"math/rand"
	"net/http"
	"time"
)

// SiteType selects KuCoin's regional site for the optional X-SITE-TYPE
// header.
//
// Docs: https://www.kucoin.com/docs-new/authentication
type SiteType = string

const (
	SiteTypeGlobal    SiteType = "global"
	SiteTypeAustralia SiteType = "australia"
)

// Credentials are a single KuCoin API key's identity. Read these from
// environment variables or your own secret store — never hardcode them.
//
// Docs: https://www.kucoin.com/docs-new/authentication
type Credentials struct {
	APIKey        string
	APISecret     string
	APIPassphrase string

	// APIKeyVersion is the "KC-API-KEY-VERSION" header value assigned when
	// the key was created. Per-key metadata, not a fixed protocol
	// constant — do not hardcode it.
	APIKeyVersion string
}

// IsZero reports whether no credentials were supplied, meaning only
// public endpoints can be called.
func (c Credentials) IsZero() bool {
	return c.APIKey == "" && c.APISecret == "" && c.APIPassphrase == ""
}

func (c Credentials) isComplete() bool {
	return c.APIKey != "" && c.APISecret != "" && c.APIPassphrase != ""
}

// Clock abstracts time so tests can control timestamps and callers can
// implement documented clock-skew correction against KuCoin's server-time
// endpoint. Never synchronized implicitly.
type Clock interface {
	Now() time.Time
}

// SystemClock is the real wall clock, used unless a Clock is injected.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }

// Logger is a minimal structured-logging interface. Implementations must
// never log credentials, signatures, or full private request bodies.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// NoopLogger discards everything; the default unless a Logger is injected.
type NoopLogger struct{}

func (NoopLogger) Debug(string, ...any) {}
func (NoopLogger) Info(string, ...any)  {}
func (NoopLogger) Warn(string, ...any)  {}
func (NoopLogger) Error(string, ...any) {}

// RetryPolicy controls automatic retries for idempotent (GET) REST calls
// only. It is never applied to POST/DELETE requests (place/cancel/amend
// order, transfer, withdrawal, etc.) regardless of configuration.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts, including the first.
	// 1 disables retries.
	MaxAttempts int

	// BaseDelay and MaxDelay bound the exponential backoff (with full
	// jitter) between attempts.
	BaseDelay time.Duration
	MaxDelay  time.Duration

	// MaxElapsed caps total time spent retrying a single call, including
	// the original attempt.
	MaxElapsed time.Duration

	// OnRetry, if set, is called before each retry attempt (1-indexed,
	// excluding the first attempt) with the delay about to be slept.
	OnRetry func(attempt int, delay time.Duration, err error)
}

// NewDefaultRetryPolicy returns a conservative policy: 3 attempts,
// 250ms-5s exponential backoff with full jitter, 20s max elapsed time.
func NewDefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   250 * time.Millisecond,
		MaxDelay:    5 * time.Second,
		MaxElapsed:  20 * time.Second,
	}
}

// NoRetry disables automatic retries entirely.
func NoRetry() *RetryPolicy {
	return &RetryPolicy{MaxAttempts: 1}
}

func (p *RetryPolicy) delayForAttempt(attempt int) time.Duration {
	if p.BaseDelay <= 0 {
		return 0
	}
	backoff := p.BaseDelay << uint(attempt-1) //nolint:gosec
	if backoff <= 0 || backoff > p.MaxDelay {
		backoff = p.MaxDelay
	}
	//nolint:gosec // non-cryptographic jitter is fine for retry pacing
	return time.Duration(rand.Int63n(int64(backoff) + 1))
}

// ExecutorConfig configures a single Executor, which talks to one REST
// base URL (KuCoin's UTA and Classic APIs currently share a host, but a
// separate Executor per API keeps them independently configurable).
type ExecutorConfig struct {
	BaseURL     string
	Credentials Credentials
	HTTPClient  *http.Client
	SiteType    SiteType
	EnableNS    bool
	Clock       Clock
	Logger      Logger
	RetryPolicy *RetryPolicy
}
