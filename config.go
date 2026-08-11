// Package kucoin is an idiomatic Go client for KuCoin's UTA (Unified
// Trading Account) and Classic (Spot/Margin/Futures) REST and WebSocket
// APIs.
//
// UTA and Classic are exposed as explicit, separate service roots
// (Client.UTA / Client.Classic) rather than a merged type — they are
// different account models with different permissions and data shapes,
// and treating them as interchangeable would be misleading.
//
// Docs: https://www.kucoin.com/docs-new/introduction
package kucoin

import (
	"net/http"
	"time"

	"github.com/tigusigalpa/kucoin-go/transport"
	"github.com/tigusigalpa/kucoin-go/uta/market"
)

// Default production REST host. UTA and Classic currently share a host;
// kept as two options so they can diverge without a breaking change.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/introduction
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/introduction
const (
	DefaultUTABaseURL     = "https://api.kucoin.com"
	DefaultClassicBaseURL = "https://api.kucoin.com"
)

// Re-exported from package transport so callers only need to import
// package kucoin for the common case.
type (
	SiteType    = transport.SiteType
	Credentials = transport.Credentials
	Clock       = transport.Clock
	Logger      = transport.Logger
	RetryPolicy = transport.RetryPolicy
)

const (
	SiteTypeGlobal    = transport.SiteTypeGlobal
	SiteTypeAustralia = transport.SiteTypeAustralia
)

var (
	NewDefaultRetryPolicy = transport.NewDefaultRetryPolicy
	NoRetry               = transport.NoRetry
)

// ClientConfig holds every configurable knob of a Client. Zero value is
// usable; NewClient fills in defaults for unset fields.
type ClientConfig struct {
	Credentials Credentials

	UTABaseURL     string
	ClassicBaseURL string

	HTTPClient *http.Client
	Timeout    time.Duration

	SiteType SiteType
	EnableNS bool

	Clock  Clock
	Logger Logger

	RetryPolicy *RetryPolicy
}

// Option configures a ClientConfig at construction time.
type Option func(*ClientConfig)

func WithCredentials(creds Credentials) Option {
	return func(c *ClientConfig) { c.Credentials = creds }
}

func WithUTABaseURL(url string) Option {
	return func(c *ClientConfig) { c.UTABaseURL = url }
}

func WithClassicBaseURL(url string) Option {
	return func(c *ClientConfig) { c.ClassicBaseURL = url }
}

func WithHTTPClient(hc *http.Client) Option {
	return func(c *ClientConfig) { c.HTTPClient = hc }
}

func WithTimeout(d time.Duration) Option {
	return func(c *ClientConfig) { c.Timeout = d }
}

func WithSiteType(siteType SiteType) Option {
	return func(c *ClientConfig) { c.SiteType = siteType }
}

// WithEnableNS sends "kc-enable-ns: true", requesting nanosecond-precision
// x-in-time/x-out-time response headers.
//
// Docs: https://www.kucoin.com/docs-new/authentication
func WithEnableNS(enabled bool) Option {
	return func(c *ClientConfig) { c.EnableNS = enabled }
}

func WithClock(clock Clock) Option {
	return func(c *ClientConfig) { c.Clock = clock }
}

func WithLogger(logger Logger) Option {
	return func(c *ClientConfig) { c.Logger = logger }
}

// WithRetryPolicy overrides the default conservative retry policy (GET
// requests only; see transport.RetryPolicy).
func WithRetryPolicy(policy *RetryPolicy) Option {
	return func(c *ClientConfig) { c.RetryPolicy = policy }
}

func newConfig(opts ...Option) *ClientConfig {
	cfg := &ClientConfig{
		UTABaseURL:     DefaultUTABaseURL,
		ClassicBaseURL: DefaultClassicBaseURL,
		Timeout:        15 * time.Second,
		SiteType:       SiteTypeGlobal,
		Clock:          transport.SystemClock{},
		Logger:         transport.NoopLogger{},
		RetryPolicy:    transport.NewDefaultRetryPolicy(),
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: cfg.Timeout}
	}
	return cfg
}

// UTAServices groups every implemented UTA (Unified Trading Account)
// service.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/introduction
type UTAServices struct {
	Market *market.Client
}

// Client is the SDK entry point. Construct with NewClient. Public
// endpoints (e.g. Client.UTA.Market) work with zero-value Credentials;
// private endpoints return ErrCredentialsRequired locally if none are
// configured.
type Client struct {
	cfg *ClientConfig

	utaExecutor *transport.Executor

	// UTA groups every implemented UTA (Unified Trading Account) service.
	UTA UTAServices
}

// NewClient builds a fully wired Client. It performs no network I/O.
func NewClient(opts ...Option) *Client {
	cfg := newConfig(opts...)

	utaExecutor := transport.NewExecutor(transport.ExecutorConfig{
		BaseURL:     cfg.UTABaseURL,
		Credentials: cfg.Credentials,
		HTTPClient:  cfg.HTTPClient,
		SiteType:    cfg.SiteType,
		EnableNS:    cfg.EnableNS,
		Clock:       cfg.Clock,
		Logger:      cfg.Logger,
		RetryPolicy: cfg.RetryPolicy,
	})

	return &Client{
		cfg:         cfg,
		utaExecutor: utaExecutor,
		UTA: UTAServices{
			Market: market.NewClient(utaExecutor),
		},
	}
}
