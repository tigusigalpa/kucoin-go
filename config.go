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

	classicfuturesorders "github.com/tigusigalpa/kucoin-go/classic/futures/orders"
	classicfuturespositions "github.com/tigusigalpa/kucoin-go/classic/futures/positions"
	classicfuturesws "github.com/tigusigalpa/kucoin-go/classic/futures/ws"
	classicmargindebit "github.com/tigusigalpa/kucoin-go/classic/margin/debit"
	classicmarginmarket "github.com/tigusigalpa/kucoin-go/classic/margin/market"
	classicmarginorders "github.com/tigusigalpa/kucoin-go/classic/margin/orders"
	classicspotmarket "github.com/tigusigalpa/kucoin-go/classic/spot/market"
	classicspotorders "github.com/tigusigalpa/kucoin-go/classic/spot/orders"
	classicspotws "github.com/tigusigalpa/kucoin-go/classic/spot/ws"
	"github.com/tigusigalpa/kucoin-go/transport"
	"github.com/tigusigalpa/kucoin-go/uta/account"
	"github.com/tigusigalpa/kucoin-go/uta/leverage"
	"github.com/tigusigalpa/kucoin-go/uta/market"
	"github.com/tigusigalpa/kucoin-go/uta/orders"
	"github.com/tigusigalpa/kucoin-go/uta/positions"
	utaws "github.com/tigusigalpa/kucoin-go/uta/ws"
)

// Default production REST hosts. UTA and Classic Spot currently share a
// host; kept as separate options so they can diverge without a breaking
// change. Classic Futures has always used a distinct host, confirmed
// across every Futures endpoint.
//
// Docs: https://www.kucoin.com/docs-new/rest/ua/introduction
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/introduction
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/introduction
const (
	DefaultUTABaseURL            = "https://api.kucoin.com"
	DefaultClassicBaseURL        = "https://api.kucoin.com"
	DefaultClassicFuturesBaseURL = "https://api-futures.kucoin.com"
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

	UTABaseURL            string
	ClassicBaseURL        string
	ClassicFuturesBaseURL string

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

func WithClassicFuturesBaseURL(url string) Option {
	return func(c *ClientConfig) { c.ClassicFuturesBaseURL = url }
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
		UTABaseURL:            DefaultUTABaseURL,
		ClassicBaseURL:        DefaultClassicBaseURL,
		ClassicFuturesBaseURL: DefaultClassicFuturesBaseURL,
		Timeout:               15 * time.Second,
		SiteType:              SiteTypeGlobal,
		Clock:                 transport.SystemClock{},
		Logger:                transport.NoopLogger{},
		RetryPolicy:           transport.NewDefaultRetryPolicy(),
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
	Market    *market.Client
	Account   *account.Client
	Orders    *orders.Client
	Positions *positions.Client
	Leverage  *leverage.Client
	// Ws fetches the private WebSocket bullet-token
	// (POST /api/v2/bullet-private). It does not open a socket itself —
	// pass the returned token to websocket/uta.NewClient.
	Ws *utaws.Client
}

// SpotServices groups Classic Spot's implemented services.
//
// Docs: https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-all-symbols
type SpotServices struct {
	Market *classicspotmarket.Client
	Orders *classicspotorders.Client
	// Ws fetches public/private WebSocket bullet-tokens
	// (POST /api/v1/bullet-public, /api/v1/bullet-private). It does not
	// open a socket itself — pass the returned token/instanceServers to
	// websocket/classic.NewClient.
	Ws *classicspotws.Client
}

// FuturesServices groups Classic Futures' implemented services (a seed
// set — Phase 1 scope, not the full Futures API; see
// docs/ENDPOINTS.md for exactly what's covered).
//
// Docs: https://www.kucoin.com/docs-new/rest/futures-trading/introduction
type FuturesServices struct {
	Orders    *classicfuturesorders.Client
	Positions *classicfuturespositions.Client
	// Ws fetches public/private WebSocket bullet-tokens
	// (POST /api/v1/bullet-public, /api/v1/bullet-private). It does not
	// open a socket itself — pass the returned token/instanceServers to
	// websocket/classic.NewClient.
	Ws *classicfuturesws.Client
}

// MarginServices groups Classic Margin's implemented services (a seed
// set covering symbols/mark-price/config/risk-limit market data, core
// order management, and borrow/repay/interest — not the full Margin API;
// see docs/ENDPOINTS.md for exactly what's covered). Margin shares
// Classic Spot's host (api.kucoin.com).
//
// Docs: https://www.kucoin.com/docs-new/rest/margin-trading/introduction
type MarginServices struct {
	Market *classicmarginmarket.Client
	Orders *classicmarginorders.Client
	Debit  *classicmargindebit.Client
}

// ClassicServices groups every implemented Classic (pre-UTA) account-mode
// service.
type ClassicServices struct {
	Spot    SpotServices
	Futures FuturesServices
	Margin  MarginServices
}

// Client is the SDK entry point. Construct with NewClient. Public
// endpoints (e.g. Client.UTA.Market) work with zero-value Credentials;
// private endpoints return ErrCredentialsRequired locally if none are
// configured.
type Client struct {
	cfg *ClientConfig

	utaExecutor            *transport.Executor
	classicExecutor        *transport.Executor
	classicFuturesExecutor *transport.Executor

	// UTA groups every implemented UTA (Unified Trading Account) service.
	UTA UTAServices
	// Classic groups every implemented Classic account-mode service.
	Classic ClassicServices
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

	classicExecutor := transport.NewExecutor(transport.ExecutorConfig{
		BaseURL:     cfg.ClassicBaseURL,
		Credentials: cfg.Credentials,
		HTTPClient:  cfg.HTTPClient,
		SiteType:    cfg.SiteType,
		EnableNS:    cfg.EnableNS,
		Clock:       cfg.Clock,
		Logger:      cfg.Logger,
		RetryPolicy: cfg.RetryPolicy,
	})

	classicFuturesExecutor := transport.NewExecutor(transport.ExecutorConfig{
		BaseURL:     cfg.ClassicFuturesBaseURL,
		Credentials: cfg.Credentials,
		HTTPClient:  cfg.HTTPClient,
		SiteType:    cfg.SiteType,
		EnableNS:    cfg.EnableNS,
		Clock:       cfg.Clock,
		Logger:      cfg.Logger,
		RetryPolicy: cfg.RetryPolicy,
	})

	return &Client{
		cfg:                    cfg,
		classicExecutor:        classicExecutor,
		classicFuturesExecutor: classicFuturesExecutor,
		utaExecutor:            utaExecutor,
		UTA: UTAServices{
			Market:    market.NewClient(utaExecutor),
			Account:   account.NewClient(utaExecutor),
			Orders:    orders.NewClient(utaExecutor),
			Positions: positions.NewClient(utaExecutor),
			Leverage:  leverage.NewClient(utaExecutor),
			Ws:        utaws.NewClient(utaExecutor),
		},
		Classic: ClassicServices{
			Spot: SpotServices{
				Market: classicspotmarket.NewClient(classicExecutor),
				Orders: classicspotorders.NewClient(classicExecutor),
				Ws:     classicspotws.NewClient(classicExecutor),
			},
			Futures: FuturesServices{
				Orders:    classicfuturesorders.NewClient(classicFuturesExecutor),
				Positions: classicfuturespositions.NewClient(classicFuturesExecutor),
				Ws:        classicfuturesws.NewClient(classicFuturesExecutor),
			},
			Margin: MarginServices{
				Market: classicmarginmarket.NewClient(classicExecutor),
				Orders: classicmarginorders.NewClient(classicExecutor),
				Debit:  classicmargindebit.NewClient(classicExecutor),
			},
		},
	}
}
