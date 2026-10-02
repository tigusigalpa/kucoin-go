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
	utav2market "github.com/tigusigalpa/kucoin-go/uta/v2/market"
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

// SiteType re-exports transport.SiteType for callers of package kucoin.
type SiteType = transport.SiteType

// Credentials re-exports transport.Credentials for callers of package kucoin.
type Credentials = transport.Credentials

// Clock re-exports transport.Clock for callers of package kucoin.
type Clock = transport.Clock

// Logger re-exports transport.Logger for callers of package kucoin.
type Logger = transport.Logger

// RetryPolicy re-exports transport.RetryPolicy for callers of package kucoin.
type RetryPolicy = transport.RetryPolicy

// SiteTypeGlobal and SiteTypeAustralia re-export KuCoin's supported regional site types.
const (
	SiteTypeGlobal    = transport.SiteTypeGlobal
	SiteTypeAustralia = transport.SiteTypeAustralia
)

// NewDefaultRetryPolicy re-exports transport.NewDefaultRetryPolicy.
var NewDefaultRetryPolicy = transport.NewDefaultRetryPolicy

// NoRetry re-exports transport.NoRetry.
var NoRetry = transport.NoRetry

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

// WithCredentials sets API credentials used for private endpoints.
func WithCredentials(creds Credentials) Option {
	return func(c *ClientConfig) { c.Credentials = creds }
}

// WithUTABaseURL overrides the base URL used for UTA REST requests.
func WithUTABaseURL(url string) Option {
	return func(c *ClientConfig) { c.UTABaseURL = url }
}

// WithClassicBaseURL overrides the base URL used for Classic Spot and Margin REST requests.
func WithClassicBaseURL(url string) Option {
	return func(c *ClientConfig) { c.ClassicBaseURL = url }
}

// WithClassicFuturesBaseURL overrides the base URL used for Classic Futures REST requests.
func WithClassicFuturesBaseURL(url string) Option {
	return func(c *ClientConfig) { c.ClassicFuturesBaseURL = url }
}

// WithHTTPClient sets the HTTP client used for REST requests.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *ClientConfig) { c.HTTPClient = hc }
}

// WithTimeout sets the timeout of the default HTTP client.
func WithTimeout(d time.Duration) Option {
	return func(c *ClientConfig) { c.Timeout = d }
}

// WithSiteType sets the X-SITE-TYPE header sent with requests.
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

// WithClock sets the clock used for request timestamps and retry timing.
func WithClock(clock Clock) Option {
	return func(c *ClientConfig) { c.Clock = clock }
}

// WithLogger sets the logger used by REST transport.
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
	// Ws fetches a legacy private WebSocket bullet-token. The current UTA v2
	// WebSocket flow uses websocket/uta.WithCredentials after welcome; this
	// service remains available only for source compatibility.
	Ws *utaws.Client
	// V2 contains current UTA REST v2 services. Existing UTA v1 services
	// remain in place for source compatibility; do not mix v1 and v2 data
	// models in one workflow without consulting KuCoin's migration notes.
	V2 UTAV2Services
}

// UTAV2Services groups current UTA REST v2 services. It is intentionally
// nested below UTA so Classic and UTA account semantics remain explicit.
type UTAV2Services struct {
	Market *utav2market.Client
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
			V2: UTAV2Services{
				Market: utav2market.NewClient(utaExecutor),
			},
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
