// Package streaming is the typed WebSocket API for KuCoin's current UTA WebSocket
// v2. It covers every channel of the UTA WebSocket v2 documentation — eight public
// market-data channels and seven private account channels — and hides the protocol
// entirely: connections reconnect on their own, subscriptions are restored, private
// connections authenticate with a signed message after the welcome frame (and again
// after every reconnect), and every push arrives as a Go struct. Applications never
// parse raw JSON.
//
//	session, err := client.UTA.V2.Stream.DialFutures(ctx)
//	if err != nil { ... }
//	defer session.Close()
//
//	ticks, err := session.SubscribeTicker(ctx, []string{"XBTUSDTM", "ETHUSDTM"})
//	if err != nil { ... }
//	for tick := range ticks.C() {
//		fmt.Println(tick.Symbol, tick.BestBidPrice, tick.BestAskPrice)
//	}
//
// # Sessions
//
// KuCoin serves the markets from separate hosts, and a session is bound to one:
// DialSpot (public spot data), DialFutures (public futures data) and DialPrivate
// (account data; it needs API credentials). A channel that does not exist on the
// session's market, or a private channel on a public session, is refused locally
// with ErrFuturesOnly, ErrSpotOnly, ErrPrivateSessionRequired or ErrWrongSession
// before anything is sent.
//
// # Channels
//
// Public: SubscribeTicker, SubscribeTrades, SubscribeKlines,
// SubscribeOrderBookUpdates (every depth), SubscribeMarkPrice, SubscribeFundingRate
// and SubscribeAllFundingRates (futures only), SubscribeCallAuction (spot only).
// Private: SubscribeOrders, SubscribeExecutions, SubscribeExecutionsLite,
// SubscribeBalance, SubscribePositions, SubscribeLiquidationWarning and
// SubscribeLeverage.
//
// Every method takes a list of up to MaxSymbolsPerSubscription symbols. KuCoin
// accepts several symbols in one subscription request for the ticker and
// funding-fee channels only; for trade, kline, obu, mark-price and callAuctionInfo
// it rejects the "symbols" form ("symbols are not supported for topic ..."), so
// this package sends one request per symbol, a few at a time and spaced apart, and
// merges the results into the single Subscription it returns. Either way the
// subscription is all or nothing: if one symbol is rejected, the others are
// unsubscribed again and the error is returned. Subscription options
// (stream.WithBuffer, stream.WithOverflow) apply to each request separately. A
// connection may hold 600 topics (public) or 400 (private), and KuCoin may
// disconnect one that sends more than 300 (public) or 100 (private) messages per
// 10 seconds; for more topics, open more sessions.
//
// # Order books
//
// SubscribeOrderBook maintains a local, exact-decimal order book from KuCoin's
// 10ms incremental feed (depth increment@10ms), which pushes a snapshot first and
// deltas afterwards, so it needs no REST call. After a sequence gap, a decode
// failure or updates the connection had to drop it subscribes again, which makes
// the server push a fresh snapshot; after a reconnect the new connection's
// snapshot restores it. A consumer that reads the book's events too slowly does not
// disturb the book. Note that KuCoin delays the first snapshot of this feed by
// roughly one to five seconds after a subscription. SubscribeOrderBookIncrement does
// the same for the deprecated "increment" depth with a REST snapshot you provide.
//
// The other depths need no maintenance: depths 1, 5 and 50 push complete snapshots
// (SubscribeOrderBookUpdates).
//
// # Not implemented
//
// WebSocket order entry (adding, canceling and amending orders over the socket,
// KuCoin's "WS trade" connection) is not market data and is not provided; use the
// REST order endpoints.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/introduction
package streaming

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
	"github.com/tigusigalpa/kucoin-go/websocket/uta"
)

// Local validation errors. They are returned before anything is sent.
var (
	// ErrNoSymbols is returned when a subscription names no symbol.
	ErrNoSymbols = errors.New("kucoin: uta stream: at least one symbol is required")
	// ErrTooManySymbols is returned when a subscription names more than
	// MaxSymbolsPerSubscription symbols.
	ErrTooManySymbols = errors.New("kucoin: uta stream: too many symbols in one subscription")
	// ErrInvalidSymbol is returned for an empty symbol or one containing white
	// space or a comma.
	ErrInvalidSymbol = errors.New("kucoin: uta stream: invalid symbol")
	// ErrInvalidInterval is returned for a candle interval KuCoin does not offer on
	// the session's market (6hour does not exist on futures).
	ErrInvalidInterval = errors.New("kucoin: uta stream: invalid candle interval")
	// ErrInvalidDepth is returned for an order-book depth KuCoin does not offer, or
	// one the requested options cannot use (the RPI filter supports depths 5 and 50).
	ErrInvalidDepth = errors.New("kucoin: uta stream: invalid order-book depth")
	// ErrInvalidAccountType is returned for a balance account type KuCoin does not
	// offer.
	ErrInvalidAccountType = errors.New("kucoin: uta stream: invalid account type")
	// ErrWrongSession is returned when a market-data channel is subscribed on a
	// session opened with DialPrivate, which carries no market data.
	ErrWrongSession = errors.New("kucoin: uta stream: market-data channels need a session from DialSpot or DialFutures")
	// ErrPrivateSessionRequired is returned when a private channel is subscribed on
	// a session opened with DialSpot or DialFutures.
	ErrPrivateSessionRequired = errors.New("kucoin: uta stream: this channel needs a session from DialPrivate")
	// ErrFuturesOnly is returned when a futures-only channel or option is used on a
	// spot session.
	ErrFuturesOnly = errors.New("kucoin: uta stream: this channel exists on futures only (use DialFutures)")
	// ErrSpotOnly is returned when a spot-only channel is used on a futures session.
	ErrSpotOnly = errors.New("kucoin: uta stream: this channel exists on spot only (use DialSpot)")
	// ErrNoSnapshotSource is returned by SubscribeOrderBookIncrement when the
	// Service was built without a REST snapshot function.
	ErrNoSnapshotSource = errors.New("kucoin: uta stream: no order-book snapshot source configured")
)

// MaxSymbolsPerSubscription is the most symbols one subscription call accepts.
// KuCoin documents no limit per request; this is the SDK's own cap, as every
// symbol of a fanned-out channel is a separate request. KuCoin's real limits are
// per connection: 600 topics on a public and 400 on a private connection, and 300
// (public) or 100 (private) client messages per 10 seconds, subscribe,
// unsubscribe and ping included; a connection that exceeds them may be
// disconnected. Open another session to subscribe to more.
const MaxSymbolsPerSubscription = 100

// Channel names of the UTA WebSocket.
const (
	channelTicker             = "ticker"
	channelKline              = "kline"
	channelTrade              = "trade"
	channelOrderBook          = "obu"
	channelMarkPrice          = "mark-price"
	channelFundingRate        = "funding-fee"
	channelAllFundingRates    = "funding-fee-all-symbols"
	channelCallAuction        = "callAuctionInfo"
	channelOrder              = "order"
	channelOrderAll           = "orderAll"
	channelExecution          = "execution"
	channelExecutionLite      = "execution.lite"
	channelBalance            = "balance"
	channelPosition           = "position"
	channelPositionAll        = "positionAll"
	channelLiquidationWarning = "lw"
	channelLeverage           = "leverage"

	// privateTradeType is the trade type of every private subscription: the
	// unified account delivers all products on one subscription.
	privateTradeType = string(TradeTypeUnified)
)

// Hosts are the WebSocket endpoints a Service dials. An empty field selects the
// documented host of the uta package (PublicSpotWSURL, PublicFuturesWSURL and
// PrivateWSURL); set a field to point a Service at a proxy or a test server.
type Hosts struct {
	// Spot is the public spot host.
	Spot string
	// Futures is the public futures host.
	Futures string
	// Private is the private host.
	Private string
}

func (h Hosts) withDefaults() Hosts {
	if h.Spot == "" {
		h.Spot = uta.PublicSpotWSURL
	}
	if h.Futures == "" {
		h.Futures = uta.PublicFuturesWSURL
	}
	if h.Private == "" {
		h.Private = uta.PrivateWSURL
	}
	return h
}

// SnapshotFunc fetches the full REST order book of a symbol on a market
// (tradeType is "SPOT" or "FUTURES"). It is only needed for the deprecated
// REST-synchronised book of SubscribeOrderBookIncrement; the UTA order-book
// endpoint requires authentication.
type SnapshotFunc func(ctx context.Context, tradeType, symbol string) (orderbook.Snapshot, error)

// Service opens UTA WebSocket v2 sessions. Obtain it as Client.UTA.V2.Stream.
type Service struct {
	hosts    Hosts
	creds    *transport.Credentials
	clock    transport.Clock
	snapshot SnapshotFunc
	defaults []stream.Option
}

// NewService builds a Service. creds are only used by DialPrivate and may be nil
// for a Service that serves public data; snapshot may be nil unless
// SubscribeOrderBookIncrement is used; opts become the default connection options
// of every session. It is not normally called directly; use kucoin.NewClient.
func NewService(hosts Hosts, creds *transport.Credentials, snapshot SnapshotFunc, opts ...stream.Option) *Service {
	s := &Service{hosts: hosts.withDefaults(), snapshot: snapshot, defaults: opts}
	if creds != nil {
		c := *creds
		s.creds = &c
	}
	return s
}

// WithClock returns a copy of the Service whose private sessions take the
// authentication timestamp from clock, for example a clock corrected against
// KuCoin's server time. KuCoin rejects an authentication whose timestamp is far
// from its own.
func (s *Service) WithClock(clock transport.Clock) *Service {
	c := *s
	c.clock = clock
	return &c
}

// DialSpot opens a connection for the public spot channels. It needs no
// credentials.
func (s *Service) DialSpot(ctx context.Context, opts ...stream.Option) (*Session, error) {
	return s.dial(ctx, s.hosts.Spot, string(TradeTypeSpot), false, opts)
}

// DialFutures opens a connection for the public futures channels. It needs no
// credentials.
func (s *Service) DialFutures(ctx context.Context, opts ...stream.Option) (*Session, error) {
	return s.dial(ctx, s.hosts.Futures, string(TradeTypeFutures), false, opts)
}

// DialPrivate opens an authenticated connection for the private account channels
// of the unified account. The Service needs complete API credentials (key, secret
// and passphrase); without them the call fails at once, before any network access,
// with an error that matches uta.ErrIncompleteCredentials and is permanent
// (stream.IsPermanent). A key KuCoin rejects fails with an error matching
// uta.ErrAuthenticationFailed, also permanent: it is never retried, neither here
// nor after a reconnect.
func (s *Service) DialPrivate(ctx context.Context, opts ...stream.Option) (*Session, error) {
	return s.dial(ctx, s.hosts.Private, "", true, opts)
}

func (s *Service) dial(ctx context.Context, host, tradeType string, private bool, opts []stream.Option) (*Session, error) {
	all := append(append([]stream.Option(nil), s.defaults...), opts...)
	clientOpts := []uta.Option{uta.WithStreamOptions(all...)}
	if private {
		if s.creds == nil || s.creds.APIKey == "" || s.creds.APISecret == "" || s.creds.APIPassphrase == "" {
			return nil, stream.Permanent(fmt.Errorf("kucoin: uta stream: DialPrivate needs an API key, secret and passphrase: %w", uta.ErrIncompleteCredentials))
		}
		clientOpts = append(clientOpts, uta.WithCredentials(*s.creds))
		if s.clock != nil {
			clientOpts = append(clientOpts, uta.WithClock(s.clock))
		}
	}
	client := uta.NewClient(host, "", clientOpts...)
	if err := client.Connect(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &Session{client: client, tradeType: tradeType, private: private, snapshot: s.snapshot}, nil
}

// Session is one live UTA WebSocket connection with typed subscriptions. It is
// safe for concurrent use. Close it when done: that ends every subscription and
// releases every goroutine.
type Session struct {
	client    *uta.Client
	tradeType string
	private   bool
	snapshot  SnapshotFunc
}

// Close ends every subscription, closes the connection and waits for the
// session's goroutines to exit. It is idempotent.
func (s *Session) Close() error { return s.client.Close() }

// Shutdown is Close with a caller-supplied deadline.
func (s *Session) Shutdown(ctx context.Context) error { return s.client.Shutdown(ctx) }

// State returns the connection state (connected, reconnecting, ...).
func (s *Session) State() stream.State { return s.client.State() }

// Events returns the lifecycle events: connects, disconnects, reconnect
// attempts, dropped updates and decode errors. The channel is closed when the
// session closes. Unread events are discarded oldest first, so ignoring the
// channel is safe.
func (s *Session) Events() <-chan stream.Event { return s.client.Events() }

// Stats returns the connection counters. Subscriptions counts the requests made
// to KuCoin, so a channel fanned out over several symbols counts once per symbol.
func (s *Session) Stats() stream.Stats { return s.client.Stats() }

// Done is closed when the session is closed, by Close or by an unrecoverable
// error; Err then says which.
func (s *Session) Done() <-chan struct{} { return s.client.Done() }

// Err returns the unrecoverable error that closed the session, or nil.
func (s *Session) Err() error { return s.client.Err() }

// Client returns the underlying low-level client, for channels this package does
// not cover.
func (s *Session) Client() *uta.Client { return s.client }

// TradeType returns the market of the session: "SPOT" for DialSpot, "FUTURES" for
// DialFutures and "" for a private session.
func (s *Session) TradeType() string { return s.tradeType }

// market checks that a market-data channel is used on a public session.
func (s *Session) market(method string) error {
	if s.private {
		return fmt.Errorf("%w: %s", ErrWrongSession, method)
	}
	return nil
}

// futuresOnly checks that a futures-only channel is used on a futures session.
func (s *Session) futuresOnly(method string) error {
	if err := s.market(method); err != nil {
		return err
	}
	if s.tradeType != string(TradeTypeFutures) {
		return fmt.Errorf("%w: %s", ErrFuturesOnly, method)
	}
	return nil
}

// spotOnly checks that a spot-only channel is used on a spot session.
func (s *Session) spotOnly(method string) error {
	if err := s.market(method); err != nil {
		return err
	}
	if s.tradeType != string(TradeTypeSpot) {
		return fmt.Errorf("%w: %s", ErrSpotOnly, method)
	}
	return nil
}

// privateOnly checks that a private channel is used on a private session.
func (s *Session) privateOnly(method string) error {
	if !s.private {
		return fmt.Errorf("%w: %s", ErrPrivateSessionRequired, method)
	}
	return nil
}

// checkSymbol validates one symbol.
func checkSymbol(symbol string) error {
	if symbol == "" || strings.ContainsAny(symbol, ", \t\r\n") {
		return fmt.Errorf("%w: %q", ErrInvalidSymbol, symbol)
	}
	return nil
}

// checkSymbols validates a symbol list and removes duplicates, keeping the
// order: a duplicate would be routed, and therefore delivered, twice.
func checkSymbols(symbols []string) ([]string, error) {
	if len(symbols) == 0 {
		return nil, ErrNoSymbols
	}
	if len(symbols) > MaxSymbolsPerSubscription {
		return nil, fmt.Errorf("%w: %d (limit %d)", ErrTooManySymbols, len(symbols), MaxSymbolsPerSubscription)
	}
	seen := make(map[string]struct{}, len(symbols))
	out := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		if err := checkSymbol(symbol); err != nil {
			return nil, err
		}
		if _, dup := seen[symbol]; dup {
			continue
		}
		seen[symbol] = struct{}{}
		out = append(out, symbol)
	}
	return out, nil
}

// SubscribeTicker subscribes to the ticker channel for up to 100 symbols
// (ticker). KuCoin pushes a ticker when a trade occurs; best-bid/offer changes
// alone do not trigger one. Several symbols travel in one request.
//
// Docs: https://www.kucoin.com/docs-new/3470355w0
func (s *Session) SubscribeTicker(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[Ticker], error) {
	if err := s.market("SubscribeTicker"); err != nil {
		return nil, err
	}
	list, err := checkSymbols(symbols)
	if err != nil {
		return nil, err
	}
	spec := uta.SubscribeSpec{Channel: channelTicker, TradeType: s.tradeType, Symbols: list}
	return uta.SubscribeTyped(ctx, s.client, spec, decodeTicker, opts...)
}

// SubscribeTrades subscribes to the trade channel for up to 100 symbols (trade),
// one push per match; futures trades carry the RPI flag. KuCoin accepts one symbol
// per request on this channel, so several symbols are subscribed one by one and
// merged (see the package documentation).
//
// Docs: https://www.kucoin.com/docs-new/3470359w0
func (s *Session) SubscribeTrades(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[Trade], error) {
	if err := s.market("SubscribeTrades"); err != nil {
		return nil, err
	}
	list, err := checkSymbols(symbols)
	if err != nil {
		return nil, err
	}
	base := uta.SubscribeSpec{Channel: channelTrade, TradeType: s.tradeType}
	return subscribeEach(ctx, s.client, base, list, decodeTrade, opts)
}

// SubscribeKlines subscribes to the candle updates of one interval for up to 100
// symbols (kline), pushed every second. The 6hour interval exists on spot only;
// KuCoin accepts the subscription on futures but never pushes a candle, so it is
// refused locally with ErrInvalidInterval. Symbols are subscribed one by one and
// merged, as KuCoin accepts one symbol per request on this channel.
//
// Docs: https://www.kucoin.com/docs-new/3470356w0
func (s *Session) SubscribeKlines(ctx context.Context, interval Interval, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[Kline], error) {
	if err := s.market("SubscribeKlines"); err != nil {
		return nil, err
	}
	if !interval.ValidFor(s.tradeType) {
		return nil, fmt.Errorf("%w: %q on %s", ErrInvalidInterval, string(interval), s.tradeType)
	}
	list, err := checkSymbols(symbols)
	if err != nil {
		return nil, err
	}
	base := uta.SubscribeSpec{Channel: channelKline, TradeType: s.tradeType, Interval: string(interval)}
	return subscribeEach(ctx, s.client, base, list, decodeKline, opts)
}

// SubscribeOrderBookUpdates subscribes to the order-book channel at a depth for up
// to 100 symbols (obu). Depth1, Depth5 and Depth50 push complete snapshots that
// replace the previous one; DepthIncrement10ms pushes one snapshot and then
// deltas; DepthIncrement (deprecated by KuCoin) pushes deltas only and needs a
// REST snapshot. Most applications want SubscribeOrderBook, which maintains a
// book from the 10ms feed.
//
// Symbols are subscribed one by one and merged, as KuCoin accepts one symbol per
// request on this channel. A plain subscription and a managed book of the same
// symbol and depth, or an RPI subscription of the same symbol and depth, cannot
// coexist on one session: they would receive the same pushes, which the
// connection rejects with stream.ErrAlreadySubscribed.
//
// Docs: https://www.kucoin.com/docs-new/3470354w0
func (s *Session) SubscribeOrderBookUpdates(ctx context.Context, symbols []string, depth Depth, opts ...stream.SubscribeOption) (*stream.Subscription[OrderBookUpdate], error) {
	return s.subscribeOrderBook(ctx, "SubscribeOrderBookUpdates", symbols, depth, false, opts)
}

// SubscribeOrderBookUpdatesRPI is SubscribeOrderBookUpdates with KuCoin's
// rpiFilter set to 1: the books include Retail Price Improvement orders, and
// every level carries a third element (Level.Size is the non-RPI size,
// Level.RPISize the RPI size). It is available on futures only and only at
// Depth5 and Depth50; anything else is refused locally.
//
// Docs: https://www.kucoin.com/docs-new/3470354w0
func (s *Session) SubscribeOrderBookUpdatesRPI(ctx context.Context, symbols []string, depth Depth, opts ...stream.SubscribeOption) (*stream.Subscription[OrderBookUpdate], error) {
	return s.subscribeOrderBook(ctx, "SubscribeOrderBookUpdatesRPI", symbols, depth, true, opts)
}

func (s *Session) subscribeOrderBook(ctx context.Context, method string, symbols []string, depth Depth, rpi bool, opts []stream.SubscribeOption) (*stream.Subscription[OrderBookUpdate], error) {
	if err := s.market(method); err != nil {
		return nil, err
	}
	if !depth.Valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidDepth, string(depth))
	}
	if rpi {
		if err := s.futuresOnly(method); err != nil {
			return nil, err
		}
		if depth != Depth5 && depth != Depth50 {
			return nil, fmt.Errorf("%w: the RPI filter supports depths 5 and 50, not %q", ErrInvalidDepth, string(depth))
		}
	}
	list, err := checkSymbols(symbols)
	if err != nil {
		return nil, err
	}
	base := uta.SubscribeSpec{Channel: channelOrderBook, TradeType: s.tradeType, Depth: string(depth)}
	if rpi {
		base.RPIFilter = 1
	}
	return subscribeEach(ctx, s.client, base, list, decodeOrderBookUpdate, opts)
}

// SubscribeMarkPrice subscribes to the mark price, index price and open interest
// of up to 100 futures symbols (mark-price), pushed every second. It needs a
// futures session. Symbols are subscribed one by one and merged, as KuCoin accepts
// one symbol per request on this channel.
//
// Docs: https://www.kucoin.com/docs-new/3470358w0
func (s *Session) SubscribeMarkPrice(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[MarkPrice], error) {
	if err := s.futuresOnly("SubscribeMarkPrice"); err != nil {
		return nil, err
	}
	list, err := checkSymbols(symbols)
	if err != nil {
		return nil, err
	}
	return subscribeEach(ctx, s.client, uta.SubscribeSpec{Channel: channelMarkPrice}, list, decodeMarkPrice, opts)
}

// SubscribeFundingRate subscribes to the funding rate and settlement times of up
// to 100 futures symbols (funding-fee), pushed every minute. It needs a futures
// session. Several symbols travel in one request.
//
// Docs: https://www.kucoin.com/docs-new/3470357w0
func (s *Session) SubscribeFundingRate(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[FundingRate], error) {
	if err := s.futuresOnly("SubscribeFundingRate"); err != nil {
		return nil, err
	}
	list, err := checkSymbols(symbols)
	if err != nil {
		return nil, err
	}
	spec := uta.SubscribeSpec{Channel: channelFundingRate, Symbols: list}
	return uta.SubscribeTyped(ctx, s.client, spec, decodeFundingRate, opts...)
}

// SubscribeAllFundingRates subscribes to the funding data of every futures
// contract in one push per minute (funding-fee-all-symbols). It needs a futures
// session. KuCoin notes that this channel's latency may be higher than that of
// SubscribeFundingRate.
//
// Docs: https://www.kucoin.com/docs-new/3470412w0
func (s *Session) SubscribeAllFundingRates(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[AllFundingRates], error) {
	if err := s.futuresOnly("SubscribeAllFundingRates"); err != nil {
		return nil, err
	}
	spec := uta.SubscribeSpec{Channel: channelAllFundingRates}
	return uta.SubscribeTyped(ctx, s.client, spec, decodeAllFundingRates, opts...)
}

// SubscribeCallAuction subscribes to the call-auction data of up to 100 spot
// symbols (callAuctionInfo): while a symbol is in its call-auction phase KuCoin
// pushes the estimated price and size and the order price ranges every 100ms;
// outside it nothing is pushed. It needs a spot session. Symbols are subscribed
// one by one and merged, as KuCoin accepts one symbol per request on this channel.
//
// Docs: https://www.kucoin.com/docs-new/3470353w0
func (s *Session) SubscribeCallAuction(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[CallAuctionInfo], error) {
	if err := s.spotOnly("SubscribeCallAuction"); err != nil {
		return nil, err
	}
	list, err := checkSymbols(symbols)
	if err != nil {
		return nil, err
	}
	return subscribeEach(ctx, s.client, uta.SubscribeSpec{Channel: channelCallAuction}, list, decodeCallAuction, opts)
}

// SubscribeOrders subscribes to the change events of your orders on every product
// of the unified account (spot, margin and futures): for one symbol when symbol
// is set (order), for all symbols when it is empty (orderAll). It needs
// DialPrivate.
//
// Subscribing "all symbols" and a single symbol on the same session delivers the
// events of that symbol twice, once per subscription.
//
// Docs: https://www.kucoin.com/docs-new/3470346w0
func (s *Session) SubscribeOrders(ctx context.Context, symbol string, opts ...stream.SubscribeOption) (*stream.Subscription[OrderUpdate], error) {
	if err := s.privateOnly("SubscribeOrders"); err != nil {
		return nil, err
	}
	spec := uta.SubscribeSpec{Channel: channelOrderAll, TradeType: privateTradeType}
	if symbol != "" {
		if err := checkSymbol(symbol); err != nil {
			return nil, err
		}
		spec.Channel, spec.Symbols = channelOrder, []string{symbol}
	}
	return uta.SubscribeTyped(ctx, s.client, spec, decodeOrder, opts...)
}

// SubscribeExecutions subscribes to your fills (execution), including the fees,
// the fill type and the closed profit and loss, and the fills liquidation,
// auto-deleveraging and settlement cause. It needs DialPrivate. For lower latency
// without the fee fields see SubscribeExecutionsLite.
//
// Docs: https://www.kucoin.com/docs-new/3470406w0
func (s *Session) SubscribeExecutions(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[Execution], error) {
	if err := s.privateOnly("SubscribeExecutions"); err != nil {
		return nil, err
	}
	spec := uta.SubscribeSpec{Channel: channelExecution, TradeType: privateTradeType}
	return uta.SubscribeTyped(ctx, s.client, spec, decodeExecution, opts...)
}

// SubscribeExecutionsLite subscribes to your fills in the lightweight form
// (execution.lite): the same events as SubscribeExecutions without the fee, fee
// currency, fill type and closed PnL, for lower latency. It needs DialPrivate.
//
// Docs: https://www.kucoin.com/docs-new/3470348w0
func (s *Session) SubscribeExecutionsLite(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[ExecutionLite], error) {
	if err := s.privateOnly("SubscribeExecutionsLite"); err != nil {
		return nil, err
	}
	spec := uta.SubscribeSpec{Channel: channelExecutionLite, TradeType: privateTradeType}
	return uta.SubscribeTyped(ctx, s.client, spec, decodeExecutionLite, opts...)
}

// SubscribeBalance subscribes to the balance changes of an account (balance):
// AccountTypeUnified for the unified trading account, AccountTypeFunding for the
// funding account or AccountTypeIsolated. It needs DialPrivate.
//
// Docs: https://www.kucoin.com/docs-new/3470347w0
func (s *Session) SubscribeBalance(ctx context.Context, accountType AccountType, opts ...stream.SubscribeOption) (*stream.Subscription[BalanceUpdate], error) {
	if err := s.privateOnly("SubscribeBalance"); err != nil {
		return nil, err
	}
	if !accountType.Valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidAccountType, string(accountType))
	}
	spec := uta.SubscribeSpec{Channel: channelBalance, AccountType: string(accountType)}
	return uta.SubscribeTyped(ctx, s.client, spec, decodeBalance, opts...)
}

// SubscribePositions subscribes to the futures position updates of one symbol
// when symbol is set (position), or of all symbols when it is empty (positionAll).
// It needs DialPrivate.
//
// Subscribing "all symbols" and a single symbol on the same session delivers the
// updates of that symbol twice, once per subscription.
//
// Docs: https://www.kucoin.com/docs-new/3470350w0
func (s *Session) SubscribePositions(ctx context.Context, symbol string, opts ...stream.SubscribeOption) (*stream.Subscription[PositionUpdate], error) {
	if err := s.privateOnly("SubscribePositions"); err != nil {
		return nil, err
	}
	spec := uta.SubscribeSpec{Channel: channelPositionAll, TradeType: privateTradeType}
	if symbol != "" {
		if err := checkSymbol(symbol); err != nil {
			return nil, err
		}
		spec.Channel, spec.Symbols = channelPosition, []string{symbol}
	}
	return uta.SubscribeTyped(ctx, s.client, spec, decodePosition, opts...)
}

// SubscribeLiquidationWarning subscribes to the risk notices that warn you when a
// position approaches a dangerous risk level (lw): pushed only while the risk
// ratio is 80% or more and the event type changes, otherwise once a minute. It
// needs DialPrivate.
//
// Docs: https://www.kucoin.com/docs-new/3470351w0
func (s *Session) SubscribeLiquidationWarning(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[LiquidationWarning], error) {
	if err := s.privateOnly("SubscribeLiquidationWarning"); err != nil {
		return nil, err
	}
	spec := uta.SubscribeSpec{Channel: channelLiquidationWarning, TradeType: privateTradeType}
	return uta.SubscribeTyped(ctx, s.client, spec, decodeLiquidationWarning, opts...)
}

// SubscribeLeverage subscribes to the leverage changes you make (leverage):
// per symbol for futures, per currency for cross margin. It needs DialPrivate.
//
// Docs: https://www.kucoin.com/docs-new/3470352w0
func (s *Session) SubscribeLeverage(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[LeverageUpdate], error) {
	if err := s.privateOnly("SubscribeLeverage"); err != nil {
		return nil, err
	}
	spec := uta.SubscribeSpec{Channel: channelLeverage, TradeType: privateTradeType}
	return uta.SubscribeTyped(ctx, s.client, spec, decodeLeverage, opts...)
}
