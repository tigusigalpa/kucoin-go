// Package streaming is the typed WebSocket API for KuCoin Classic Spot. It
// covers every channel of the current Classic Spot WebSocket documentation —
// twelve public market-data channels and four private account channels — and
// hides the protocol entirely: tokens are obtained and renewed for you, the
// connection reconnects with a fresh token, subscriptions are restored, and
// every push arrives as a Go struct. Applications never parse raw JSON.
//
//	session, err := client.Classic.Spot.Stream.DialPublic(ctx)
//	if err != nil { ... }
//	defer session.Close()
//
//	ticks, err := session.SubscribeTicker(ctx, []string{"BTC-USDT", "ETH-USDT"})
//	if err != nil { ... }
//	for tick := range ticks.C() {
//		fmt.Println(tick.Symbol, tick.BestBid, tick.BestAsk)
//	}
//
// SubscribeOrderBook maintains a local, exact-decimal order book from the level-2
// incremental feed and a REST snapshot, resynchronising on its own after a
// sequence gap, a reconnect or lost pushes. Unlike every other channel it needs
// API credentials, because KuCoin serves the Spot full order-book snapshot only to
// signed requests; see SubscribeOrderBook.
//
// Private channels (orders, balance, stop orders) need a session from
// DialPrivate, which requires API credentials on the client. The same
// WebSocket host and tokens serve Classic Margin, whose typed channels are in
// package classic/margin/streaming; its sessions embed the Session of this
// package.
//
// KuCoin acknowledges a subscription to a symbol or a candle interval that does
// not exist and then simply never sends anything. Intervals are therefore
// checked locally; take symbol names from the symbol list of the REST API.
//
// Channels in the current docs that are not implemented: none. WebSocket order
// entry (add/cancel order over the socket) is not market data and is not
// provided; use the REST order endpoints.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/introduction
package streaming

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	classicws "github.com/tigusigalpa/kucoin-go/classic/ws"
	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
	"github.com/tigusigalpa/kucoin-go/websocket/classic"
)

// Local validation errors. They are returned before anything is sent. The Margin
// streaming package returns the same values.
var (
	// ErrNoSymbols is returned when a subscription names no symbol.
	ErrNoSymbols = errors.New("kucoin: spot/margin stream: at least one symbol is required")
	// ErrTooManySymbols is returned when a subscription names more than
	// MaxSymbolsPerSubscription symbols.
	ErrTooManySymbols = errors.New("kucoin: spot/margin stream: too many symbols in one subscription")
	// ErrInvalidSymbol is returned for an empty symbol or market name, or one
	// containing a comma, colon, underscore, slash or white space that would
	// corrupt the topic. Symbols use a hyphen ("BTC-USDT"); a market is a name
	// such as "BTC" or "USDS".
	ErrInvalidSymbol = errors.New("kucoin: spot/margin stream: invalid symbol")
	// ErrInvalidInterval is returned for a candle interval KuCoin does not offer.
	ErrInvalidInterval = errors.New("kucoin: spot/margin stream: invalid candle interval")
	// ErrPrivateConnectionRequired is returned when a private channel is
	// subscribed on a session opened with DialPublic.
	ErrPrivateConnectionRequired = errors.New("kucoin: spot/margin stream: this channel needs a session from DialPrivate")
	// ErrNoSnapshotSource is returned by SubscribeOrderBook when the Service was
	// built without a REST snapshot function.
	ErrNoSnapshotSource = errors.New("kucoin: spot/margin stream: no order-book snapshot source configured")
)

// MaxSymbolsPerSubscription is how many symbols KuCoin accepts in one topic.
const MaxSymbolsPerSubscription = 100

// allTickersKey is the topic suffix of the all-tickers channel; as a symbol it
// would address that channel instead of a symbol.
const allTickersKey = "all"

// TokenFunc fetches a WebSocket connection token (the REST bullet-token calls).
type TokenFunc func(ctx context.Context) (*classicws.Token, error)

// SnapshotFunc fetches the full REST order book of a symbol
// (GET /api/v3/market/orderbook/level2). KuCoin serves that endpoint only to
// signed requests, so the function needs API credentials; without them it fails
// with an error matching transport.ErrCredentialsRequired.
type SnapshotFunc func(ctx context.Context, symbol string) (orderbook.Snapshot, error)

// Service opens Classic Spot streaming sessions. Obtain it as
// Client.Classic.Spot.Stream.
type Service struct {
	public, private TokenFunc
	snapshot        SnapshotFunc
	defaults        []stream.Option
}

// NewService builds a Service from the two token calls and the order-book
// snapshot call. opts become the default connection options of every session.
// It is not normally called directly; use kucoin.NewClient.
func NewService(public, private TokenFunc, snapshot SnapshotFunc, opts ...stream.Option) *Service {
	return &Service{public: public, private: private, snapshot: permanentSnapshot(snapshot), defaults: opts}
}

// DialPublic opens a connection for the public market-data channels. It needs no
// credentials (the order book of SubscribeOrderBook does, see there).
func (s *Service) DialPublic(ctx context.Context, opts ...stream.Option) (*Session, error) {
	return s.dial(ctx, false, opts)
}

// DialPrivate opens a connection for the private account channels (and the
// public ones). The client must be configured with API credentials; without
// them the call fails at once, before any network access, with an error that
// matches transport.ErrCredentialsRequired.
func (s *Service) DialPrivate(ctx context.Context, opts ...stream.Option) (*Session, error) {
	return s.dial(ctx, true, opts)
}

func (s *Service) dial(ctx context.Context, private bool, opts []stream.Option) (*Session, error) {
	token := s.public
	if private {
		token = s.private
	}
	if token == nil {
		return nil, errors.New("kucoin: spot/margin stream: no token source configured")
	}
	all := append(append([]stream.Option(nil), s.defaults...), opts...)
	client := classic.NewClientWithTokenSource(classic.TokenSourceFunc(permanentOnAuthFailure(token)), classic.WithStreamOptions(all...))
	if err := client.Connect(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &Session{client: client, snapshot: s.snapshot, private: private}, nil
}

// isAuthFailure reports whether err says the API credentials are missing or were
// rejected, which no retry can fix.
func isAuthFailure(err error) bool {
	return errors.Is(err, transport.ErrCredentialsRequired) || errors.Is(err, transport.ErrUnauthorized)
}

// permanentOnAuthFailure marks token errors that retrying cannot fix, so the
// reconnect loop gives up on them instead of hammering the API.
func permanentOnAuthFailure(f TokenFunc) func(context.Context) (*classicws.Token, error) {
	return func(ctx context.Context) (*classicws.Token, error) {
		tok, err := f(ctx)
		if err != nil && isAuthFailure(err) {
			return nil, stream.Permanent(err)
		}
		return tok, err
	}
}

// permanentSnapshot marks snapshot errors that retrying cannot fix, so a book
// whose credentials are missing or rejected ends at once with a clear error
// instead of retrying a call that cannot succeed. It leaves a nil function nil.
func permanentSnapshot(f SnapshotFunc) SnapshotFunc {
	if f == nil {
		return nil
	}
	return func(ctx context.Context, symbol string) (orderbook.Snapshot, error) {
		snap, err := f(ctx, symbol)
		switch {
		case err == nil:
		case errors.Is(err, transport.ErrCredentialsRequired):
			err = stream.Permanent(fmt.Errorf("kucoin: spot/margin stream: the order-book snapshot is a signed endpoint and needs API credentials: %w", err))
		case errors.Is(err, transport.ErrUnauthorized):
			err = stream.Permanent(fmt.Errorf("kucoin: spot/margin stream: the API credentials were rejected for the order-book snapshot: %w", err))
		}
		return snap, err
	}
}

// Session is one live Classic Spot WebSocket connection with typed
// subscriptions. It is safe for concurrent use. Close it when done: that ends
// every subscription and releases every goroutine.
type Session struct {
	client   *classic.Client
	snapshot SnapshotFunc
	private  bool
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

// Stats returns the connection counters.
func (s *Session) Stats() stream.Stats { return s.client.Stats() }

// Done is closed when the session is closed, by Close or by an unrecoverable
// error; Err then says which.
func (s *Session) Done() <-chan struct{} { return s.client.Done() }

// Err returns the unrecoverable error that closed the session, or nil.
func (s *Session) Err() error { return s.client.Err() }

// Client returns the underlying low-level client, for topics this package does
// not cover.
func (s *Session) Client() *classic.Client { return s.client }

// validName reports whether s can be a symbol or a market name inside a topic:
// it must not be empty and must not contain a character that delimits the
// parts of a topic.
func validName(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r == ',' || r == ':' || r == '_' || r == '/' || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

// joinSymbols builds the symbol list of a topic. A symbol listed twice is listed
// once: the connection routes every push to each occurrence of its topic, so a
// repeated symbol would deliver every update of it twice.
func joinSymbols(symbols []string, decorate func(string) string) (string, error) {
	if len(symbols) == 0 {
		return "", ErrNoSymbols
	}
	seen := make(map[string]struct{}, len(symbols))
	parts := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		if !validName(symbol) {
			return "", fmt.Errorf("%w: %q", ErrInvalidSymbol, symbol)
		}
		if _, repeated := seen[symbol]; repeated {
			continue
		}
		seen[symbol] = struct{}{}
		parts = append(parts, decorate(symbol))
	}
	if len(parts) > MaxSymbolsPerSubscription {
		return "", fmt.Errorf("%w: %d (limit %d)", ErrTooManySymbols, len(parts), MaxSymbolsPerSubscription)
	}
	return strings.Join(parts, ","), nil
}

func identity(s string) string { return s }

func subscribeTopic[T any](s *Session, ctx context.Context, prefix string, symbols []string, decode classic.DecodeFunc[T], opts []stream.SubscribeOption) (*stream.Subscription[T], error) {
	list, err := joinSymbols(symbols, identity)
	if err != nil {
		return nil, err
	}
	return classic.SubscribeTyped(ctx, s.client, prefix+list, false, decode, opts...)
}

func subscribePrivate[T any](s *Session, ctx context.Context, topic string, decode classic.DecodeFunc[T], opts []stream.SubscribeOption) (*stream.Subscription[T], error) {
	if !s.private {
		return nil, ErrPrivateConnectionRequired
	}
	return classic.SubscribeTyped(ctx, s.client, topic, true, decode, opts...)
}

// SubscribeTicker subscribes to best-bid/offer updates of up to 100 symbols
// (/market/ticker:{symbol},{symbol}), pushed at most every 100ms per symbol. The
// name "all" is reserved for SubscribeAllTickers.
//
// Every symbol can be part of one subscription of a channel at a time:
// subscribing a symbol twice on the same session returns an error matching
// stream.ErrAlreadySubscribed. A symbol listed twice in one call is subscribed
// once.
func (s *Session) SubscribeTicker(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[Ticker], error) {
	for _, symbol := range symbols {
		if symbol == allTickersKey {
			return nil, fmt.Errorf("%w: %q is the all-tickers channel, use SubscribeAllTickers", ErrInvalidSymbol, symbol)
		}
	}
	return subscribeTopic(s, ctx, "/market/ticker:", symbols, decodeTicker, opts)
}

// SubscribeAllTickers subscribes to the best-bid/offer updates of every symbol
// (/market/ticker:all), pushed at most every 100ms per symbol. The symbol of each
// update is in AllTickerUpdate.Symbol. The channel is busy: pass
// stream.WithBuffer to size the queue for a slower consumer.
func (s *Session) SubscribeAllTickers(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[AllTickerUpdate], error) {
	return classic.SubscribeTyped(ctx, s.client, "/market/ticker:"+allTickersKey, false, decodeAllTicker, opts...)
}

// SubscribeSymbolSnapshot subscribes to the market statistics of up to 100
// symbols, pushed every two seconds (/market/snapshot:{symbol}).
func (s *Session) SubscribeSymbolSnapshot(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[SymbolSnapshot], error) {
	return subscribeTopic(s, ctx, "/market/snapshot:", symbols, decodeSymbolSnapshot, opts)
}

// SubscribeMarketSnapshot subscribes to the market statistics of every symbol of
// one market, such as "BTC", "USDS" or "ALTS", pushed every two seconds
// (/market/snapshot:{market}). Each update carries the symbol it belongs to.
func (s *Session) SubscribeMarketSnapshot(ctx context.Context, market string, opts ...stream.SubscribeOption) (*stream.Subscription[SymbolSnapshot], error) {
	if !validName(market) {
		return nil, fmt.Errorf("%w: market %q", ErrInvalidSymbol, market)
	}
	return classic.SubscribeTyped(ctx, s.client, "/market/snapshot:"+market, false, decodeMarketSnapshot, opts...)
}

// SubscribeLevel1 subscribes to the best bid and offer of up to 100 symbols,
// pushed at most every 10ms when the top of the book changes
// (/spotMarket/level1:{symbol}).
func (s *Session) SubscribeLevel1(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[Level1], error) {
	return subscribeTopic(s, ctx, "/spotMarket/level1:", symbols, decodeLevel1, opts)
}

// SubscribeDepth5 subscribes to the best five levels of each side, pushed at most
// every 100ms (/spotMarket/level2Depth5:{symbol}). Each push is a complete
// snapshot.
func (s *Session) SubscribeDepth5(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[OrderBookDepth], error) {
	return subscribeTopic(s, ctx, "/spotMarket/level2Depth5:", symbols, decodeDepth, opts)
}

// SubscribeDepth50 subscribes to the best fifty levels of each side, pushed at
// most every 100ms (/spotMarket/level2Depth50:{symbol}). Each push is a complete
// snapshot.
func (s *Session) SubscribeDepth50(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[OrderBookDepth], error) {
	return subscribeTopic(s, ctx, "/spotMarket/level2Depth50:", symbols, decodeDepth, opts)
}

// SubscribeOrderBookChanges subscribes to the raw level-2 incremental updates
// (/market/level2:{symbol}). You are responsible for combining them with a REST
// snapshot; most applications should use SubscribeOrderBook instead.
func (s *Session) SubscribeOrderBookChanges(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[OrderBookChange], error) {
	return subscribeTopic(s, ctx, "/market/level2:", symbols, decodeOrderBookChange, opts)
}

// SubscribeKlines subscribes to candle updates of the given interval for up to
// 100 symbols (/market/candles:{symbol}_{type}), pushed in real time while the
// candle changes.
func (s *Session) SubscribeKlines(ctx context.Context, interval Interval, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[Kline], error) {
	if !interval.Valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidInterval, string(interval))
	}
	list, err := joinSymbols(symbols, func(symbol string) string { return symbol + "_" + string(interval) })
	if err != nil {
		return nil, err
	}
	return classic.SubscribeTyped(ctx, s.client, "/market/candles:"+list, false, decodeKline, opts...)
}

// SubscribeTrades subscribes to the matches of up to 100 symbols
// (/market/match:{symbol}), pushed in real time.
func (s *Session) SubscribeTrades(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[Trade], error) {
	return subscribeTopic(s, ctx, "/market/match:", symbols, decodeTrade, opts)
}

// SubscribeCallAuctionDepth50 subscribes to the best fifty levels of each side
// during the call auction of a symbol, pushed at most every 100ms
// (/callauction/level2Depth50:{symbol}). Nothing is pushed while the symbol is
// not in a call auction. The documentation shows a single symbol; KuCoin
// acknowledges a list of up to 100 here as on every other channel.
func (s *Session) SubscribeCallAuctionDepth50(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[OrderBookDepth], error) {
	return subscribeTopic(s, ctx, "/callauction/level2Depth50:", symbols, decodeDepth, opts)
}

// SubscribeCallAuctionData subscribes to the estimated price and size and the
// order price ranges of the call auction of a symbol, pushed at most every 100ms
// (/callauction/callauctionData:{symbol}). Nothing is pushed while the symbol is
// not in a call auction. The documentation shows a single symbol; KuCoin
// acknowledges a list of up to 100 here as on every other channel.
func (s *Session) SubscribeCallAuctionData(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[CallAuctionData], error) {
	return subscribeTopic(s, ctx, "/callauction/callauctionData:", symbols, decodeCallAuctionData, opts)
}

// SubscribeOrdersV2 subscribes to every change of the user's orders, Spot and
// Margin alike (/spotMarket/tradeOrdersV2). It needs DialPrivate. Compared with
// SubscribeOrdersV1 it adds the "received" event (OrderEventReceived with
// OrderStatusNew) that is pushed when an order enters the matching system, before
// it was matched; there is no difference in speed.
func (s *Session) SubscribeOrdersV2(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[OrderUpdate], error) {
	return subscribePrivate(s, ctx, "/spotMarket/tradeOrdersV2", decodeOrderUpdate, opts)
}

// SubscribeOrdersV1 subscribes to every change of the user's orders, Spot and
// Margin alike (/spotMarket/tradeOrders). It needs DialPrivate. It does not
// deliver the "received" event of SubscribeOrdersV2; prefer V2 in new code.
//
// Subscribing V1 and V2 on the same session delivers the events V1 has twice, once
// per subscription.
func (s *Session) SubscribeOrdersV1(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[OrderUpdate], error) {
	return subscribePrivate(s, ctx, "/spotMarket/tradeOrders", decodeOrderUpdate, opts)
}

// SubscribeBalance subscribes to the balance changes of every account of the user
// (/account/balance). It needs DialPrivate.
func (s *Session) SubscribeBalance(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[BalanceUpdate], error) {
	return subscribePrivate(s, ctx, "/account/balance", decodeBalance, opts)
}

// SubscribeStopOrders subscribes to the stop-order events of the user, Spot and
// Margin alike (/spotMarket/advancedOrders). It needs DialPrivate.
func (s *Session) SubscribeStopOrders(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[StopOrderUpdate], error) {
	return subscribePrivate(s, ctx, "/spotMarket/advancedOrders", decodeStopOrder, opts)
}
