// Package streaming is the typed WebSocket API for KuCoin Classic Futures. It
// covers every channel of the current Classic Futures WebSocket documentation —
// ten public market-data channels and six private account channels — and hides
// the protocol entirely: tokens are obtained and renewed for you, the connection
// reconnects with a fresh token, subscriptions are restored, and every push
// arrives as a Go struct. Applications never parse raw JSON.
//
//	session, err := client.Classic.Futures.Stream.DialPublic(ctx)
//	if err != nil { ... }
//	defer session.Close()
//
//	ticks, err := session.SubscribeTickerV2(ctx, []string{"XBTUSDTM", "ETHUSDTM"})
//	if err != nil { ... }
//	for tick := range ticks.C() {
//		fmt.Println(tick.Symbol, tick.BestBidPrice, tick.BestAskPrice)
//	}
//
// SubscribeOrderBook maintains a local, exact-decimal order book from the
// level-2 incremental feed and a REST snapshot, resynchronising on its own after
// a sequence gap, a reconnect or a slow consumer.
//
// Private channels (orders, balance, positions, ...) need a session from
// DialPrivate, which requires API credentials with the Futures permission on the
// client.
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

	classicws "github.com/tigusigalpa/kucoin-go/classic/ws"
	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
	"github.com/tigusigalpa/kucoin-go/websocket/classic"
)

// Local validation errors. They are returned before anything is sent.
var (
	// ErrNoSymbols is returned when a subscription names no symbol.
	ErrNoSymbols = errors.New("kucoin: futures stream: at least one symbol is required")
	// ErrTooManySymbols is returned when a subscription names more than
	// MaxSymbolsPerSubscription symbols.
	ErrTooManySymbols = errors.New("kucoin: futures stream: too many symbols in one subscription")
	// ErrInvalidSymbol is returned for an empty symbol or one containing a comma
	// or underscore delimiter that would corrupt the topic.
	ErrInvalidSymbol = errors.New("kucoin: futures stream: invalid symbol")
	// ErrInvalidInterval is returned for a candle interval KuCoin does not offer.
	ErrInvalidInterval = errors.New("kucoin: futures stream: invalid candle interval")
	// ErrPrivateConnectionRequired is returned when a private channel is
	// subscribed on a session opened with DialPublic.
	ErrPrivateConnectionRequired = errors.New("kucoin: futures stream: this channel needs a session from DialPrivate")
	// ErrNoSnapshotSource is returned by SubscribeOrderBook when the Service was
	// built without a REST snapshot function.
	ErrNoSnapshotSource = errors.New("kucoin: futures stream: no order-book snapshot source configured")
)

// MaxSymbolsPerSubscription is how many symbols KuCoin accepts in one topic.
const MaxSymbolsPerSubscription = 100

// TokenFunc fetches a WebSocket connection token (the REST bullet-token calls).
type TokenFunc func(ctx context.Context) (*classicws.Token, error)

// SnapshotFunc fetches the full REST order book of a symbol.
type SnapshotFunc func(ctx context.Context, symbol string) (orderbook.Snapshot, error)

// Service opens Classic Futures streaming sessions. Obtain it as
// Client.Classic.Futures.Stream.
type Service struct {
	public, private TokenFunc
	snapshot        SnapshotFunc
	defaults        []stream.Option
}

// NewService builds a Service from the two token calls and the order-book
// snapshot call. opts become the default connection options of every session.
// It is not normally called directly; use kucoin.NewClient.
func NewService(public, private TokenFunc, snapshot SnapshotFunc, opts ...stream.Option) *Service {
	return &Service{public: public, private: private, snapshot: snapshot, defaults: opts}
}

// DialPublic opens a connection for the public market-data channels. It needs no
// credentials.
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
		return nil, errors.New("kucoin: futures stream: no token source configured")
	}
	all := append(append([]stream.Option(nil), s.defaults...), opts...)
	client := classic.NewClientWithTokenSource(classic.TokenSourceFunc(permanentOnAuthFailure(token)), classic.WithStreamOptions(all...))
	if err := client.Connect(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &Session{client: client, snapshot: s.snapshot, private: private}, nil
}

// permanentOnAuthFailure marks token errors that retrying cannot fix, so the
// reconnect loop gives up on them instead of hammering the API.
func permanentOnAuthFailure(f TokenFunc) func(context.Context) (*classicws.Token, error) {
	return func(ctx context.Context) (*classicws.Token, error) {
		tok, err := f(ctx)
		if err != nil && (errors.Is(err, transport.ErrCredentialsRequired) || errors.Is(err, transport.ErrUnauthorized)) {
			return nil, stream.Permanent(err)
		}
		return tok, err
	}
}

// Session is one live Classic Futures WebSocket connection with typed
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
		if symbol == "" || strings.ContainsAny(symbol, ", \t_:") {
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

// SubscribeTickerV2 subscribes to best-bid/offer updates of up to 100 symbols
// (/contractMarket/tickerV2:{symbol}). This is the recommended real-time ticker.
func (s *Session) SubscribeTickerV2(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[TickerV2], error) {
	return subscribeTopic(s, ctx, "/contractMarket/tickerV2:", symbols, decodeTickerV2, opts)
}

// SubscribeTickerV1 subscribes to the deprecated per-trade ticker
// (/contractMarket/ticker:{symbol}). KuCoin recommends SubscribeTickerV2.
func (s *Session) SubscribeTickerV1(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[TickerV1], error) {
	return subscribeTopic(s, ctx, "/contractMarket/ticker:", symbols, decodeTickerV1, opts)
}

// SubscribeDepth5 subscribes to the best five levels of each side, pushed every
// 100ms (/contractMarket/level2Depth5:{symbol}). Each push is a complete
// snapshot of up to five levels per side.
func (s *Session) SubscribeDepth5(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[OrderBookDepth], error) {
	return subscribeTopic(s, ctx, "/contractMarket/level2Depth5:", symbols, decodeDepth, opts)
}

// SubscribeDepth50 subscribes to the best fifty levels of each side, pushed every
// 100ms (/contractMarket/level2Depth50:{symbol}). Each push is a complete
// snapshot of up to fifty levels per side.
func (s *Session) SubscribeDepth50(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[OrderBookDepth], error) {
	return subscribeTopic(s, ctx, "/contractMarket/level2Depth50:", symbols, decodeDepth, opts)
}

// SubscribeOrderBookChanges subscribes to the raw level-2 incremental updates
// (/contractMarket/level2:{symbol}). You are responsible for combining them with
// a REST snapshot; most applications should use SubscribeOrderBook instead.
func (s *Session) SubscribeOrderBookChanges(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[OrderBookChange], error) {
	return subscribeTopic(s, ctx, "/contractMarket/level2:", symbols, decodeOrderBookChange, opts)
}

// SubscribeKlines subscribes to candle updates of the given interval for up to
// 100 symbols (/contractMarket/limitCandle:{symbol}_{type}), pushed every second.
func (s *Session) SubscribeKlines(ctx context.Context, interval Interval, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[Kline], error) {
	if !interval.Valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidInterval, string(interval))
	}
	list, err := joinSymbols(symbols, func(symbol string) string { return symbol + "_" + string(interval) })
	if err != nil {
		return nil, err
	}
	return classic.SubscribeTyped(ctx, s.client, "/contractMarket/limitCandle:"+list, false, decodeKline, opts...)
}

// SubscribeTrades subscribes to matches (/contractMarket/execution:{symbol}).
func (s *Session) SubscribeTrades(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[Trade], error) {
	return subscribeTopic(s, ctx, "/contractMarket/execution:", symbols, decodeTrade, opts)
}

// SubscribeInstrument subscribes to mark price and index price (every second) and
// funding rate (every minute) updates (/contract/instrument:{symbol}). Events
// carry a Subject that tells the two apart.
func (s *Session) SubscribeInstrument(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[InstrumentEvent], error) {
	return subscribeTopic(s, ctx, "/contract/instrument:", symbols, decodeInstrument, opts)
}

// SubscribeFundingSettlement subscribes to the funding-fee settlement notices of
// all contracts (/contract/announcement).
func (s *Session) SubscribeFundingSettlement(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[FundingSettlement], error) {
	return classic.SubscribeTyped(ctx, s.client, "/contract/announcement", false, decodeFundingSettlement, opts...)
}

// SubscribeSnapshot subscribes to the 24-hour statistics of up to 100 symbols,
// pushed every five seconds (/contractMarket/snapshot:{symbol}).
func (s *Session) SubscribeSnapshot(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[SymbolSnapshot], error) {
	return subscribeTopic(s, ctx, "/contractMarket/snapshot:", symbols, decodeSnapshot, opts)
}

// SubscribeOrders subscribes to the order events of one symbol, or of all symbols
// when symbol is empty (/contractMarket/tradeOrders). It needs DialPrivate.
//
// Subscribing "all symbols" and a single symbol on the same session delivers the
// events of that symbol twice, once per subscription.
func (s *Session) SubscribeOrders(ctx context.Context, symbol string, opts ...stream.SubscribeOption) (*stream.Subscription[OrderChange], error) {
	return subscribePrivate(s, ctx, optionalSymbolTopic("/contractMarket/tradeOrders", symbol), decodeOrderChange, opts)
}

// SubscribeStopOrders subscribes to stop-order events
// (/contractMarket/advancedOrders). It needs DialPrivate.
func (s *Session) SubscribeStopOrders(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[StopOrderEvent], error) {
	return subscribePrivate(s, ctx, "/contractMarket/advancedOrders", decodeStopOrder, opts)
}

// SubscribeBalance subscribes to account balance changes
// (/contractAccount/wallet). It needs DialPrivate.
func (s *Session) SubscribeBalance(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[BalanceEvent], error) {
	return subscribePrivate(s, ctx, "/contractAccount/wallet", decodeBalance, opts)
}

// SubscribePositions subscribes to the position updates of one symbol
// (/contract/position:{symbol}), or of all symbols when symbol is empty
// (/contract/positionAll). It needs DialPrivate.
func (s *Session) SubscribePositions(ctx context.Context, symbol string, opts ...stream.SubscribeOption) (*stream.Subscription[PositionEvent], error) {
	topic := "/contract/positionAll"
	if symbol != "" {
		topic = "/contract/position:" + symbol
	}
	return subscribePrivate(s, ctx, topic, decodePosition, opts)
}

// SubscribeMarginMode subscribes to margin-mode changes of all symbols
// (/contract/marginMode). It needs DialPrivate.
func (s *Session) SubscribeMarginMode(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[MarginModeEvent], error) {
	return subscribePrivate(s, ctx, "/contract/marginMode", decodeMarginMode, opts)
}

// SubscribeCrossLeverage subscribes to leverage changes of cross-margin futures
// (/contract/crossLeverage). It needs DialPrivate.
func (s *Session) SubscribeCrossLeverage(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[CrossLeverageEvent], error) {
	return subscribePrivate(s, ctx, "/contract/crossLeverage", decodeCrossLeverage, opts)
}

func optionalSymbolTopic(base, symbol string) string {
	if symbol == "" {
		return base
	}
	return base + ":" + symbol
}
