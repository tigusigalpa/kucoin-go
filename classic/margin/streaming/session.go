// Package streaming is the typed WebSocket API for KuCoin Classic Margin. It covers
// every channel of the current Classic Margin WebSocket documentation — two public
// channels (index price, mark price) and six private ones (orders V2 and V1,
// balance, stop orders, cross-margin position, isolated-margin position) — and
// hides the protocol entirely: tokens are obtained and renewed for you, the
// connection reconnects with a fresh token, subscriptions are restored, and every
// push arrives as a Go struct. Applications never parse raw JSON.
//
//	session, err := client.Classic.Margin.Stream.DialPrivate(ctx)
//	if err != nil { ... }
//	defer session.Close()
//
//	positions, err := session.SubscribeCrossMarginPosition(ctx)
//	if err != nil { ... }
//	for ev := range positions.C() {
//		if ev.IsDebtRatio() {
//			fmt.Println(ev.DebtRatio, ev.TotalDebt)
//		}
//	}
//
// Classic Margin shares its WebSocket host, tokens and several channels with
// Classic Spot, so a Session embeds the Session of package classic/spot/streaming
// and has all of its methods. Its private channels — SubscribeOrdersV2,
// SubscribeOrdersV1, SubscribeBalance and SubscribeStopOrders — are the Margin
// channels of the same name: the Margin documentation reuses the Spot
// specifications, a stop order tells Margin and Spot apart by its TradeType
// (TradeTypeMargin and TradeTypeIsolatedMargin of the Spot package) and a balance
// update by its RelationEvent. Spot's public channels (tickers, order books,
// trades, ...) are available on a Margin session too.
//
// The private channels need a session from DialPrivate, which requires API
// credentials on the client.
//
// Channels in the current docs that are not implemented: none. WebSocket order
// entry is not market data and is not provided; use the REST order endpoints.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/base-info/introduction
package streaming

import (
	"context"
	"fmt"
	"strings"
	"unicode"

	spotstreaming "github.com/tigusigalpa/kucoin-go/classic/spot/streaming"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/websocket/classic"
)

// Local validation errors are the ones of package classic/spot/streaming: the
// same values, so errors.Is matches either name.
var (
	// ErrNoSymbols is returned when a subscription names no symbol.
	ErrNoSymbols = spotstreaming.ErrNoSymbols
	// ErrTooManySymbols is returned when a subscription names more than
	// MaxSymbolsPerSubscription symbols.
	ErrTooManySymbols = spotstreaming.ErrTooManySymbols
	// ErrInvalidSymbol is returned for an empty symbol or one containing a comma,
	// colon, underscore, slash or white space that would corrupt the topic.
	ErrInvalidSymbol = spotstreaming.ErrInvalidSymbol
	// ErrPrivateConnectionRequired is returned when a private channel is
	// subscribed on a session opened with DialPublic.
	ErrPrivateConnectionRequired = spotstreaming.ErrPrivateConnectionRequired
)

// MaxSymbolsPerSubscription is how many symbols KuCoin accepts in one topic.
const MaxSymbolsPerSubscription = spotstreaming.MaxSymbolsPerSubscription

// Service opens Classic Margin streaming sessions. Obtain it as
// Client.Classic.Margin.Stream.
type Service struct {
	spot *spotstreaming.Service
}

// NewService builds a Service from the two token calls (the Spot/Margin bullet
// tokens) and the order-book snapshot call that the managed order book of the
// embedded Spot channels uses. opts become the default connection options of
// every session. It is not normally called directly; use kucoin.NewClient.
func NewService(public, private spotstreaming.TokenFunc, snapshot spotstreaming.SnapshotFunc, opts ...stream.Option) *Service {
	return &Service{spot: spotstreaming.NewService(public, private, snapshot, opts...)}
}

// DialPublic opens a connection for the public channels (the two Margin ones and
// the public channels of Spot). It needs no credentials.
func (s *Service) DialPublic(ctx context.Context, opts ...stream.Option) (*Session, error) {
	sess, err := s.spot.DialPublic(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &Session{Session: sess}, nil
}

// DialPrivate opens a connection for the private account channels (and the
// public ones). The client must be configured with API credentials; without
// them the call fails at once, before any network access, with an error that
// matches transport.ErrCredentialsRequired.
func (s *Service) DialPrivate(ctx context.Context, opts ...stream.Option) (*Session, error) {
	sess, err := s.spot.DialPrivate(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &Session{Session: sess, private: true}, nil
}

// Session is one live Classic Margin WebSocket connection with typed
// subscriptions. It embeds the Spot Session, whose methods (Close, Shutdown,
// State, Events, Stats, Done, Err, Client and every Spot subscription) it has too.
// It is safe for concurrent use. Close it when done: that ends every subscription
// and releases every goroutine.
type Session struct {
	*spotstreaming.Session
	private bool
}

// validName reports whether s can be a symbol inside a topic: it must not be
// empty and must not contain a character that delimits the parts of a topic. It
// is the rule of package classic/spot/streaming.
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
// once, as in package classic/spot/streaming.
func joinSymbols(symbols []string) (string, error) {
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
		parts = append(parts, symbol)
	}
	if len(parts) > MaxSymbolsPerSubscription {
		return "", fmt.Errorf("%w: %d (limit %d)", ErrTooManySymbols, len(parts), MaxSymbolsPerSubscription)
	}
	return strings.Join(parts, ","), nil
}

func subscribeTopic[T any](s *Session, ctx context.Context, prefix string, symbols []string, decode classic.DecodeFunc[T], opts []stream.SubscribeOption) (*stream.Subscription[T], error) {
	list, err := joinSymbols(symbols)
	if err != nil {
		return nil, err
	}
	return classic.SubscribeTyped(ctx, s.Client(), prefix+list, false, decode, opts...)
}

// SubscribeIndexPrice subscribes to the index prices of up to 100 symbols such as
// "USDT-BTC", pushed once a second (/indicator/index:{symbol}).
func (s *Session) SubscribeIndexPrice(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[IndexPrice], error) {
	return subscribeTopic(s, ctx, "/indicator/index:", symbols, decodeIndexPrice, opts)
}

// SubscribeMarkPrice subscribes to the mark prices of up to 100 symbols such as
// "USDT-BTC", pushed once a second (/indicator/markPrice:{symbol}).
func (s *Session) SubscribeMarkPrice(ctx context.Context, symbols []string, opts ...stream.SubscribeOption) (*stream.Subscription[MarkPrice], error) {
	return subscribeTopic(s, ctx, "/indicator/markPrice:", symbols, decodeMarkPrice, opts)
}

// SubscribeCrossMarginPosition subscribes to the cross-margin position: the debt
// ratio with the asset and debt lists every three seconds while there is a
// liability, and a notice whenever the position status changes
// (/margin/position). It needs DialPrivate.
func (s *Session) SubscribeCrossMarginPosition(ctx context.Context, opts ...stream.SubscribeOption) (*stream.Subscription[CrossMarginPositionEvent], error) {
	if !s.private {
		return nil, ErrPrivateConnectionRequired
	}
	return classic.SubscribeTyped(ctx, s.Client(), "/margin/position", true, decodeCrossMarginPosition, opts...)
}

// SubscribeIsolatedMarginPosition subscribes to the position of one isolated-margin
// symbol such as "BTC-USDT": a notice whenever its status changes and a periodic
// update while it carries a liability (/margin/isolatedPosition:{symbol}). It
// needs DialPrivate.
func (s *Session) SubscribeIsolatedMarginPosition(ctx context.Context, symbol string, opts ...stream.SubscribeOption) (*stream.Subscription[IsolatedMarginPositionEvent], error) {
	if !validName(symbol) {
		return nil, fmt.Errorf("%w: %q", ErrInvalidSymbol, symbol)
	}
	if !s.private {
		return nil, ErrPrivateConnectionRequired
	}
	return classic.SubscribeTyped(ctx, s.Client(), "/margin/isolatedPosition:"+symbol, true, decodeIsolatedMarginPosition, opts...)
}
