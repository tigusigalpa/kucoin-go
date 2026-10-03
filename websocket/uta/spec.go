package uta

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
)

// SubscribeSpec describes one UTA v2 channel subscription: which channel, for
// which product, symbols and options. Only the fields a channel uses need to be
// set; the typed streaming package (uta/v2/streaming) fills them in for every
// documented channel.
//
// Docs: https://www.kucoin.com/docs-new/websocket-api/introduction
type SubscribeSpec struct {
	// Channel is the KuCoin channel name: "ticker", "kline", "trade", "obu",
	// "mark-price", "funding-fee", "funding-fee-all-symbols", "callAuctionInfo",
	// and the private "order", "orderAll", "execution", "execution.lite",
	// "balance", "position", "positionAll", "lw", "leverage".
	Channel string
	// TradeType is SPOT, FUTURES or, for private channels, UNIFIED / ISOLATED /
	// CROSS. Leave it empty for channels that do not take one.
	TradeType string
	// Symbols are the symbols to receive. One symbol is sent as "symbol", several
	// as "symbols". Leave it empty for channels that are not symbol-scoped.
	Symbols []string
	// Interval is the candle interval of the kline channel ("1min", ...).
	Interval string
	// Depth is the order-book depth of the obu channel: "1", "5", "50",
	// "increment" or "increment@10ms".
	Depth string
	// RPIFilter is the obu rpiFilter (0 = only non-RPI orders, 1 = also RPI
	// orders, futures only).
	RPIFilter int
	// AccountType selects the balance channel's account ("UNIFIED", "FUNDING",
	// "ISOLATED").
	AccountType string
	// Extra holds additional request fields for channels this SDK does not know.
	Extra map[string]any
}

// fixedPushTypes lists the channels whose pushes do not carry the usual
// "channel.TRADETYPE" type.
var fixedPushTypes = map[string][]string{
	"mark-price":              {"mark-price"},
	"funding-fee":             {"funding-fee"},
	"funding-fee-all-symbols": {"funding-fee-all-symbols"},
	"callauctioninfo":         {"callAuctionInfo.SPOT"},
}

// PushTypes returns the values the T field of this channel's pushes can take.
// Most channels push "<channel>.<TRADETYPE>" ("ticker.FUTURES"); a few use a
// fixed name, and the liquidation-warning channel is documented under two
// spellings, so both are listed.
func (s SubscribeSpec) PushTypes() []string {
	key := strings.ToLower(s.Channel)
	if fixed, ok := fixedPushTypes[key]; ok {
		return fixed
	}
	suffix := s.TradeType
	if suffix == "" {
		suffix = s.AccountType
	}
	if suffix == "" {
		return []string{s.Channel}
	}
	types := []string{s.Channel + "." + suffix}
	if key == "lw" {
		types = append(types, "risk."+suffix)
	}
	return types
}

// Name returns the subscription's identity: two specs with equal names are the
// same subscription.
func (s SubscribeSpec) Name() string {
	symbols := append([]string(nil), s.Symbols...)
	sort.Strings(symbols)
	parts := []string{s.Channel, s.TradeType, strings.Join(symbols, ","), s.Interval, s.Depth, strconv.Itoa(s.RPIFilter), s.AccountType}
	if len(s.Extra) > 0 {
		b, _ := json.Marshal(s.Extra) // map keys marshal in sorted order
		parts = append(parts, string(b))
	}
	return strings.Join(parts, "|")
}

// routes returns the routing keys this subscription receives: one per push type
// and symbol, or a wildcard key when the channel is not symbol-scoped. A kline
// subscription is also scoped by its interval and an order book by its depth, so
// two subscriptions of the same symbol with different intervals or depths each
// receive only their own data.
func (s SubscribeSpec) routes() []string {
	depth := ""
	if strings.EqualFold(s.Channel, "obu") {
		depth = s.Depth
	}
	interval := ""
	if strings.EqualFold(s.Channel, "kline") {
		interval = s.Interval
	}
	symbols := s.Symbols
	if len(symbols) == 0 {
		symbols = []string{"*"}
	}
	var routes []string
	for _, t := range s.PushTypes() {
		for _, sym := range symbols {
			routes = append(routes, routeKey(t, depth, sym, interval))
		}
	}
	return routes
}

// frame builds the subscribe or unsubscribe request carrying id.
func (s SubscribeSpec) frame(action, id string) []byte {
	m := map[string]any{"id": id, "action": action, "channel": s.Channel}
	for k, v := range s.Extra {
		m[k] = v
	}
	if s.TradeType != "" {
		m["tradeType"] = s.TradeType
	}
	switch len(s.Symbols) {
	case 0:
	case 1:
		m["symbol"] = s.Symbols[0]
	default:
		m["symbols"] = s.Symbols
	}
	if s.Interval != "" {
		m["interval"] = s.Interval
	}
	if s.Depth != "" {
		m["depth"] = s.Depth
	}
	if s.RPIFilter != 0 {
		m["rpiFilter"] = s.RPIFilter
	}
	if s.AccountType != "" {
		m["accountType"] = s.AccountType
	}
	b, _ := json.Marshal(m)
	return b
}
