package streaming

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/websocket/uta"
)

// frameParts splits a push frame into its push type and its payload.
func frameParts(t *testing.T, frame string) (pushType string, d json.RawMessage) {
	t.Helper()
	var env map[string]json.RawMessage // a map matches keys exactly: "t" and "T" stay apart
	if err := json.Unmarshal([]byte(frame), &env); err != nil {
		t.Fatalf("frame is not valid JSON: %v\n%s", err, frame)
	}
	if err := json.Unmarshal(env["T"], &pushType); err != nil || len(env["d"]) == 0 {
		t.Fatalf("frame has no T or d: %s", frame)
	}
	return pushType, env["d"]
}

// jsonTags returns the exact JSON key names a struct type declares.
func jsonTags(typ reflect.Type) map[string]bool {
	tags := map[string]bool{}
	for i := 0; i < typ.NumField(); i++ {
		name, _, _ := strings.Cut(typ.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			tags[name] = true
		}
	}
	return tags
}

// payloadKeys returns the keys of a payload object, or of every element of a
// payload array.
func payloadKeys(t *testing.T, d json.RawMessage) []string {
	t.Helper()
	var objects []map[string]json.RawMessage
	var one map[string]json.RawMessage
	if err := json.Unmarshal(d, &one); err == nil {
		objects = []map[string]json.RawMessage{one}
	} else if err := json.Unmarshal(d, &objects); err != nil {
		t.Fatalf("payload is neither an object nor an array of objects: %s", d)
	}
	seen := map[string]bool{}
	for _, o := range objects {
		for k := range o {
			seen[k] = true
		}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func TestFixturesAreValidJSON(t *testing.T) {
	for _, f := range payloadFixtures {
		if !json.Valid([]byte(f.frame)) {
			t.Errorf("%s: invalid JSON: %s", f.name, f.frame)
		}
	}
}

// Go's encoding/json matches a key with no exact counterpart case-insensitively.
// UTA payloads use s and S (and a/A, b/B, o/O, c/C, e/E, os/oS, ls/lS) for different
// things, so a key silently overwrites its twin unless the struct declares both
// spellings. This test proves that every key of every official example binds to a
// field with exactly that spelling, and that every field is covered by an example.
func TestEveryDocumentedKeyHasAnExactCaseField(t *testing.T) {
	covered := map[reflect.Type]map[string]bool{}
	for _, f := range payloadFixtures {
		_, d := frameParts(t, f.frame)
		target := f.target()
		if err := json.Unmarshal(d, target); err != nil {
			t.Errorf("%s: %v", f.name, err)
			continue
		}
		typ := reflect.TypeOf(target).Elem()
		if typ.Kind() == reflect.Slice {
			typ = typ.Elem()
		}
		tags := jsonTags(typ)
		if covered[typ] == nil {
			covered[typ] = map[string]bool{}
		}
		for _, key := range payloadKeys(t, d) {
			if !tags[key] {
				t.Errorf("%s: key %q has no field of %s with exactly that spelling", f.name, key, typ)
			}
			covered[typ][key] = true
		}
	}
	for typ, seen := range covered {
		for tag := range jsonTags(typ) {
			if !seen[tag] {
				t.Errorf("no fixture carries the key %q of %s", tag, typ)
			}
		}
	}
}

// orderedObject is a JSON object whose keys can be put in any order.
type orderedObject []struct {
	key   string
	value json.RawMessage
}

func parseOrdered(t *testing.T, d json.RawMessage) (orderedObject, bool) {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(d))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, false
	}
	var out orderedObject
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, struct {
			key   string
			value json.RawMessage
		}{key.(string), v})
	}
	return out, true
}

func (o orderedObject) encode() json.RawMessage {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, kv := range o {
		if i > 0 {
			b.WriteByte(',')
		}
		k, _ := json.Marshal(kv.key)
		b.Write(k)
		b.WriteByte(':')
		b.Write(kv.value)
	}
	b.WriteByte('}')
	return b.Bytes()
}

// Whatever order the keys arrive in, the same fields come out.
func TestKeyOrderDoesNotChangeTheDecodedPayload(t *testing.T) {
	orderings := map[string]func(o orderedObject){
		"reversed": func(o orderedObject) {
			for i, j := 0, len(o)-1; i < j; i, j = i+1, j-1 {
				o[i], o[j] = o[j], o[i]
			}
		},
		"upper case first": func(o orderedObject) { sort.SliceStable(o, func(i, j int) bool { return o[i].key < o[j].key }) },
		"lower case first": func(o orderedObject) { sort.SliceStable(o, func(i, j int) bool { return o[i].key > o[j].key }) },
	}
	for _, f := range payloadFixtures {
		_, d := frameParts(t, f.frame)
		base := f.target()
		if err := json.Unmarshal(d, base); err != nil {
			t.Fatalf("%s: %v", f.name, err)
		}
		obj, ok := parseOrdered(t, d)
		if !ok {
			continue // an array payload: no key pairs to confuse
		}
		for name, order := range orderings {
			shuffled := append(orderedObject(nil), obj...)
			order(shuffled)
			got := f.target()
			if err := json.Unmarshal(shuffled.encode(), got); err != nil {
				t.Fatalf("%s (%s): %v", f.name, name, err)
			}
			if !reflect.DeepEqual(got, base) {
				t.Errorf("%s: keys %s decode differently:\n got  %+v\n want %+v", f.name, name, got, base)
			}
		}
	}
}

// The twin keys of each payload, spelled out with distinct values, in both orders.
func TestBothCaseKeysBindToTheirOwnFields(t *testing.T) {
	push := func(channel, d string) *uta.Push {
		return &uta.Push{T: channel, P: 1, Data: json.RawMessage(d)}
	}
	for _, order := range []string{"lower first", "upper first"} {
		pairs := func(lower, upper string) string {
			if order == "lower first" {
				return lower + "," + upper
			}
			return upper + "," + lower
		}
		t.Run(order, func(t *testing.T) {
			tk, _, err := decodeTicker(push("ticker.FUTURES", "{"+pairs(`"s":"SYM","b":"1","a":"3"`, `"S":"sell","B":"2","A":"4"`)+"}"))
			if err != nil || tk.Symbol != "SYM" || tk.Side != SideSell || tk.BestBidPrice != "1" || tk.BestBidSize != "2" || tk.BestAskPrice != "3" || tk.BestAskSize != "4" {
				t.Errorf("ticker: %+v %v", tk, err)
			}
			tr, _, err := decodeTrade(push("trade.SPOT", "{"+pairs(`"s":"SYM"`, `"S":"buy"`)+"}"))
			if err != nil || tr.Symbol != "SYM" || tr.Side != SideBuy {
				t.Errorf("trade: %+v %v", tr, err)
			}
			kl, _, err := decodeKline(push("kline.SPOT", "{"+pairs(`"s":"SYM","o":"1","c":"2"`, `"S":true,"O":10,"C":20`)+"}"))
			if err != nil || kl.Symbol != "SYM" || !kl.First || kl.Open != "1" || kl.Close != "2" || kl.StartTimestamp != 10 || kl.EndTimestamp != 20 {
				t.Errorf("kline: %+v %v", kl, err)
			}
			bal, _, err := decodeBalance(push("balance.UNIFIED", "{"+pairs(`"e":"5"`, `"E":6`)+"}"))
			if err != nil || bal.Equity != "5" || bal.Sequence != 6 {
				t.Errorf("balance: %+v %v", bal, err)
			}
			ord, _, err := decodeOrder(push("orderAll.UNIFIED", "{"+pairs(`"s":"SYM","os":4,"ls":"7"`, `"S":"BUY","oS":"USER","lS":"8"`)+"}"))
			if err != nil || ord.Symbol != "SYM" || ord.Side != SideBuy || ord.Status != OrderStatusPartiallyFilled || ord.Source != OrderSourceUser ||
				ord.LastFilledPrice != "7" || ord.LastFilledQuantity != "8" {
				t.Errorf("order: %+v %v", ord, err)
			}
			ex, _, err := decodeExecution(push("execution.UNIFIED", "{"+pairs(`"s":"SYM"`, `"S":"sell"`)+"}"))
			if err != nil || ex.Symbol != "SYM" || ex.Side != SideSell {
				t.Errorf("execution: %+v %v", ex, err)
			}
			lite, _, err := decodeExecutionLite(push("execution.lite.UNIFIED", "{"+pairs(`"s":"SYM"`, `"S":"BUY"`)+"}"))
			if err != nil || lite.Symbol != "SYM" || lite.Side != SideBuy {
				t.Errorf("execution lite: %+v %v", lite, err)
			}
		})
	}
}

func TestDecodersRejectMalformedPayloads(t *testing.T) {
	data := func(d string) *uta.Push { return &uta.Push{T: "x.SPOT", P: 1, Data: json.RawMessage(d)} }
	// A payload that is not an object (or, for the funding channel, not an array of
	// objects) is reported by every decoder.
	objectDecoders := map[string]func(*uta.Push) error{
		"ticker":         func(p *uta.Push) error { _, _, err := decodeTicker(p); return err },
		"trade":          func(p *uta.Push) error { _, _, err := decodeTrade(p); return err },
		"kline":          func(p *uta.Push) error { _, _, err := decodeKline(p); return err },
		"order book":     func(p *uta.Push) error { _, _, err := decodeOrderBookUpdate(p); return err },
		"book update":    func(p *uta.Push) error { _, err := decodeBookUpdate(p); return err },
		"mark price":     func(p *uta.Push) error { _, _, err := decodeMarkPrice(p); return err },
		"funding rate":   func(p *uta.Push) error { _, _, err := decodeFundingRate(p); return err },
		"call auction":   func(p *uta.Push) error { _, _, err := decodeCallAuction(p); return err },
		"order":          func(p *uta.Push) error { _, _, err := decodeOrder(p); return err },
		"execution":      func(p *uta.Push) error { _, _, err := decodeExecution(p); return err },
		"execution lite": func(p *uta.Push) error { _, _, err := decodeExecutionLite(p); return err },
		"balance":        func(p *uta.Push) error { _, _, err := decodeBalance(p); return err },
		"position":       func(p *uta.Push) error { _, _, err := decodePosition(p); return err },
		"liquidation":    func(p *uta.Push) error { _, _, err := decodeLiquidationWarning(p); return err },
		"leverage":       func(p *uta.Push) error { _, _, err := decodeLeverage(p); return err },
	}
	for name, decode := range objectDecoders {
		for _, payload := range []string{``, `null`, `"text"`, `[1]`, `7`} {
			if decode(data(payload)) == nil {
				t.Errorf("%s accepted the payload %q", name, payload)
			}
		}
	}
	for _, payload := range []string{``, `null`, `"text"`, `{}`, `[1]`, `[{"fr":[]}]`} {
		if _, _, err := decodeAllFundingRates(data(payload)); err == nil {
			t.Errorf("the all-funding decoder accepted %q", payload)
		}
	}
	// Valid JSON of the wrong shape for one field.
	for name, check := range map[string]func() error{
		"ticker sequence":   func() error { _, _, err := decodeTicker(data(`{"E":"x"}`)); return err },
		"ticker price":      func() error { _, _, err := decodeTicker(data(`{"b":{}}`)); return err },
		"ticker side":       func() error { _, _, err := decodeTicker(data(`{"S":5}`)); return err },
		"trade flag":        func() error { _, _, err := decodeTrade(data(`{"rpi":"maybe"}`)); return err },
		"order book level":  func() error { _, _, err := decodeOrderBookUpdate(data(`{"b":[["1"]]}`)); return err },
		"order book levels": func() error { _, _, err := decodeOrderBookUpdate(data(`{"a":{}}`)); return err },
		"mark price stamp":  func() error { _, _, err := decodeMarkPrice(data(`{"ts":[]}`)); return err },
		"call auction":      func() error { _, _, err := decodeCallAuction(data(`{"ts":[]}`)); return err },
		"position":          func() error { _, _, err := decodePosition(data(`{"U":"x"}`)); return err },
		"balance":           func() error { _, _, err := decodeBalance(data(`{"cS":{}}`)); return err },
		"order status":      func() error { _, _, err := decodeOrder(data(`{"os":{}}`)); return err },
		"order flag":        func() error { _, _, err := decodeOrder(data(`{"rO":"yes"}`)); return err },
	} {
		if check() == nil {
			t.Errorf("%s: a malformed value must be reported", name)
		}
	}
}

func TestDecodeBookUpdateRejectsWhatABookCannotUse(t *testing.T) {
	push := func(kind, d string) *uta.Push {
		return &uta.Push{T: "obu.FUTURES", Depth: "increment@10ms", Kind: kind, Data: json.RawMessage(d)}
	}
	if _, err := decodeBookUpdate(push("snapshot", `{"s":"A","O":5,"C":5,"b":[],"a":[]}`)); err != nil {
		t.Fatal(err)
	}
	if u, err := decodeBookUpdate(push("DELTA", `{"s":"A","O":5,"C":7,"b":[],"a":[]}`)); err != nil || u.Kind != UpdateDelta {
		t.Fatalf("the kind is case-insensitive: %+v %v", u, err)
	}
	for name, p := range map[string]*uta.Push{
		"no kind":       push("", `{"s":"A","O":1,"C":1}`),
		"unknown kind":  push("patch", `{"s":"A","O":1,"C":1}`),
		"backwards":     push("delta", `{"s":"A","O":9,"C":3}`),
		"not an object": push("delta", `[1]`),
		"no data":       push("delta", ``),
	} {
		if _, err := decodeBookUpdate(p); err == nil {
			t.Errorf("%s: must be rejected", name)
		}
	}
}

func TestCallAuctionPrefersTheSchemasPriceKey(t *testing.T) {
	p := &uta.Push{T: "callAuctionInfo.SPOT", Kind: "snapshot", P: 3, Data: json.RawMessage(`{"s":"A","ep":"1","eq":"2"}`)}
	v, ok, err := decodeCallAuction(p)
	if err != nil || !ok || v.EstimatedPrice != "1" || v.GatewayTimestamp != 3 {
		t.Fatalf("ep and eq: %+v %v %v", v, ok, err)
	}
	p.Data = json.RawMessage(`{"s":"A","eq":"2"}`)
	if v, _, _ := decodeCallAuction(p); v.EstimatedPrice != "2" {
		t.Fatalf("eq alone: %+v", v)
	}
	p.Data = json.RawMessage(`{"s":"A"}`)
	if v, _, _ := decodeCallAuction(p); !v.EstimatedPrice.IsEmpty() {
		t.Fatalf("neither: %+v", v)
	}
}

func TestDecodeObjectWithoutAFillFunction(t *testing.T) {
	v, ok, err := decodeObject[Ticker](nil)(&uta.Push{Data: json.RawMessage(`{"s":"A"}`)})
	if err != nil || !ok || v.Symbol != "A" {
		t.Fatalf("%+v %v %v", v, ok, err)
	}
}

func TestSideIsAlwaysUpperCase(t *testing.T) {
	for in, want := range map[string]Side{`"buy"`: SideBuy, `"BUY"`: SideBuy, `"Sell"`: SideSell, `"sell"`: SideSell, `""`: "", `"odd"`: "ODD"} {
		var s Side
		if err := json.Unmarshal([]byte(in), &s); err != nil || s != want {
			t.Errorf("%s -> %q, %v (want %q)", in, s, err, want)
		}
	}
	s := SideBuy
	if err := json.Unmarshal([]byte(`null`), &s); err != nil || s != SideBuy {
		t.Errorf("null must leave the side alone: %q %v", s, err)
	}
	if err := json.Unmarshal([]byte(`5`), &s); err == nil {
		t.Error("a number is not a side")
	}
}

func TestIntervalAndDepthValidity(t *testing.T) {
	all := []Interval{Interval1Min, Interval3Min, Interval5Min, Interval15Min, Interval30Min, Interval1Hour, Interval2Hour, Interval4Hour,
		Interval6Hour, Interval8Hour, Interval12Hour, Interval1Day, Interval1Week, Interval1Month}
	for _, i := range all {
		if !i.Valid() || !i.ValidFor("SPOT") {
			t.Errorf("%q must be valid on spot", i)
		}
		if want := i != Interval6Hour; i.ValidFor("FUTURES") != want || i.ValidFor("futures") != want {
			t.Errorf("%q on futures: want %v", i, want)
		}
	}
	for _, i := range []Interval{"", "2min", "1Min", "6 hour"} {
		if i.Valid() || i.ValidFor("SPOT") {
			t.Errorf("%q must be invalid", i)
		}
	}
	for _, d := range []Depth{Depth1, Depth5, Depth50, DepthIncrement, DepthIncrement10ms} {
		if !d.Valid() {
			t.Errorf("%q must be valid", d)
		}
		if want := d == Depth1 || d == Depth5 || d == Depth50; d.IsSnapshot() != want {
			t.Errorf("%q IsSnapshot = %v", d, d.IsSnapshot())
		}
	}
	for _, d := range []Depth{"", "10", "20", "full", "increment@100ms"} {
		if d.Valid() {
			t.Errorf("%q must be invalid", d)
		}
	}
}

func TestTimeConversions(t *testing.T) {
	if !nanos(0).IsZero() || !millis(0).IsZero() || !secs(0).IsZero() || !unixAuto(0).IsZero() {
		t.Error("a zero timestamp is the zero time")
	}
	if nanos(1_500_000_000).UnixNano() != 1_500_000_000 || millis(1500).UnixMilli() != 1500 || secs(7).Unix() != 7 {
		t.Error("conversions")
	}
	for _, tc := range []struct {
		in   int64
		want time.Time
	}{
		{1_780_543_242_933_123_456, time.Unix(0, 1_780_543_242_933_123_456)}, // nanoseconds
		{1_780_543_242_933_123, time.UnixMicro(1_780_543_242_933_123)},       // microseconds
		{1_780_543_242_933, time.UnixMilli(1_780_543_242_933)},               // milliseconds
		{1_780_543_242, time.Unix(1_780_543_242, 0)},                         // seconds
		{-1_780_543_242_933_123_456, time.Unix(0, -1_780_543_242_933_123_456)},
		{-1_780_543_242_933, time.UnixMilli(-1_780_543_242_933)},
		{-1_780_543_242_933_123, time.UnixMicro(-1_780_543_242_933_123)},
	} {
		if got := unixAuto(tc.in); !got.Equal(tc.want) {
			t.Errorf("unixAuto(%d) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestOrderBookUpdateConversionsOfEmptyUpdates(t *testing.T) {
	u := OrderBookUpdate{Symbol: "X", StartSequence: 4, EndSequence: 6}
	if d := u.ToDelta(); d.Symbol != "X" || d.Start != 4 || d.End != 6 || len(d.Changes) != 0 {
		t.Fatalf("delta: %+v", d)
	}
	if s := u.ToSnapshot(); s.Symbol != "X" || s.Sequence != 6 || len(s.Bids) != 0 || len(s.Asks) != 0 {
		t.Fatalf("snapshot: %+v", s)
	}
	// A delta converted from a push applies to a book exactly like the push says.
	book := orderbook.New("X")
	if err := book.Reset(orderbook.Snapshot{Symbol: "X", Sequence: 3, Bids: []orderbook.Level{lv("10", "1")}, Asks: []orderbook.Level{lv("11", "1")}}); err != nil {
		t.Fatal(err)
	}
	u.Bids = []orderbook.Level{lv("10", "0"), lv("9", "5")}
	u.Asks = []orderbook.Level{lv("11", "2")}
	if applied, err := book.Apply(u.ToDelta()); err != nil || !applied {
		t.Fatalf("apply: %v %v", applied, err)
	}
	if bid, _ := book.BestBid(); bid.Price != "9" || bid.Size != "5" || book.Sequence() != 6 {
		t.Fatalf("book after the delta: bid %v seq %d", bid, book.Sequence())
	}
	if ask, _ := book.BestAsk(); ask.Size != "2" {
		t.Fatalf("ask: %v", ask)
	}
}

func TestEnumStringsAreTheDocumentedWireValues(t *testing.T) {
	for _, pair := range [][2]string{
		{string(TradeTypeSpot), "SPOT"}, {string(TradeTypeFutures), "FUTURES"}, {string(TradeTypeMargin), "MARGIN"}, {string(TradeTypeUnified), "UNIFIED"},
		{string(MarginModeIsolated), "ISOLATED"}, {string(MarginModeCross), "CROSS"},
		{string(PositionSideBoth), "BOTH"}, {string(PositionSideLong), "LONG"}, {string(PositionSideShort), "SHORT"},
		{string(OrderTypeLimit), "LIMIT"}, {string(OrderTypeMarket), "MARKET"},
		{string(LiquidityMaker), "MAKER"}, {string(LiquidityTaker), "TAKER"},
		{string(OrderEventOpen), "OPEN"}, {string(OrderEventUpdate), "UPDATE"}, {string(OrderEventFill), "FILL"},
		{string(OrderEventCancel), "CANCEL"}, {string(OrderEventTrigger), "TRIGGER"}, {string(OrderEventMatch), "MATCH"},
		{string(QuantityUnitBase), "BASECCY"}, {string(QuantityUnitQuote), "QUOTECCY"}, {string(QuantityUnitUnit), "UNIT"},
		{string(TriggerDown), "DOWN"}, {string(TriggerUp), "UP"},
		{string(TriggerPriceTrade), "TP"}, {string(TriggerPriceIndex), "IP"}, {string(TriggerPriceMark), "MP"},
		{string(TimeInForceGTC), "GTC"}, {string(TimeInForceIOC), "IOC"}, {string(TimeInForceFOK), "FOK"}, {string(TimeInForceGTT), "GTT"}, {string(TimeInForceRPI), "RPI"},
		{string(STPDecreaseAndCancel), "DC"}, {string(STPCancelOld), "CO"}, {string(STPCancelNew), "CN"}, {string(STPCancelBoth), "CB"},
		{string(OrderSourceUser), "USER"},
		{string(FillTypeNormal), "NORMAL"}, {string(FillTypeLiquid), "LIQUID"}, {string(FillTypeSettlement), "SETTLEMENT"}, {string(FillTypeADL), "ADL"},
		{string(RiskEventMarginCall), "MARGIN_CALL"}, {string(RiskEventReduceOnly), "REDUCE_ONLY"},
		{string(RiskEventLiquidationWarning), "LIQUIDATION_WARNING"}, {string(RiskEventForceLiquidation), "FORCE_LIQUIDATION"},
		{string(UpdateSnapshot), "snapshot"}, {string(UpdateDelta), "delta"},
		{string(DepthIncrement10ms), "increment@10ms"}, {string(DepthIncrement), "increment"},
		{string(AccountTypeUnified), "UNIFIED"}, {string(AccountTypeFunding), "FUNDING"}, {string(AccountTypeIsolated), "ISOLATED"},
	} {
		got, want := pair[0], pair[1]
		if got != want {
			t.Errorf("constant is %q, the wire value is %q", got, want)
		}
	}
	// The cancel reasons are the strings of KuCoin's table; spot-check the unusual spellings.
	for got, want := range map[CancelReason]string{
		CancelReasonIceFrogFrozen: "iceFrogFrozen", CancelReasonPostOnly: "POST_ONLY", CancelReasonSystemCancel: "SYSTEM_CANCEL",
		CancelReasonNoMarginSizeLargerThanPos: "NO_MARGIN_SIZE_LARGER_THAN_POSITION", CancelReasonExceededAccountLiability: "EXCEEDED_ACCOUNT_LIABILITY_LIMIT",
	} {
		if string(got) != want {
			t.Errorf("cancel reason %q, want %q", got, want)
		}
	}
	if !strings.Contains(fmt.Sprint(CancelReasonUser), "USER") {
		t.Error("cancel reason")
	}
}
