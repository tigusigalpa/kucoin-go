package streaming

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	classicws "github.com/tigusigalpa/kucoin-go/classic/ws"
	"github.com/tigusigalpa/kucoin-go/orderbook"
	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/transport"
	"github.com/tigusigalpa/kucoin-go/websocket/classic"
)

// msg builds the envelope a decoder receives.
func msg(topic, subject, data string) *classic.Message {
	m := &classic.Message{Type: "message", Topic: topic, Subject: subject}
	if data != "" {
		m.Data = json.RawMessage(data)
	}
	return m
}

func TestSymbolFromTopic(t *testing.T) {
	for topic, want := range map[string]string{
		"/market/ticker:BTC-USDT":              "BTC-USDT",
		"/market/ticker:all":                   "all",
		"/market/candles:BTC-USDT_1min":        "BTC-USDT_1min",
		"/spotMarket/tradeOrdersV2":            "",
		"":                                     "",
		"/callauction/callauctionData:ETH-BTC": "ETH-BTC",
	} {
		eq(t, "symbolFromTopic("+topic+")", symbolFromTopic(topic), want)
	}
}

func TestValidName(t *testing.T) {
	for _, ok := range []string{"BTC-USDT", "BTC", "USDS", "1INCH-USDT", "ETH3L-USDT", "DeFi", "Layer1", "USDT-BTC"} {
		if !validName(ok) {
			t.Errorf("%q must be a valid name", ok)
		}
	}
	for _, bad := range []string{"", ",", "A,B", "A:B", "A_B", "A/B", " ", "A B", "A\tB", "A\nB", "A B", "BTC-USDT,", "_1min"} {
		if validName(bad) {
			t.Errorf("%q must not be a valid name", bad)
		}
	}
}

func TestEveryDecoderRejectsAPushWithoutData(t *testing.T) {
	empty := msg("/market/ticker:BTC-USDT", "x", "")
	decoders := map[string]func(*classic.Message) error{
		"ticker":          func(m *classic.Message) error { _, _, err := decodeTicker(m); return err },
		"all tickers":     func(m *classic.Message) error { _, _, err := decodeAllTicker(m); return err },
		"depth":           func(m *classic.Message) error { _, _, err := decodeDepth(m); return err },
		"trade":           func(m *classic.Message) error { _, _, err := decodeTrade(m); return err },
		"order":           func(m *classic.Message) error { _, _, err := decodeOrderUpdate(m); return err },
		"stop order":      func(m *classic.Message) error { _, _, err := decodeStopOrder(m); return err },
		"balance":         func(m *classic.Message) error { _, _, err := decodeBalance(m); return err },
		"symbol snapshot": func(m *classic.Message) error { _, _, err := decodeSymbolSnapshot(m); return err },
		"market snapshot": func(m *classic.Message) error { _, _, err := decodeMarketSnapshot(m); return err },
		"level 1":         func(m *classic.Message) error { _, _, err := decodeLevel1(m); return err },
		"order changes":   func(m *classic.Message) error { _, _, err := decodeOrderBookChange(m); return err },
		"kline":           func(m *classic.Message) error { _, _, err := decodeKline(m); return err },
		"call auction":    func(m *classic.Message) error { _, _, err := decodeCallAuctionData(m); return err },
	}
	for name, decode := range decoders {
		if err := decode(empty); err == nil {
			t.Errorf("%s: a push without data must be reported", name)
		}
	}
}

func TestEveryDecoderRejectsMalformedJSON(t *testing.T) {
	broken := msg("/market/ticker:BTC-USDT", "x", `{"nope"`)
	decoders := map[string]func(*classic.Message) error{
		"ticker":          func(m *classic.Message) error { _, _, err := decodeTicker(m); return err },
		"symbol snapshot": func(m *classic.Message) error { _, _, err := decodeSymbolSnapshot(m); return err },
		"level 1":         func(m *classic.Message) error { _, _, err := decodeLevel1(m); return err },
		"order changes":   func(m *classic.Message) error { _, _, err := decodeOrderBookChange(m); return err },
		"kline":           func(m *classic.Message) error { _, _, err := decodeKline(m); return err },
		"call auction":    func(m *classic.Message) error { _, _, err := decodeCallAuctionData(m); return err },
	}
	for name, decode := range decoders {
		if err := decode(broken); err == nil {
			t.Errorf("%s: malformed JSON must be reported", name)
		}
	}
}

func TestTickerTimeKeyCaseDoesNotMatter(t *testing.T) {
	// The documentation spells the key both ways; encoding/json falls back to a
	// case-insensitive match, so one field serves every spelling and a payload
	// carrying two of them is not an error.
	for _, key := range []string{"time", "Time", "TIME"} {
		v, ok, err := decodeTicker(msg("/market/ticker:BTC-USDT", "trade.ticker", fmt.Sprintf(`{"sequence":"1",%q:123456}`, key)))
		if err != nil || !ok || v.Timestamp != 123456 {
			t.Errorf("key %q: %+v ok=%v err=%v", key, v, ok, err)
		}
	}
	v, _, err := decodeTicker(msg("/market/ticker:BTC-USDT", "trade.ticker", `{"time":1,"Time":2}`))
	if err != nil || v.Timestamp != 2 {
		t.Errorf("both spellings: %+v err=%v", v, err)
	}
}

func TestDecodeBest(t *testing.T) {
	for name, tc := range map[string]struct {
		raw       string
		want      orderbook.Level
		wantError bool
	}{
		"absent":         {raw: ``},
		"null":           {raw: `null`},
		"empty array":    {raw: `[]`},
		"pair":           {raw: `["1.5","2"]`, want: lvl("1.5", "2")},
		"numbers":        {raw: `[1.50,2]`, want: lvl("1.50", "2")},
		"one element":    {raw: `["1"]`, wantError: true},
		"not an array":   {raw: `"x"`, wantError: true},
		"object":         {raw: `{}`, wantError: true},
		"bad element":    {raw: `[true,"1"]`, wantError: true},
		"extra element":  {raw: `["1","2","3"]`, want: orderbook.Level{Price: "1", Size: "2", RPISize: "3"}},
		"nested garbage": {raw: `[[1],[2]]`, wantError: true},
	} {
		got, err := decodeBest(json.RawMessage(tc.raw))
		if (err != nil) != tc.wantError || got != tc.want {
			t.Errorf("%s: got %+v err=%v, want %+v error=%v", name, got, err, tc.want, tc.wantError)
		}
	}
}

func TestLevelChangeUnmarshal(t *testing.T) {
	var ok LevelChange
	if err := json.Unmarshal([]byte(`["1.50","0","123"]`), &ok); err != nil || ok.Price != "1.50" || ok.Size != "0" || ok.Sequence != 123 {
		t.Fatalf("full row: %+v err=%v", ok, err)
	}
	var bare LevelChange
	if err := json.Unmarshal([]byte(`[1.5,2]`), &bare); err != nil || bare.Price != "1.5" || bare.Size != "2" || bare.Sequence != 0 {
		t.Fatalf("row without sequence: %+v err=%v", bare, err)
	}
	for name, raw := range map[string]string{
		"not an array":        `"x"`,
		"empty":               `[]`,
		"one element":         `["1"]`,
		"price is a boolean":  `[true,"1","5"]`,
		"size is an object":   `["1",{},"5"]`,
		"sequence not number": `["1","1","x"]`,
		"price not a number":  `["abc","1","5"]`,
		"size not a number":   `["1","abc","5"]`,
		"empty price":         `["","1","5"]`,
		"null size":           `["1",null,"5"]`,
	} {
		var c LevelChange
		if err := json.Unmarshal([]byte(raw), &c); err == nil {
			t.Errorf("%s: %s must be rejected, got %+v", name, raw, c)
		}
	}
}

func TestDecodeOrderBookChange(t *testing.T) {
	good, ok, err := decodeOrderBookChange(msg("/market/level2:ETH-USDT", "trade.l2update",
		`{"changes":{"asks":[["2","3","9"]],"bids":[]},"sequenceStart":8,"sequenceEnd":9,"time":5}`))
	if err != nil || !ok {
		t.Fatalf("err=%v ok=%v", err, ok)
	}
	eq(t, "the symbol falls back to the topic", good.Symbol, "ETH-USDT")
	eq(t, "SequenceStart", good.SequenceStart, 8)
	eq(t, "SequenceEnd", good.SequenceEnd, 9)
	eq(t, "len(Asks)", len(good.Asks), 1)
	for name, data := range map[string]string{
		"start after end":          `{"changes":{},"sequenceStart":9,"sequenceEnd":8,"symbol":"X"}`,
		"zero start":               `{"changes":{},"sequenceStart":0,"sequenceEnd":8,"symbol":"X"}`,
		"negative start":           `{"changes":{},"sequenceStart":-1,"sequenceEnd":8,"symbol":"X"}`,
		"no sequences":             `{"changes":{},"symbol":"X"}`,
		"sequence not a number":    `{"changes":{},"sequenceStart":"x","sequenceEnd":8,"symbol":"X"}`,
		"changes is not an object": `{"changes":[],"sequenceStart":1,"sequenceEnd":1,"symbol":"X"}`,
		"row is malformed":         `{"changes":{"bids":[["1"]]},"sequenceStart":1,"sequenceEnd":1,"symbol":"X"}`,
	} {
		if _, _, err := decodeOrderBookChange(msg("/market/level2:X", "trade.l2update", data)); err == nil {
			t.Errorf("%s: must be rejected", name)
		}
	}
	// A push may omit "changes" or one side; the sequence range alone is valid.
	if v, _, err := decodeOrderBookChange(msg("/market/level2:X", "trade.l2update", `{"sequenceStart":1,"sequenceEnd":1,"symbol":"X","time":1}`)); err != nil || len(v.Asks)+len(v.Bids) != 0 {
		t.Errorf("a push without rows: %+v err=%v", v, err)
	}
}

func TestOrderBookChangeDelta(t *testing.T) {
	c := OrderBookChange{
		Symbol: "BTC-USDT", SequenceStart: 4, SequenceEnd: 6,
		Asks: []LevelChange{{Price: "10", Size: "1"}, {Price: "0", Size: "5"}, {Price: "0.00", Size: "0"}},
		Bids: []LevelChange{{Price: "9", Size: "0"}, {Price: "0.0", Size: "2"}, {Price: "8.5", Size: "3"}},
	}
	d := c.delta()
	if d.Symbol != "BTC-USDT" || d.Start != 4 || d.End != 6 {
		t.Fatalf("delta header: %+v", d)
	}
	want := []orderbook.Change{
		{Side: orderbook.Ask, Price: "10", Size: "1"},
		{Side: orderbook.Bid, Price: "9", Size: "0"},
		{Side: orderbook.Bid, Price: "8.5", Size: "3"},
	}
	if len(d.Changes) != len(want) {
		t.Fatalf("changes = %+v, want %+v (zero-price rows are not part of the book)", d.Changes, want)
	}
	for i := range want {
		if d.Changes[i] != want[i] {
			t.Errorf("change %d = %+v, want %+v", i, d.Changes[i], want[i])
		}
	}
	if err := d.Validate(); err != nil {
		t.Fatalf("a delta built from decoded rows must validate: %v", err)
	}
	if empty := (OrderBookChange{SequenceStart: 1, SequenceEnd: 1}).delta(); len(empty.Changes) != 0 || empty.Start != 1 {
		t.Fatalf("empty delta: %+v", empty)
	}
}

func TestDecodeKline(t *testing.T) {
	good, ok, err := decodeKline(msg("/market/candles:ETH-BTC_12hour", "trade.candles.update",
		`{"candles":["1700000000","1","2","3","0.5","7","8"],"time":9}`))
	if err != nil || !ok {
		t.Fatalf("err=%v ok=%v", err, ok)
	}
	eq(t, "the symbol falls back to the topic", good.Symbol, "ETH-BTC")
	eq(t, "the interval comes from the topic", good.Interval, Interval12Hour)
	eq(t, "Close precedes High and Low", good.Close, "2")
	eq(t, "High", good.High, "3")
	eq(t, "Low", good.Low, "0.5")
	// A topic without the interval suffix leaves the interval empty.
	odd, _, err := decodeKline(msg("/market/candles:ETHBTC", "trade.candles.update", `{"symbol":"ETHBTC","candles":["1","2","3","4","5","6","7"],"time":9}`))
	if err != nil || odd.Interval != "" || odd.Symbol != "ETHBTC" {
		t.Fatalf("odd topic: %+v err=%v", odd, err)
	}
	for name, data := range map[string]string{
		"too few elements":    `{"candles":["1","2"],"time":9}`,
		"no candles":          `{"time":9}`,
		"start not a number":  `{"candles":["x","2","3","4","5","6","7"]}`,
		"start is fractional": `{"candles":["1.5","2","3","4","5","6","7"]}`,
		"candles not array":   `{"candles":"nope"}`,
	} {
		if _, _, err := decodeKline(msg("/market/candles:X_1min", "x", data)); err == nil {
			t.Errorf("%s: must be rejected", name)
		}
	}
}

func TestDecodeSnapshotSymbolSources(t *testing.T) {
	const noSymbol = `{"sequence":7,"data":{"baseCurrency":"BTC","datetime":1}}`
	bySymbolTopic, ok, err := decodeSymbolSnapshot(msg("/market/snapshot:BTC-USDT", "trade.snapshot", noSymbol))
	if err != nil || !ok || bySymbolTopic.Symbol != "BTC-USDT" || bySymbolTopic.Sequence != 7 {
		t.Fatalf("symbol snapshot: %+v ok=%v err=%v", bySymbolTopic, ok, err)
	}
	// The market channel's topic names a market, which is not a symbol.
	byMarketTopic, _, err := decodeMarketSnapshot(msg("/market/snapshot:BTC", "trade.snapshot", noSymbol))
	if err != nil || byMarketTopic.Symbol != "" {
		t.Fatalf("market snapshot: %+v err=%v", byMarketTopic, err)
	}
	for name, data := range map[string]string{
		"no snapshot object":   `{"sequence":7}`,
		"null snapshot object": `{"sequence":7,"data":null}`,
		"object is a string":   `{"sequence":7,"data":"x"}`,
		"bad outer sequence":   `{"sequence":"x","data":{}}`,
		"bad inner field":      `{"sequence":7,"data":{"board":"x"}}`,
	} {
		if _, _, err := decodeSymbolSnapshot(msg("/market/snapshot:X", "x", data)); err == nil {
			t.Errorf("%s: must be rejected", name)
		}
	}
}

func TestDecodeCallAuctionDataFallbacks(t *testing.T) {
	// No symbol in the payload: the topic names it.
	v, _, err := decodeCallAuctionData(msg("/callauction/callauctionData:BTC-USDT", "x", `{"estimatedPrice":"1","ts":5}`))
	if err != nil || v.Symbol != "BTC-USDT" || v.EstimatedPrice != "1" || v.Timestamp != 5 {
		t.Fatalf("%+v err=%v", v, err)
	}
	// Full names win over abbreviations when a payload carries both.
	v, _, err = decodeCallAuctionData(msg("/callauction/callauctionData:X", "x", `{"symbol":"A-B","s":"C-D","estimatedPrice":"1","ep":"2","time":3,"ts":4}`))
	if err != nil || v.Symbol != "A-B" || v.EstimatedPrice != "1" || v.Timestamp != 3 {
		t.Fatalf("%+v err=%v", v, err)
	}
	if _, _, err := decodeCallAuctionData(msg("/callauction/callauctionData:X", "x", `{"time":"x"}`)); err == nil {
		t.Fatal("a malformed timestamp must be rejected")
	}
}

func TestDecodeLevel1Errors(t *testing.T) {
	for name, data := range map[string]string{
		"asks not an array": `{"asks":1,"bids":[]}`,
		"bids not an array": `{"asks":[],"bids":{}}`,
		"bids one element":  `{"asks":[],"bids":["1"]}`,
		"bad timestamp":     `{"asks":[],"bids":[],"timestamp":"x"}`,
	} {
		if _, _, err := decodeLevel1(msg("/spotMarket/level1:X", "level1", data)); err == nil {
			t.Errorf("%s: must be rejected", name)
		}
	}
	v, ok, err := decodeLevel1(msg("/spotMarket/level1:X", "level1", `{"timestamp":3}`))
	if err != nil || !ok || v.Symbol != "X" || v.Ask.Price != "" || v.Bid.Price != "" {
		t.Fatalf("a push with neither side: %+v ok=%v err=%v", v, ok, err)
	}
}

func TestDecodeEnvelopeFields(t *testing.T) {
	m := msg("/account/balance", "account.balance", `{"currency":"USDT"}`)
	m.ID, m.UserID, m.ChannelType = "77", "user-1", "private"
	v, ok, err := decodeBalance(m)
	if err != nil || !ok || v.ID != "77" || v.UserID != "user-1" || v.ChannelType != "private" || v.Subject != "account.balance" || v.Currency != "USDT" {
		t.Fatalf("%+v ok=%v err=%v", v, ok, err)
	}
	o, _, err := decodeOrderUpdate(m)
	if err != nil || o.UserID != "user-1" || o.Subject != "account.balance" {
		t.Fatalf("%+v err=%v", o, err)
	}
	// A trade whose payload names no symbol takes it from the topic.
	tr, _, err := decodeTrade(msg("/market/match:ETH-USDT", "trade.l3match", `{"price":"1"}`))
	if err != nil || tr.Symbol != "ETH-USDT" {
		t.Fatalf("%+v err=%v", tr, err)
	}
}

func TestPermanentSnapshotClassifiesCredentialFailures(t *testing.T) {
	if permanentSnapshot(nil) != nil {
		t.Fatal("a missing snapshot function must stay missing so ErrNoSnapshotSource can be reported")
	}
	boom := errors.New("connection reset")
	good := orderbook.Snapshot{Symbol: "BTC-USDT", Sequence: 9}
	for name, tc := range map[string]struct {
		err           error
		wantPermanent bool
		wantText      string
	}{
		"success":              {err: nil},
		"missing credentials":  {err: fmt.Errorf("call: %w", transport.ErrCredentialsRequired), wantPermanent: true, wantText: "needs API credentials"},
		"rejected credentials": {err: fmt.Errorf("%w: %w", transport.ErrUnauthorized, &transport.KucoinError{HTTPStatus: 401}), wantPermanent: true, wantText: "rejected"},
		"network failure":      {err: boom},
		"service unavailable":  {err: transport.ErrServiceUnavailable},
		"rate limited":         {err: transport.ErrRateLimited},
		"forbidden or limited": {err: transport.ErrForbiddenOrLimited},
	} {
		wrapped := permanentSnapshot(func(_ context.Context, symbol string) (orderbook.Snapshot, error) {
			if symbol != "BTC-USDT" {
				t.Errorf("%s: symbol = %q", name, symbol)
			}
			return good, tc.err
		})
		snap, err := wrapped(context.Background(), "BTC-USDT")
		if snap.Sequence != 9 {
			t.Errorf("%s: the snapshot must be passed through", name)
		}
		if stream.IsPermanent(err) != tc.wantPermanent {
			t.Errorf("%s: permanent = %v, want %v (err=%v)", name, stream.IsPermanent(err), tc.wantPermanent, err)
		}
		if tc.err != nil && !errors.Is(err, tc.err) {
			t.Errorf("%s: the original error must stay matchable: %v", name, err)
		}
		if tc.wantText != "" && !strings.Contains(err.Error(), tc.wantText) {
			t.Errorf("%s: %q does not mention %q", name, err, tc.wantText)
		}
		if tc.err == nil && err != nil {
			t.Errorf("%s: unexpected error %v", name, err)
		}
	}
}

func TestPermanentOnAuthFailureClassifiesTokenFailures(t *testing.T) {
	tok := &classicws.Token{Token: "abc"}
	for name, tc := range map[string]struct {
		err           error
		wantPermanent bool
	}{
		"success":              {err: nil},
		"missing credentials":  {err: transport.ErrCredentialsRequired, wantPermanent: true},
		"rejected credentials": {err: fmt.Errorf("%w: x", transport.ErrUnauthorized), wantPermanent: true},
		"service unavailable":  {err: transport.ErrServiceUnavailable},
		"other":                {err: errors.New("boom")},
	} {
		got, err := permanentOnAuthFailure(func(context.Context) (*classicws.Token, error) {
			if tc.err != nil {
				return nil, tc.err
			}
			return tok, nil
		})(context.Background())
		if stream.IsPermanent(err) != tc.wantPermanent || (tc.err == nil) != (err == nil) || (tc.err == nil && got != tok) {
			t.Errorf("%s: token=%v err=%v permanent=%v", name, got, err, stream.IsPermanent(err))
		}
		if tc.err != nil && !errors.Is(err, tc.err) {
			t.Errorf("%s: the original error must stay matchable: %v", name, err)
		}
	}
}
