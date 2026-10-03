package orderbook

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/tigusigalpa/kucoin-go/types"
)

// Typed errors reported by Book.
var (
	// ErrNotInitialized is returned by Apply before the book was seeded with a
	// snapshot (Reset).
	ErrNotInitialized = errors.New("kucoin: order book is not initialised")
	// ErrSequenceGap is wrapped by *GapError: a delta does not continue the
	// sequence of the book, so updates were missed and the book must be rebuilt
	// from a fresh snapshot.
	ErrSequenceGap = errors.New("kucoin: order book sequence gap")
	// ErrInvalidLevel is returned for a price or size that is not a decimal
	// number, or for a negative size.
	ErrInvalidLevel = errors.New("kucoin: invalid order book level")
)

// GapError is the error Apply returns for a delta that skips sequence numbers.
// It matches ErrSequenceGap with errors.Is.
type GapError struct {
	// Have is the sequence of the book; the next delta must start at or before
	// Have+1.
	Have int64
	// Start and End are the sequence range of the offending delta.
	Start, End int64
}

// Error implements error.
func (e *GapError) Error() string {
	return fmt.Sprintf("kucoin: order book sequence gap: book is at %d but the delta covers %d..%d", e.Have, e.Start, e.End)
}

// Is reports that a GapError is an ErrSequenceGap.
func (e *GapError) Is(target error) bool { return target == ErrSequenceGap }

// Side is a side of the order book.
type Side uint8

// Order book sides.
const (
	Bid Side = iota
	Ask
)

// String returns "bid" or "ask".
func (s Side) String() string {
	if s == Ask {
		return "ask"
	}
	return "bid"
}

// ParseSide accepts KuCoin's side spellings: "buy"/"bid" for the bid side and
// "sell"/"ask" for the ask side, in any case.
func ParseSide(s string) (Side, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "buy", "bid", "bids":
		return Bid, nil
	case "sell", "ask", "asks":
		return Ask, nil
	}
	return Bid, fmt.Errorf("%w: unknown side %q", ErrInvalidLevel, s)
}

// Change is one update of a price level. A zero Size removes the level.
type Change struct {
	Side  Side
	Price types.Decimal
	Size  types.Decimal
}

// Snapshot is a complete picture of an order book at one sequence number.
// Bids are ordered best (highest) first and asks best (lowest) first when a
// Book produces them; a Snapshot built by hand may be in any order.
type Snapshot struct {
	Symbol   string
	Sequence int64
	Bids     []Level
	Asks     []Level
}

// Delta is an incremental update covering the inclusive sequence range
// Start..End (Start == End for feeds with one sequence number per message).
type Delta struct {
	Symbol  string
	Start   int64
	End     int64
	Changes []Change
}

// entry is a stored price level: the canonical price and the size as received.
type entry struct {
	price string
	size  types.Decimal
}

// side holds the levels of one side sorted best first.
type side struct {
	descending bool // bids: highest price first
	levels     []entry
}

// index returns the position of price, or where it would be inserted.
func (s *side) index(price string) (int, bool) {
	i := sort.Search(len(s.levels), func(i int) bool {
		c := types.CompareCanonical(s.levels[i].price, price)
		if s.descending {
			return c <= 0
		}
		return c >= 0
	})
	return i, i < len(s.levels) && s.levels[i].price == price
}

func (s *side) set(price string, size types.Decimal) {
	i, found := s.index(price)
	if found {
		s.levels[i].size = size
		return
	}
	s.levels = append(s.levels, entry{})
	copy(s.levels[i+1:], s.levels[i:])
	s.levels[i] = entry{price: price, size: size}
}

func (s *side) remove(price string) {
	if i, found := s.index(price); found {
		s.levels = append(s.levels[:i], s.levels[i+1:]...)
	}
}

func (s *side) top(n int) []Level {
	if n <= 0 || n > len(s.levels) {
		n = len(s.levels)
	}
	out := make([]Level, n)
	for i := 0; i < n; i++ {
		out[i] = Level{Price: types.Decimal(s.levels[i].price), Size: s.levels[i].size}
	}
	return out
}

// Book is a local order book with exact decimal prices. It is seeded with a
// Snapshot, kept current with sequenced Deltas, and safe for concurrent use:
// any number of goroutines may read it while one applies updates.
//
// Prices are stored in canonical form ("84486.0" and "84486" are the same level
// and are reported as "84486"); sizes are kept exactly as received.
//
// The sequence rule is the one KuCoin documents for every feed: a delta applies
// when Start <= Sequence+1 and End > Sequence; a delta that ends at or before the
// current sequence is stale and ignored; a delta that starts after Sequence+1
// means updates were missed (*GapError).
type Book struct {
	mu     sync.RWMutex
	symbol string
	bids   side
	asks   side
	seq    int64
	ready  bool
}

// New creates an empty, uninitialised book for symbol.
func New(symbol string) *Book {
	return &Book{symbol: symbol, bids: side{descending: true}}
}

// Symbol returns the symbol the book was created for.
func (b *Book) Symbol() string { return b.symbol }

// Ready reports whether the book holds a snapshot and can accept deltas.
func (b *Book) Ready() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.ready
}

// Sequence returns the sequence number of the last applied snapshot or delta.
func (b *Book) Sequence() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.seq
}

// Clear empties the book and marks it uninitialised.
func (b *Book) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bids.levels, b.asks.levels = nil, nil
	b.seq, b.ready = 0, false
}

// Reset replaces the content of the book with the snapshot. Levels with a zero
// size are skipped, a repeated price keeps its last size, and input order does
// not matter. On error the book is left unchanged.
func (b *Book) Reset(s Snapshot) error {
	bids, err := buildSide(s.Bids, true)
	if err != nil {
		return err
	}
	asks, err := buildSide(s.Asks, false)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bids, b.asks = bids, asks
	b.seq, b.ready = s.Sequence, true
	return nil
}

func buildSide(levels []Level, descending bool) (side, error) {
	index := make(map[string]types.Decimal, len(levels))
	for _, l := range levels {
		price, err := canonicalPrice(l.Price)
		if err != nil {
			return side{}, err
		}
		zero, err := checkSize(l.Size)
		if err != nil {
			return side{}, err
		}
		if zero {
			delete(index, price)
			continue
		}
		index[price] = l.Size
	}
	s := side{descending: descending, levels: make([]entry, 0, len(index))}
	for price, size := range index {
		s.levels = append(s.levels, entry{price: price, size: size})
	}
	sort.Slice(s.levels, func(i, j int) bool {
		c := types.CompareCanonical(s.levels[i].price, s.levels[j].price)
		if descending {
			return c > 0
		}
		return c < 0
	})
	return s, nil
}

func canonicalPrice(p types.Decimal) (string, error) {
	c, err := p.Canonical()
	if err != nil {
		return "", fmt.Errorf("%w: price %q", ErrInvalidLevel, string(p))
	}
	return string(c), nil
}

// checkSize validates a size and reports whether it is zero.
func checkSize(s types.Decimal) (zero bool, err error) {
	sign, err := s.Sign()
	if err != nil || sign < 0 {
		return false, fmt.Errorf("%w: size %q", ErrInvalidLevel, string(s))
	}
	return sign == 0, nil
}

// Apply applies a delta under the sequence rule documented on Book. It reports
// whether the delta changed the book: a stale delta (one that ends at or before
// the book's sequence) is ignored and returns (false, nil). A delta that skips
// sequence numbers returns a *GapError and leaves the book unchanged, as does a
// delta containing an invalid price or size.
func (b *Book) Apply(d Delta) (applied bool, err error) {
	changes, err := prepare(d.Changes)
	if err != nil {
		return false, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.ready {
		return false, ErrNotInitialized
	}
	if d.End <= b.seq {
		return false, nil
	}
	if d.Start > b.seq+1 {
		return false, &GapError{Have: b.seq, Start: d.Start, End: d.End}
	}
	for _, c := range changes {
		target := &b.bids
		if c.side == Ask {
			target = &b.asks
		}
		if c.zero {
			target.remove(c.price)
		} else {
			target.set(c.price, c.size)
		}
	}
	b.seq = d.End
	return true, nil
}

// prepared is a validated, canonicalised Change.
type prepared struct {
	side  Side
	price string
	size  types.Decimal
	zero  bool
}

// prepare validates every change up front so a delta is applied completely or
// not at all.
func prepare(changes []Change) ([]prepared, error) {
	out := make([]prepared, len(changes))
	for i, c := range changes {
		price, err := canonicalPrice(c.Price)
		if err != nil {
			return nil, err
		}
		zero, err := checkSize(c.Size)
		if err != nil {
			return nil, err
		}
		out[i] = prepared{side: c.Side, price: price, size: c.Size, zero: zero}
	}
	return out, nil
}

// Validate reports whether every price and size of the delta is well formed, so
// that a malformed update can be rejected before it is stored.
func (d Delta) Validate() error {
	_, err := prepare(d.Changes)
	return err
}

// BestBid returns the highest bid.
func (b *Book) BestBid() (Level, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.bids.levels) == 0 {
		return Level{}, false
	}
	e := b.bids.levels[0]
	return Level{Price: types.Decimal(e.price), Size: e.size}, true
}

// BestAsk returns the lowest ask.
func (b *Book) BestAsk() (Level, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.asks.levels) == 0 {
		return Level{}, false
	}
	e := b.asks.levels[0]
	return Level{Price: types.Decimal(e.price), Size: e.size}, true
}

// bestPair returns the best bid and the best ask of one and the same state of
// the book: reading them with two separate calls could pair a bid and an ask from
// different updates.
func (b *Book) bestPair() (bid, ask Level, ok bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.bids.levels) == 0 || len(b.asks.levels) == 0 {
		return Level{}, Level{}, false
	}
	hi, lo := b.bids.levels[0], b.asks.levels[0]
	return Level{Price: types.Decimal(hi.price), Size: hi.size}, Level{Price: types.Decimal(lo.price), Size: lo.size}, true
}

// Spread returns best ask minus best bid, exactly. ok is false when either side
// is empty. A negative spread means a crossed book, which KuCoin's feeds do not
// produce in steady state. Both prices come from the same state of the book,
// also while other goroutines update it.
func (b *Book) Spread() (spread types.Decimal, ok bool) {
	bid, ask, ok := b.bestPair()
	if !ok {
		return "", false
	}
	s, err := ask.Price.Sub(bid.Price)
	return s, err == nil
}

// Mid returns the mid price, (best bid + best ask) / 2, exactly. Like Spread it
// uses one consistent state of the book.
func (b *Book) Mid() (mid types.Decimal, ok bool) {
	bid, ask, ok := b.bestPair()
	if !ok {
		return "", false
	}
	sum, err := ask.Price.Add(bid.Price)
	if err != nil {
		return "", false
	}
	m, err := sum.Mul("0.5")
	return m, err == nil
}

// Len returns the number of bid and ask levels.
func (b *Book) Len() (bids, asks int) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.bids.levels), len(b.asks.levels)
}

// Top returns copies of the best n levels of each side, best first. n <= 0
// returns every level.
func (b *Book) Top(n int) (bids, asks []Level) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.bids.top(n), b.asks.top(n)
}

// Snapshot returns a consistent copy of the best depth levels of each side
// together with the sequence number they correspond to; depth <= 0 copies the
// whole book. A book that is not Ready (not yet initialised, or being rebuilt)
// yields an empty snapshot with sequence 0; use SnapshotIfReady to tell that apart
// from a book that is merely empty.
func (b *Book) Snapshot(depth int) Snapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return Snapshot{Symbol: b.symbol, Sequence: b.seq, Bids: b.bids.top(depth), Asks: b.asks.top(depth)}
}

// SnapshotIfReady is Snapshot that also reports, in the same atomic step, whether
// the book was Ready. When it was not, the snapshot is empty and ok is false: the
// book is being (re)built and a consumer reloading from it must wait for the next
// EventSnapshot instead of adopting an empty copy.
func (b *Book) SnapshotIfReady(depth int) (snap Snapshot, ok bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if !b.ready {
		return Snapshot{Symbol: b.symbol}, false
	}
	return Snapshot{Symbol: b.symbol, Sequence: b.seq, Bids: b.bids.top(depth), Asks: b.asks.top(depth)}, true
}
