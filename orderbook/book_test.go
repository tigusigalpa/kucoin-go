package orderbook

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/tigusigalpa/kucoin-go/types"
)

// Spread and Mid must pair the best bid and ask of one state of the book. The
// writer alternates between two books whose spread is always 1 but whose prices
// differ by 100: a bid from one state and an ask from the other would show up as
// a spread of 101 or -99.
func TestBook_SpreadAndMidAreReadFromOneStateWhileTheBookIsUpdated(t *testing.T) {
	book := New("XBTUSDTM")
	states := [2]Snapshot{
		{Sequence: 1, Bids: []Level{lv("100", "1")}, Asks: []Level{lv("101", "1")}},
		{Sequence: 2, Bids: []Level{lv("200", "1")}, Asks: []Level{lv("201", "1")}},
	}
	if err := book.Reset(states[0]); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := book.Reset(states[i%2]); err != nil {
				t.Errorf("Reset: %v", err)
				return
			}
			runtime.Gosched()
		}
	}()
	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := 0; i < 20000; i++ {
				if spread, ok := book.Spread(); !ok || spread != "1" {
					t.Errorf("Spread = %q, %v; the pair must come from one state", spread, ok)
					return
				}
				if mid, ok := book.Mid(); !ok || (mid != "100.5" && mid != "200.5") {
					t.Errorf("Mid = %q, %v; the pair must come from one state", mid, ok)
					return
				}
			}
		}()
	}
	readers.Wait()
	close(stop)
	writer.Wait()
}

func lv(price, size string) Level {
	return Level{Price: types.Decimal(price), Size: types.Decimal(size)}
}

func chg(side Side, price, size string) Change {
	return Change{Side: side, Price: types.Decimal(price), Size: types.Decimal(size)}
}

func prices(levels []Level) []string {
	out := make([]string, len(levels))
	for i, l := range levels {
		out[i] = string(l.Price)
	}
	return out
}

func mustApply(t *testing.T, b *Book, d Delta) bool {
	t.Helper()
	applied, err := b.Apply(d)
	if err != nil {
		t.Fatalf("Apply(%+v): %v", d, err)
	}
	return applied
}

func seeded(t *testing.T) *Book {
	t.Helper()
	b := New("XBTUSDTM")
	err := b.Reset(Snapshot{
		Sequence: 16,
		Bids:     []Level{lv("3988.49", "100"), lv("3988.51", "56"), lv("3988.50", "15"), lv("3988.48", "10")}, // unsorted on purpose
		Asks:     []Level{lv("3988.61", "32"), lv("3988.59", "3"), lv("3988.62", "8"), lv("3988.60", "47")},
	})
	if err != nil {
		t.Fatalf("Reset: %v", err)
	}
	return b
}

func TestBook_ResetSortsCanonicalisesAndSkipsZeroSizes(t *testing.T) {
	b := New("S")
	if b.Ready() || b.Symbol() != "S" {
		t.Fatal("a new book is empty and not ready")
	}
	err := b.Reset(Snapshot{
		Sequence: 5,
		Bids:     []Level{lv("100.0", "1"), lv("101", "2"), lv("99.50", "3"), lv("98", "0"), lv("101.00", "9")}, // 101 appears twice: last wins
		Asks:     []Level{lv("1.0E+2", "4"), lv("103", "5"), lv("102.5", "6")},
	})
	if err != nil {
		t.Fatal(err)
	}
	bids, asks := b.Top(0)
	if fmt.Sprint(prices(bids)) != "[101 100 99.5]" || bids[0].Size != "9" {
		t.Fatalf("bids = %v", bids)
	}
	if fmt.Sprint(prices(asks)) != "[100 102.5 103]" {
		t.Fatalf("asks = %v", asks)
	}
	if !b.Ready() || b.Sequence() != 5 {
		t.Fatalf("ready=%v seq=%d", b.Ready(), b.Sequence())
	}
	if nb, na := b.Len(); nb != 3 || na != 3 {
		t.Fatalf("len = %d/%d", nb, na)
	}
}

func TestBook_ResetRejectsInvalidInputAndKeepsTheOldBook(t *testing.T) {
	b := seeded(t)
	for name, snap := range map[string]Snapshot{
		"bad bid price": {Bids: []Level{lv("abc", "1")}},
		"bad ask size":  {Asks: []Level{lv("1", "abc")}},
		"negative size": {Bids: []Level{lv("1", "-1")}},
		"empty price":   {Bids: []Level{lv("", "1")}},
	} {
		if err := b.Reset(snap); !errors.Is(err, ErrInvalidLevel) {
			t.Errorf("%s: error = %v, want ErrInvalidLevel", name, err)
		}
	}
	if b.Sequence() != 16 {
		t.Fatalf("a failed Reset must leave the book untouched, sequence = %d", b.Sequence())
	}
	if bid, _ := b.BestBid(); bid.Price != "3988.51" {
		t.Fatalf("best bid changed to %v", bid)
	}
}

func TestBook_ApplyDocumentedFuturesExample(t *testing.T) {
	// KuCoin's own worked example: snapshot at sequence 16, then updates 17 and 18.
	b := seeded(t)
	if !mustApply(t, b, Delta{Start: 17, End: 17, Changes: []Change{chg(Bid, "3988.50", "44")}}) {
		t.Fatal("update 17 must apply")
	}
	if !mustApply(t, b, Delta{Start: 18, End: 18, Changes: []Change{chg(Ask, "3988.61", "0")}}) {
		t.Fatal("update 18 must apply")
	}
	bids, asks := b.Top(0)
	if fmt.Sprint(prices(asks)) != "[3988.59 3988.6 3988.62]" { // canonical prices drop trailing zeros
		t.Fatalf("asks = %v", asks)
	}
	if bids[1].Price != "3988.5" || bids[1].Size != "44" { // canonical price form
		t.Fatalf("bids = %v", bids)
	}
	if b.Sequence() != 18 {
		t.Fatalf("sequence = %d", b.Sequence())
	}
}

func TestBook_SequenceRule(t *testing.T) {
	tests := []struct {
		name        string
		start, end  int64
		wantApplied bool
		wantGap     bool
		wantSeq     int64
	}{
		{"next message", 17, 17, true, false, 17},
		{"range starting right after", 17, 20, true, false, 20},
		{"range overlapping the snapshot", 10, 18, true, false, 18},
		{"exactly the snapshot sequence is stale", 16, 16, false, false, 16},
		{"older is stale", 3, 9, false, false, 16},
		{"range ending at the snapshot is stale", 10, 16, false, false, 16},
		{"skipping one is a gap", 18, 18, false, true, 16},
		{"far ahead is a gap", 5000, 5001, false, true, 16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := seeded(t)
			applied, err := b.Apply(Delta{Start: tt.start, End: tt.end, Changes: []Change{chg(Bid, "1", "1")}})
			var gap *GapError
			if tt.wantGap {
				if !errors.Is(err, ErrSequenceGap) || !errors.As(err, &gap) || gap.Have != 16 || gap.Start != tt.start || gap.End != tt.end {
					t.Fatalf("error = %v, want a *GapError", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error %v", err)
			}
			if applied != tt.wantApplied || b.Sequence() != tt.wantSeq {
				t.Fatalf("applied=%v seq=%d, want %v / %d", applied, b.Sequence(), tt.wantApplied, tt.wantSeq)
			}
			bids, _ := b.Top(0)
			found := false
			for _, l := range bids {
				if l.Price == "1" {
					found = true
				}
			}
			if found != tt.wantApplied {
				t.Fatalf("level 1 present = %v, applied = %v: a rejected or stale delta must not touch the book", found, tt.wantApplied)
			}
		})
	}
	if err := (&GapError{Have: 1, Start: 5, End: 6}).Error(); err == "" {
		t.Fatal("GapError must describe itself")
	}
}

func TestBook_ApplyBeforeResetIsAnError(t *testing.T) {
	b := New("S")
	if _, err := b.Apply(Delta{Start: 1, End: 1}); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("error = %v", err)
	}
}

func TestBook_ApplyIsAtomicForInvalidChanges(t *testing.T) {
	b := seeded(t)
	_, err := b.Apply(Delta{Start: 17, End: 17, Changes: []Change{chg(Bid, "3988.51", "999"), chg(Ask, "nope", "1")}})
	if !errors.Is(err, ErrInvalidLevel) {
		t.Fatalf("error = %v", err)
	}
	if bid, _ := b.BestBid(); bid.Size != "56" || b.Sequence() != 16 {
		t.Fatalf("a delta with an invalid change must not be partially applied: %v seq=%d", bid, b.Sequence())
	}
	if _, err := b.Apply(Delta{Start: 17, End: 17, Changes: []Change{chg(Bid, "1", "-5")}}); !errors.Is(err, ErrInvalidLevel) {
		t.Fatalf("negative size: %v", err)
	}
}

func TestBook_EquivalentPriceSpellingsAreTheSameLevel(t *testing.T) {
	b := New("S")
	if err := b.Reset(Snapshot{Sequence: 1, Bids: []Level{lv("84486.0", "5")}}); err != nil { // REST number form
		t.Fatal(err)
	}
	mustApply(t, b, Delta{Start: 2, End: 2, Changes: []Change{chg(Bid, "84486", "7")}}) // WebSocket string form
	if nb, _ := b.Len(); nb != 1 {
		t.Fatalf("one price must be one level, got %d", nb)
	}
	mustApply(t, b, Delta{Start: 3, End: 3, Changes: []Change{chg(Bid, "8.4486E4", "0")}})
	if nb, _ := b.Len(); nb != 0 {
		t.Fatalf("removal through another spelling failed, %d levels left", nb)
	}
}

func TestBook_BestSpreadMidAndEmptySides(t *testing.T) {
	b := New("S")
	if _, ok := b.BestBid(); ok {
		t.Fatal("empty book has no best bid")
	}
	if _, ok := b.BestAsk(); ok {
		t.Fatal("empty book has no best ask")
	}
	if _, ok := b.Spread(); ok {
		t.Fatal("empty book has no spread")
	}
	if _, ok := b.Mid(); ok {
		t.Fatal("empty book has no mid")
	}
	b = seeded(t)
	bid, _ := b.BestBid()
	ask, _ := b.BestAsk()
	if bid.Price != "3988.51" || ask.Price != "3988.59" {
		t.Fatalf("bid=%v ask=%v", bid, ask)
	}
	if spread, ok := b.Spread(); !ok || spread != "0.08" {
		t.Fatalf("spread = %q", spread)
	}
	if mid, ok := b.Mid(); !ok || mid != "3988.55" {
		t.Fatalf("mid = %q", mid)
	}
	one := New("S")
	_ = one.Reset(Snapshot{Sequence: 1, Bids: []Level{lv("1", "1")}})
	if _, ok := one.Spread(); ok {
		t.Fatal("a one-sided book has no spread")
	}
}

func TestBook_TopAndSnapshotAreCopies(t *testing.T) {
	b := seeded(t)
	bids, asks := b.Top(2)
	if len(bids) != 2 || len(asks) != 2 || bids[0].Price != "3988.51" || asks[0].Price != "3988.59" {
		t.Fatalf("top = %v / %v", bids, asks)
	}
	bids[0].Size = "tampered"
	asks[0] = Level{}
	snap := b.Snapshot(3)
	if snap.Symbol != "XBTUSDTM" || snap.Sequence != 16 || len(snap.Bids) != 3 || len(snap.Asks) != 3 {
		t.Fatalf("snapshot = %+v", snap)
	}
	if bid, _ := b.BestBid(); bid.Size != "56" {
		t.Fatal("mutating the returned slice altered the book")
	}
	if all := b.Snapshot(0); len(all.Bids) != 4 || len(all.Asks) != 4 {
		t.Fatalf("depth 0 must return everything: %+v", all)
	}
	if bids, _ := b.Top(100); len(bids) != 4 {
		t.Fatalf("n beyond the size returns everything, got %d", len(bids))
	}
	// A snapshot round-trips into a fresh book.
	clone := New("clone")
	if err := clone.Reset(b.Snapshot(0)); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(clone.Snapshot(0).Bids) != fmt.Sprint(b.Snapshot(0).Bids) {
		t.Fatal("snapshot round trip lost levels")
	}
}

func TestBook_SnapshotIfReadyTellsAnEmptyBookFromAnUnreadyOne(t *testing.T) {
	b := New("XBTUSDTM")
	if snap, ok := b.SnapshotIfReady(0); ok || snap.Sequence != 0 || len(snap.Bids)+len(snap.Asks) != 0 || snap.Symbol != "XBTUSDTM" {
		t.Fatalf("a new book is not ready: %+v ok=%v", snap, ok)
	}
	if err := b.Reset(Snapshot{Sequence: 7}); err != nil {
		t.Fatal(err)
	}
	if snap, ok := b.SnapshotIfReady(0); !ok || snap.Sequence != 7 {
		t.Fatalf("an empty but initialised book is ready: %+v ok=%v", snap, ok)
	}
	if err := b.Reset(Snapshot{Sequence: 9, Bids: []Level{lv("100", "2")}, Asks: []Level{lv("101", "3"), lv("102", "4")}}); err != nil {
		t.Fatal(err)
	}
	snap, ok := b.SnapshotIfReady(1)
	if !ok || snap.Sequence != 9 || len(snap.Bids) != 1 || len(snap.Asks) != 1 || snap.Asks[0].Price != "101" {
		t.Fatalf("depth-limited snapshot: %+v ok=%v", snap, ok)
	}
	b.Clear()
	if snap, ok := b.SnapshotIfReady(0); ok || snap.Sequence != 0 || len(snap.Bids) != 0 {
		t.Fatalf("a cleared book is not ready: %+v ok=%v", snap, ok)
	}
}

func TestBook_ClearMakesItUnusableUntilReset(t *testing.T) {
	b := seeded(t)
	b.Clear()
	if b.Ready() || b.Sequence() != 0 {
		t.Fatal("Clear must reset the state")
	}
	if nb, na := b.Len(); nb+na != 0 {
		t.Fatal("Clear must drop all levels")
	}
	if _, err := b.Apply(Delta{Start: 1, End: 1}); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("error = %v", err)
	}
}

func TestParseSide(t *testing.T) {
	for in, want := range map[string]Side{"buy": Bid, "BUY": Bid, "bid": Bid, "bids": Bid, "sell": Ask, "Sell": Ask, "ask": Ask, " asks ": Ask} {
		got, err := ParseSide(in)
		if err != nil || got != want {
			t.Errorf("ParseSide(%q) = %v, %v", in, got, err)
		}
	}
	if _, err := ParseSide("sideways"); !errors.Is(err, ErrInvalidLevel) {
		t.Fatalf("error = %v", err)
	}
	if Bid.String() != "bid" || Ask.String() != "ask" {
		t.Fatal("side names")
	}
}

func TestLevel_JSON(t *testing.T) {
	var levels []Level
	if err := json.Unmarshal([]byte(`[[84491.3,1220],["84486.0","7"],[1.0E-4,3,"2"]]`), &levels); err != nil {
		t.Fatal(err)
	}
	if levels[0].Price != "84491.3" || levels[0].Size != "1220" || levels[1].Price != "84486.0" || levels[2].Price != "1.0E-4" || levels[2].RPISize != "2" {
		t.Fatalf("levels = %+v", levels)
	}
	b, _ := json.Marshal(levels)
	if string(b) != `[["84491.3","1220"],["84486.0","7"],["1.0E-4","3","2"]]` {
		t.Fatalf("marshalled = %s", b)
	}
	for _, bad := range []string{`[1]`, `[]`, `{}`, `["a"]`, `[1,true]`, `[true,1]`, `[1,2,[]]`, `"x"`} {
		var l Level
		if err := json.Unmarshal([]byte(bad), &l); err == nil {
			t.Errorf("%s must fail", bad)
		}
	}
}

// A randomised model check: whatever deltas arrive, the book must equal a plain
// map-based reference implementation.
func TestBook_MatchesReferenceModelUnderRandomUpdates(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for round := 0; round < 40; round++ {
		b := New("S")
		model := [2]map[string]string{{}, {}}
		var snap Snapshot
		for i := 0; i < 20; i++ {
			p := strconv.Itoa(100+rng.Intn(30)) + "." + strconv.Itoa(rng.Intn(10))
			s := strconv.Itoa(1 + rng.Intn(500))
			if i%2 == 0 {
				snap.Bids = append(snap.Bids, lv(p, s))
				model[Bid][canon(p)] = s
			} else {
				snap.Asks = append(snap.Asks, lv(p, s))
				model[Ask][canon(p)] = s
			}
		}
		snap.Sequence = 1000
		if err := b.Reset(snap); err != nil {
			t.Fatal(err)
		}
		seq := int64(1000)
		for i := 0; i < 400; i++ {
			side := Side(rng.Intn(2))
			p := strconv.Itoa(100+rng.Intn(30)) + "." + strconv.Itoa(rng.Intn(10))
			size := "0"
			if rng.Intn(3) != 0 {
				size = strconv.Itoa(1 + rng.Intn(500))
			}
			start := seq + 1
			if rng.Intn(5) == 0 { // an overlapping range now and then
				start = seq - int64(rng.Intn(3))
			}
			seq += 1 + int64(rng.Intn(2))
			mustApply(t, b, Delta{Start: start, End: seq, Changes: []Change{chg(side, p, size)}})
			if size == "0" {
				delete(model[side], canon(p))
			} else {
				model[side][canon(p)] = size
			}
		}
		snapOut := b.Snapshot(0)
		for _, sd := range []struct {
			name   string
			levels []Level
			model  map[string]string
			desc   bool
		}{{"bids", snapOut.Bids, model[Bid], true}, {"asks", snapOut.Asks, model[Ask], false}} {
			if len(sd.levels) != len(sd.model) {
				t.Fatalf("round %d %s: %d levels, model has %d", round, sd.name, len(sd.levels), len(sd.model))
			}
			if !sort.SliceIsSorted(sd.levels, func(i, j int) bool {
				c, _ := sd.levels[i].Price.Cmp(sd.levels[j].Price)
				if sd.desc {
					return c > 0
				}
				return c < 0
			}) {
				t.Fatalf("round %d %s are not sorted: %v", round, sd.name, prices(sd.levels))
			}
			for _, l := range sd.levels {
				if sd.model[string(l.Price)] != string(l.Size) {
					t.Fatalf("round %d %s: level %s size %s, model %s", round, sd.name, l.Price, l.Size, sd.model[string(l.Price)])
				}
			}
		}
	}
}

func canon(p string) string {
	c, _ := types.Decimal(p).Canonical()
	return string(c)
}

func TestBook_ConcurrentReadersAndOneWriter(t *testing.T) {
	b := seeded(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = b.BestBid()
				_, _ = b.BestAsk()
				_, _ = b.Spread()
				_, _ = b.Mid()
				_, _ = b.Len()
				_ = b.Sequence()
				snap := b.Snapshot(5)
				for i := 1; i < len(snap.Bids); i++ {
					if c, _ := snap.Bids[i-1].Price.Cmp(snap.Bids[i].Price); c <= 0 {
						t.Errorf("unsorted bids in a concurrent snapshot: %v", prices(snap.Bids))
						return
					}
				}
				runtime.Gosched() // with one CPU a spinning reader would starve the writer
			}
		}()
	}
	for seq := int64(17); seq < 3000; seq++ {
		p := strconv.Itoa(3980 + int(seq%20))
		if _, err := b.Apply(Delta{Start: seq, End: seq, Changes: []Change{chg(Bid, p, strconv.Itoa(int(seq))), chg(Ask, "4100", "1")}}); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}
