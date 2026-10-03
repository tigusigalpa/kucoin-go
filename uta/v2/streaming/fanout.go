package streaming

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tigusigalpa/kucoin-go/stream"
	"github.com/tigusigalpa/kucoin-go/websocket/uta"
)

// A fanned-out subscription sends its requests a few at a time, spaced apart:
// KuCoin allows a connection 300 client messages per 10 seconds on public hosts
// (subscribe, unsubscribe and ping count), and one request per symbol would
// otherwise take a full round trip each. It is a variable only so that tests need
// not wait.
const fanOutParallelism = 8

var fanOutGap = 50 * time.Millisecond

// subscribeEach subscribes to a channel for several symbols one request per symbol
// and merges the results into one Subscription. KuCoin accepts the "symbols" form
// only for a few channels (ticker, funding-fee); on the others it answers
// "symbols are not supported for topic ...", so those are fanned out here.
//
// A single symbol needs no merge and is subscribed directly. The fan-out is all
// or nothing: when a request fails, the symbols subscribed so far are
// unsubscribed again and the error of the first failed symbol is returned.
func subscribeEach[T any](ctx context.Context, c *uta.Client, base uta.SubscribeSpec, symbols []string, decode uta.DecodeFunc[T], opts []stream.SubscribeOption) (*stream.Subscription[T], error) {
	if len(symbols) == 1 {
		spec := base
		spec.Symbols = symbols
		return uta.SubscribeTyped(ctx, c, spec, decode, opts...)
	}
	subs := make([]*stream.Subscription[T], len(symbols))
	errs := make([]error, len(symbols))
	var (
		wg     sync.WaitGroup
		failed atomic.Bool
		slots  = make(chan struct{}, fanOutParallelism)
	)
	for i, symbol := range symbols {
		if failed.Load() {
			break
		}
		if i > 0 && !sleepCtx(ctx, fanOutGap) {
			errs[i] = ctx.Err()
			break
		}
		slots <- struct{}{}
		wg.Add(1)
		go func(i int, symbol string) {
			defer wg.Done()
			defer func() { <-slots }()
			spec := base
			spec.Symbols = []string{symbol}
			subs[i], errs[i] = uta.SubscribeTyped(ctx, c, spec, decode, opts...)
			if errs[i] != nil {
				failed.Store(true)
			}
		}(i, symbol)
	}
	wg.Wait()
	for i, err := range errs {
		if err == nil {
			continue
		}
		for _, done := range subs {
			if done != nil {
				_ = done.Close()
			}
		}
		return nil, fmt.Errorf("symbol %s: %w", symbols[i], err)
	}
	all := base
	all.Symbols = symbols
	return merge(all.Name(), subs), nil
}

// merge combines subscriptions into one that delivers all their updates. Updates
// of one member stay in order; updates of different members interleave. Closing
// the merged subscription closes every member; a member that ends with an error
// ends the merged subscription with that error and closes the others. The merged
// channel closes once every member's channel has closed.
func merge[T any](key string, subs []*stream.Subscription[T]) *stream.Subscription[T] {
	closeAll := func() error {
		var first error
		for _, s := range subs {
			if err := s.Close(); err != nil && first == nil {
				first = err
			}
		}
		return first
	}
	merged := stream.NewSubscription[T](key, 0, closeAll)
	merged.BindDropped(func() uint64 {
		var n uint64
		for _, s := range subs {
			n += s.Dropped()
		}
		return n
	})
	var wg sync.WaitGroup
	for _, member := range subs {
		wg.Add(1)
		go func(member *stream.Subscription[T]) {
			defer wg.Done()
			for v := range member.C() {
				if !merged.Deliver(v) {
					// The merged subscription ended; release this member too.
					_ = member.Close()
					return
				}
			}
		}(member)
		// A member that fails ends the merged subscription at once, also while the
		// forwarder above is held up by a consumer that is not reading.
		go func(member *stream.Subscription[T]) {
			select {
			case <-member.Done():
				if err := member.Err(); err != nil {
					merged.Finish(err)
					_ = closeAll()
				}
			case <-merged.Done():
			}
		}(member)
	}
	go func() {
		// Every member channel is closed by its producer after its last update, so
		// sealing here, after all forwarders returned, cannot race a Deliver.
		wg.Wait()
		merged.Seal()
	}()
	return merged
}
