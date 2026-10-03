package orderbook

import (
	"time"

	"github.com/tigusigalpa/kucoin-go/stream"
)

// finalEventGrace is how long a failed book waits for its consumer to read the
// final EventFailed before the subscription ends regardless. It is a variable
// only so that tests need not wait for it.
var finalEventGrace = time.Second

// DeliverTo points the Syncer's events at a typed subscription, the way every
// managed order book of this module does it.
//
// Events reach sub.C() in the order they occurred. A consumer that is slow or
// absent never stalls the book (see SyncerConfig.EventQueue and EventOverflow).
// When the Syncer fails for good the subscription ends with the cause as its Err:
// an attentive consumer gets the final EventFailed first, an absent one is not
// waited for beyond a short grace period. end is called after the subscription
// has been finished, to release what sits underneath it (typically the
// connection-level subscription); it may be called more than once and may be nil.
func (c *SyncerConfig) DeliverTo(sub *stream.Subscription[Event], end func()) {
	c.OnEvent = func(ev Event) {
		if ev.Type != EventFailed {
			sub.Deliver(ev)
			return
		}
		sub.DeliverWithin(ev, finalEventGrace)
		sub.Finish(ev.Err)
		if end != nil {
			go end()
		}
	}
	// The final event queues behind the ones nobody has read; this does not.
	c.OnFail = func(err error) {
		select {
		case <-sub.Done():
		case <-time.After(finalEventGrace):
			sub.Finish(err)
		}
		if end != nil {
			end()
		}
	}
}
