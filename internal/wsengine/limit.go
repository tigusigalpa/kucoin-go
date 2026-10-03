package wsengine

import (
	"context"
	"sync"
	"time"
)

// MessageLimiter is implemented by a Protocol whose server limits how many
// messages one connection may send. The engine then paces its requests —
// subscribe, unsubscribe, authentication — to stay inside the limit: KuCoin may
// drop a connection that exceeds it, and waiting a moment is far cheaper than the
// reconnect and resubscription that follow being dropped. A large restore after a
// reconnect is what runs into the limit most easily.
type MessageLimiter interface {
	// MessageLimit returns how many messages one connection may send per window,
	// heartbeats included. Zero messages means no limit.
	MessageLimit() (messages int, window time.Duration)
}

// sendWindow is a sliding-window budget of client messages: at most max of them
// in any interval of one window. It is safe for concurrent use; a nil
// *sendWindow does not limit anything.
type sendWindow struct {
	mu     sync.Mutex
	max    int
	window time.Duration
	sent   []time.Time // send times inside the window, oldest first
}

// newSendWindow returns the budget for a server limit of limit messages per
// window, or nil when there is no limit. A tenth of the limit is kept in reserve
// for the messages that are counted but never delayed, the heartbeats.
func newSendWindow(limit int, window time.Duration) *sendWindow {
	if limit <= 0 || window <= 0 {
		return nil
	}
	max := limit - limit/10
	if max < 1 {
		max = 1
	}
	return &sendWindow{max: max, window: window}
}

func (w *sendWindow) expireLocked(now time.Time) {
	i := 0
	for i < len(w.sent) && now.Sub(w.sent[i]) >= w.window {
		i++
	}
	if i > 0 {
		w.sent = append(w.sent[:0], w.sent[i:]...)
	}
}

// reserve books one message, first waiting until the budget allows it. It
// returns the error of ctx, or cause() once done is closed, when either ends the
// wait.
func (w *sendWindow) reserve(ctx context.Context, done <-chan struct{}, cause func() error) error {
	_, err := w.book(ctx, done, cause)
	return err
}

// book is reserve that also returns the moment the message was booked.
func (w *sendWindow) book(ctx context.Context, done <-chan struct{}, cause func() error) (time.Time, error) {
	if w == nil {
		return time.Now(), nil
	}
	for {
		w.mu.Lock()
		now := time.Now()
		w.expireLocked(now)
		if len(w.sent) < w.max {
			w.sent = append(w.sent, now)
			w.mu.Unlock()
			return now, nil
		}
		// The message may go once enough older ones have left the window for the
		// count to drop below max.
		wait := w.sent[len(w.sent)-w.max].Add(w.window).Sub(now)
		w.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return time.Time{}, ctx.Err()
		case <-done:
			timer.Stop()
			return time.Time{}, cause()
		}
	}
}

// note counts a message that is sent without waiting (a heartbeat), which the
// server counts all the same.
func (w *sendWindow) note() {
	if w == nil {
		return
	}
	w.mu.Lock()
	now := time.Now()
	w.expireLocked(now)
	w.sent = append(w.sent, now)
	w.mu.Unlock()
}
