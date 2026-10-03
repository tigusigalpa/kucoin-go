package wsengine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tigusigalpa/kucoin-go/stream"
)

// restoreParallelism bounds how many resubscribe requests are in flight at once
// after a reconnect; restoreGap spaces them to stay clear of KuCoin's request
// rate limit.
const (
	restoreParallelism = 8
	restoreGap         = 10 * time.Millisecond
	// overflowEventEvery rate-limits EventOverflow per subscription.
	overflowEventEvery = time.Second
)

// sub is the engine's record of one subscription.
type sub struct {
	spec Spec
	h    stream.Handler
	mb   *mailbox

	// ended is claimed by whoever ends the subscription first. From then on no
	// frame is delivered, even while the name and routes are still reserved.
	ended atomic.Bool
	// done is closed, after endErr was set, when the handler has been told.
	done      chan struct{}
	endErr    error
	abortOnce sync.Once
	// sentGen is the generation of the connection a subscribe frame was last
	// written to (0: never). Ending the subscription unsubscribes on that
	// connection only: on any other one KuCoin holds nothing to cancel.
	sentGen atomic.Uint64
	// unsubSent keeps the unsubscribe frame to once per subscription.
	unsubSent    atomic.Bool
	dropped      atomic.Uint64
	lastOverflow atomic.Int64
}

// SubHandle is the engine-side handle of a live subscription.
type SubHandle struct {
	c  *Conn
	sb *sub
}

// Name returns the subscription's identity.
func (h *SubHandle) Name() string { return h.sb.spec.Name }

// Dropped returns how many pushes the overflow policy discarded for this
// subscription so far.
func (h *SubHandle) Dropped() uint64 { return h.sb.dropped.Load() }

// Close unsubscribes: delivery stops immediately, the handler is aborted
// without an error and, when KuCoin may hold the subscription on the live
// connection, an unsubscribe request is sent and awaited; the topic stays taken
// until that frame has been written. The returned error only reports that
// request failing; the subscription is gone either way. Close is idempotent.
func (h *SubHandle) Close() error { return h.c.unsubscribe(h.sb) }

// Subscribe registers spec, sends its subscribe frame and waits for KuCoin's
// acknowledgement. While the connection is being re-established it waits for it
// (until ctx is done) instead of failing, and so it does when the connection
// drops before the acknowledgement arrives: the registered subscription is
// subscribed again by the reconnect. KuCoin's rejection is returned as a
// *stream.ServerError.
func (c *Conn) Subscribe(ctx context.Context, spec Spec) (*SubHandle, error) {
	s, sb, err := c.register(ctx, spec)
	if err != nil {
		return nil, err
	}
	id := newID()
	err = s.request(ctx, id, spec.Subscribe(id), func(werr error) {
		if werr == nil {
			sb.sentGen.Store(s.gen)
		}
	})
	if err == nil {
		return &SubHandle{c: c, sb: sb}, nil
	}
	cur := s
	if !isServerError(err) && ctx.Err() == nil && isDeadSession(s) {
		werr := c.awaitRestored(ctx, s, sb)
		if werr == nil {
			return &SubHandle{c: c, sb: sb}, nil
		}
		err = werr
		cur = c.currentSession()
	}
	if isServerError(err) {
		c.endSub(sb, err) // rejected: KuCoin holds nothing to cancel
	} else {
		// The outcome at KuCoin is unknown (timeout, cancellation): try to keep
		// it from leaving a stray subscription behind.
		_, _ = c.retire(sb, err, cur, false)
	}
	return nil, fmt.Errorf("kucoin: ws subscribe %s: %w", spec.Name, err)
}

func isServerError(err error) bool {
	var se *stream.ServerError
	return errors.As(err, &se)
}

// register adds the subscription to the registry — before its frame is sent, so
// no early push can be lost — and starts its delivery goroutine.
func (c *Conn) register(ctx context.Context, spec Spec) (*session, *sub, error) {
	if spec.Subscribe == nil || spec.Handler == nil || spec.Name == "" {
		return nil, nil, errors.New("kucoin: ws: subscription needs a name, a subscribe frame and a handler")
	}
	// A route listed twice (a symbol repeated in one request) would register the
	// subscription twice for it and deliver every push twice.
	spec.Routes = uniqueRoutes(spec.Routes)
	for {
		c.mu.Lock()
		if c.closed {
			err := c.closedErrLocked()
			c.mu.Unlock()
			return nil, nil, err
		}
		switch c.state {
		case stream.StateConnected:
			if c.cur == nil {
				break // the dead session is being replaced: wait like during a reconnect
			}
			if _, dup := c.subs[spec.Name]; dup {
				c.mu.Unlock()
				return nil, nil, fmt.Errorf("%w: %s", stream.ErrAlreadySubscribed, spec.Name)
			}
			// Two subscriptions must not share a routing key: KuCoin keys its own
			// subscriptions the same way, so unsubscribing one would silently stop
			// the data of the other.
			for _, r := range spec.Routes {
				if owners := c.routes[r]; len(owners) > 0 {
					c.mu.Unlock()
					return nil, nil, fmt.Errorf("%w: %s overlaps %s on %s", stream.ErrAlreadySubscribed, spec.Name, owners[0].spec.Name, r)
				}
			}
			sb := c.newSubLocked(spec)
			s := c.cur
			c.wg.Add(1)
			c.mu.Unlock()
			go c.pump(sb)
			return s, sb, nil
		case stream.StateIdle:
			c.mu.Unlock()
			return nil, nil, stream.ErrNotConnected
		case stream.StateClosing, stream.StateClosed:
			err := c.closedErrLocked()
			c.mu.Unlock()
			return nil, nil, err
		}
		wait := c.stateCh // connecting or reconnecting: wait for the next change
		c.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
}

// uniqueRoutes returns routes without repeated keys, keeping the order.
func uniqueRoutes(routes []string) []string {
	if len(routes) < 2 {
		return routes
	}
	seen := make(map[string]struct{}, len(routes))
	out := make([]string, 0, len(routes))
	for _, r := range routes {
		if _, dup := seen[r]; !dup {
			seen[r] = struct{}{}
			out = append(out, r)
		}
	}
	return out
}

func (c *Conn) newSubLocked(spec Spec) *sub {
	limit := c.cfg.BufferSize
	if spec.Buffer > 0 {
		limit = spec.Buffer
	}
	policy := c.cfg.Overflow
	if spec.OverflowSet {
		policy = spec.Overflow
	}
	sb := &sub{spec: spec, h: spec.Handler, mb: newMailbox(limit, policy), done: make(chan struct{})}
	c.subs[spec.Name] = sb
	c.order = append(c.order, sb)
	for _, r := range spec.Routes {
		c.routes[r] = append(c.routes[r], sb)
	}
	return sb
}

// awaitRestored waits until sb has been subscribed again on a connection that
// replaced dead, or has ended. It is for a Subscribe whose connection dropped
// under it.
func (c *Conn) awaitRestored(ctx context.Context, dead *session, sb *sub) error {
	for {
		c.mu.RLock()
		var closedErr error
		if c.closed {
			closedErr = c.closedErrLocked()
		}
		state, cur, wait := c.state, c.cur, c.stateCh
		c.mu.RUnlock()

		select {
		case <-sb.done:
			if sb.endErr != nil {
				return sb.endErr
			}
			return stream.ErrClosed
		default:
		}
		switch {
		case closedErr != nil:
			return closedErr
		case state == stream.StateConnected && cur != nil && cur != dead:
			return nil
		}
		select {
		case <-wait:
		case <-sb.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// pump is a subscription's delivery goroutine: it feeds the handler in order and
// is the only goroutine that ever calls the handler's data methods.
func (c *Conn) pump(sb *sub) {
	defer c.wg.Done()
	defer sb.h.OnClosed()
	defer func() {
		if r := recover(); r != nil {
			// A handler bug must not take the host application down.
			c.log.Error("kucoin: ws handler panicked", "subscription", sb.spec.Name, "panic", r)
			_, _ = c.retire(sb, fmt.Errorf("kucoin: ws: handler for %s panicked: %v", sb.spec.Name, r), c.currentSession(), false)
		}
	}()
	for {
		it, ok := sb.mb.pop()
		if !ok {
			if sb.mb.isClosed() {
				return
			}
			<-sb.mb.wake
			continue
		}
		switch it.kind {
		case itemFrame:
			sb.h.OnFrame(it.frame)
		case itemReset:
			sb.h.OnReset(it.n)
		case itemGap:
			sb.h.OnGap(it.n)
		}
	}
}

func (c *Conn) currentSession() *session {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cur
}

// finishSub claims the end of the subscription and aborts its handler with err:
// delivery stops now. It reports whether this call ended it; only the first
// call does. The name and routes stay reserved until release.
func (c *Conn) finishSub(sb *sub, err error) bool {
	if !sb.ended.CompareAndSwap(false, true) {
		return false
	}
	c.abortSub(sb, err)
	return true
}

// endSub ends the subscription and frees its name and routes at once, for the
// cases where KuCoin holds nothing to unsubscribe.
func (c *Conn) endSub(sb *sub, err error) bool {
	if !c.finishSub(sb, err) {
		return false
	}
	c.release(sb)
	return true
}

// retire ends the subscription and, when KuCoin may hold it (a subscribe frame
// was written to s), unsubscribes it there; see dispose. It reports whether this
// call ended the subscription.
func (c *Conn) retire(sb *sub, cause error, s *session, wait bool) (bool, error) {
	if !c.finishSub(sb, cause) {
		return false, nil
	}
	return true, c.dispose(sb, s, wait)
}

// dispose sends the unsubscribe frame of an ended subscription on s, when KuCoin
// may hold it there, and then frees its name and routes. They stay reserved
// until the frame has been written: a new subscription to the same topic
// therefore cannot get its subscribe frame onto the wire ahead of this
// unsubscribe, which KuCoin would apply to it. With wait the reply is awaited
// too and a failure reported.
func (c *Conn) dispose(sb *sub, s *session, wait bool) error {
	if s == nil || sb.spec.Unsubscribe == nil || sb.sentGen.Load() != s.gen || !sb.unsubSent.CompareAndSwap(false, true) {
		c.release(sb)
		return nil
	}
	if !wait {
		_ = s.send(context.Background(), sb.spec.Unsubscribe(newID()))
		c.release(sb)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.AckTimeout)
	defer cancel()
	id := newID()
	err := s.request(ctx, id, sb.spec.Unsubscribe(id), func(error) { c.release(sb) })
	if err != nil {
		// A connection that died meanwhile has dropped the subscription anyway.
		if errors.Is(err, stream.ErrClosed) || isDeadSession(s) {
			return nil
		}
		return fmt.Errorf("kucoin: ws unsubscribe %s: %w", sb.spec.Name, err)
	}
	return nil
}

// abortSub tells the handler the subscription is over, once, and releases its
// queue. The registry is not touched; callers that have already detached the
// subscription (Shutdown, fatal close) use it directly.
func (c *Conn) abortSub(sb *sub, err error) {
	sb.ended.Store(true)
	sb.abortOnce.Do(func() {
		sb.endErr = err
		close(sb.done)
		// The queue is released whatever the handler does: its goroutine would
		// otherwise wait for a close that never comes.
		defer sb.mb.closeDiscard()
		// OnAbort runs on whichever goroutine ends the subscription — often the
		// socket reader or the caller of Close — which a handler bug must not take
		// down.
		defer func() {
			if r := recover(); r != nil {
				c.log.Error("kucoin: ws handler panicked in OnAbort", "subscription", sb.spec.Name, "panic", r)
			}
		}()
		sb.h.OnAbort(err)
	})
}

// release frees the subscription's name and routes. It is idempotent.
func (c *Conn) release(sb *sub) {
	c.mu.Lock()
	c.removeLocked(sb)
	c.mu.Unlock()
}

func (c *Conn) removeLocked(sb *sub) {
	if cur, ok := c.subs[sb.spec.Name]; ok && cur == sb {
		delete(c.subs, sb.spec.Name)
	}
	for i, o := range c.order {
		if o == sb {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
	for _, r := range sb.spec.Routes {
		list := c.routes[r]
		for i, o := range list {
			if o == sb {
				list = append(list[:i:i], list[i+1:]...)
				break
			}
		}
		if len(list) == 0 {
			delete(c.routes, r)
		} else {
			c.routes[r] = list
		}
	}
}

func (c *Conn) unsubscribe(sb *sub) error {
	_, err := c.retire(sb, nil, c.currentSession(), true)
	return err
}

// UnsubscribeName ends the subscription registered under name, exactly like
// closing its SubHandle. It returns nil when there is no such subscription.
func (c *Conn) UnsubscribeName(name string) error {
	c.mu.RLock()
	sb := c.subs[name]
	c.mu.RUnlock()
	if sb == nil {
		return nil
	}
	return c.unsubscribe(sb)
}

func isDeadSession(s *session) bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// deliverReply hands an ack/nack/error frame to the request waiting for it, or
// reports it when nobody is waiting.
func (c *Conn) deliverReply(in Inbound) {
	c.mu.RLock()
	ch := c.waiters[in.ID]
	c.mu.RUnlock()
	if ch != nil {
		select {
		case ch <- in:
		default:
		}
		return
	}
	if in.Kind == KindError {
		c.log.Warn("kucoin: ws server error", "protocol", c.proto.Name(), "error", in.Err)
		c.emit(stream.Event{Type: stream.EventServerError, Err: in.Err, Generation: c.gen.Load()})
		return
	}
	c.log.Debug("kucoin: ws unmatched reply", "protocol", c.proto.Name(), "id", in.ID)
}

// route queues a push on every subscription registered for its routing key.
// It never blocks the socket reader, and it never calls out (logger, event
// handler, unsubscribe) while holding the registry lock.
func (c *Conn) route(s *session, in Inbound, raw []byte, at time.Time) {
	frame := stream.Frame{Raw: raw, Route: in.Route, Msg: in.Msg, Generation: s.gen, ReceivedAt: at}
	var dropped, failed []*sub
	push := func(list []*sub) {
		for _, sb := range list {
			switch sb.mb.pushFrame(frame) {
			case pushDroppedOldest, pushDroppedNewest:
				dropped = append(dropped, sb)
			case pushOverflowFail:
				failed = append(failed, sb)
			}
		}
	}
	c.mu.RLock()
	// Only the live connection may feed subscriptions. A superseded session's
	// reader can still be draining its last frames after the reset marker was
	// queued; delivering them would put old-connection data behind the marker.
	if c.cur == s {
		push(c.routes[in.Route])
		if in.RouteAlt != "" {
			push(c.routes[in.RouteAlt])
		}
	}
	c.mu.RUnlock()

	for _, sb := range dropped {
		c.noteDrop(sb, s.gen)
	}
	for _, sb := range failed {
		c.pushesDropped.Add(1)
		sb.dropped.Add(1)
		err := fmt.Errorf("%w: %s", stream.ErrSlowConsumer, sb.spec.Name)
		c.emit(stream.Event{Type: stream.EventOverflow, Subscription: sb.spec.Name, Dropped: sb.dropped.Load(), Generation: s.gen, Err: err})
		if c.finishSub(sb, err) {
			// KuCoin keeps sending until told otherwise. The unsubscribe is written
			// off the reader goroutine, which must keep reading.
			if !c.goTracked(func() { _ = c.dispose(sb, s, false) }) {
				c.release(sb)
			}
		}
	}
}

func (c *Conn) noteDrop(sb *sub, gen uint64) {
	c.pushesDropped.Add(1)
	n := sb.dropped.Add(1)
	now := time.Now().UnixNano()
	last := sb.lastOverflow.Load()
	if now-last < int64(overflowEventEvery) || !sb.lastOverflow.CompareAndSwap(last, now) {
		return
	}
	c.log.Warn("kucoin: ws subscriber too slow, dropping updates", "subscription", sb.spec.Name, "dropped", n)
	c.emit(stream.Event{Type: stream.EventOverflow, Subscription: sb.spec.Name, Dropped: n, Generation: gen})
}

// restore makes ns the live session: every surviving subscription gets an
// ordered reset marker and is subscribed again on ns, then the Conn becomes
// Connected. A server rejection ends just that subscription, and so does a
// missing reply on a connection that is demonstrably alive; any other
// connection-level failure aborts the whole restore so the caller can retry.
func (c *Conn) restore(ctx context.Context, ns *session) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return stream.ErrClosed
	}
	subs := append([]*sub(nil), c.order...)
	c.cur = ns
	c.mu.Unlock()

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		failure error
		sem     = make(chan struct{}, restoreParallelism)
	)
	abort := func(err error) {
		errOnce.Do(func() {
			failure = err
			cancel()
		})
	}
	for _, sb := range subs {
		if sb.ended.Load() {
			continue
		}
		// The marker goes in before the frame, so it precedes anything the new
		// connection can deliver for this subscription.
		sb.mb.pushReset(ns.gen)
		select {
		case sem <- struct{}{}:
		case <-rctx.Done():
		}
		if rctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(sb *sub) {
			defer wg.Done()
			defer func() { <-sem }()
			c.resubscribe(rctx, ns, sb, abort)
		}(sb)
		time.Sleep(restoreGap)
	}
	wg.Wait()

	switch {
	case failure != nil:
		return failure
	case rctx.Err() != nil:
		if err := ctx.Err(); err != nil {
			return err
		}
		return stream.ErrClosed
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return stream.ErrClosed
	}
	c.reconnects++
	c.readyAt = time.Now()
	ns.readyAt = c.readyAt
	c.setStateLocked(stream.StateConnected)
	c.mu.Unlock()
	return nil
}

// resubscribe subscribes sb again on ns. abort receives a connection-level
// failure.
func (c *Conn) resubscribe(ctx context.Context, ns *session, sb *sub, abort func(error)) {
	id := newID()
	var wroteAt int64
	err := ns.request(ctx, id, sb.spec.Subscribe(id), func(werr error) {
		if werr == nil {
			wroteAt = time.Now().UnixNano()
			sb.sentGen.Store(ns.gen)
		}
	})
	switch {
	case err == nil:
		if sb.ended.Load() {
			// The subscription was closed while its request was in flight; the
			// closer found nothing to cancel yet.
			_ = c.dispose(sb, ns, false)
		}
	case isServerError(err):
		c.failResubscribe(ns, sb, err, false)
	case errors.Is(err, stream.ErrAckTimeout) && wroteAt != 0 && ns.lastRx.Load() > wroteAt:
		// Frames kept arriving, so the connection is alive and KuCoin just never
		// answered this subscription. It fails alone instead of holding every
		// other subscription's restore hostage through endless reconnects.
		c.failResubscribe(ns, sb, err, true)
	default:
		abort(err)
	}
}

// failResubscribe ends sb because its resubscribe failed. unsure says whether
// KuCoin may nevertheless hold the subscription, in which case it is cancelled.
func (c *Conn) failResubscribe(ns *session, sb *sub, err error, unsure bool) {
	c.log.Warn("kucoin: ws resubscribe failed", "subscription", sb.spec.Name, "error", err)
	wrapped := fmt.Errorf("kucoin: ws resubscribe %s: %w", sb.spec.Name, err)
	var ended bool
	if unsure {
		ended, _ = c.retire(sb, wrapped, ns, false)
	} else {
		ended = c.endSub(sb, wrapped)
	}
	if ended {
		c.emit(stream.Event{Type: stream.EventSubscriptionFailed, Subscription: sb.spec.Name, Err: wrapped, Generation: ns.gen})
	}
}
