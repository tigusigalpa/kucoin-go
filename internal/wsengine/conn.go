package wsengine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	mrand "math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tigusigalpa/kucoin-go/stream"
)

// minPingInterval is KuCoin's floor: a connection that pings more than once per
// second is dropped. It is a variable only so tests can run quickly.
var minPingInterval = time.Second

const (
	defaultPingInterval = 18 * time.Second
	defaultPingTimeout  = 10 * time.Second
	// readSlack is added to the heartbeat window to form the read deadline, the
	// last-resort guard when the heartbeat goroutine itself cannot run.
	readSlack = 5 * time.Second
	// closeGrace bounds the WebSocket close handshake write.
	closeGrace = time.Second
	// shutdownWait bounds Close.
	shutdownWait = 10 * time.Second
)

var closedEvents = func() chan stream.Event {
	ch := make(chan stream.Event)
	close(ch)
	return ch
}()

// Conn is one managed KuCoin WebSocket connection. It is safe for concurrent
// use. Create it with New, call Connect once, then Subscribe as needed.
type Conn struct {
	proto Protocol
	cfg   stream.Config
	log   stream.Logger

	mu         sync.RWMutex
	state      stream.State
	cur        *session
	subs       map[string]*sub
	order      []*sub
	routes     map[string][]*sub
	waiters    map[string]chan Inbound
	closed     bool // Close or a fatal error ended the current life
	fatal      error
	reconnects uint64
	readyAt    time.Time
	shutdown   chan struct{} // closed when the current life ends
	done       chan struct{} // closed when the current life reaches StateClosed
	stateCh    chan struct{} // closed and replaced on every state change

	wg  sync.WaitGroup
	gen atomic.Uint64

	evMu     sync.Mutex
	events   chan stream.Event
	evClosed bool

	frames        atomic.Uint64
	pushesDropped atomic.Uint64
	decodeErrors  atomic.Uint64
	lastFrame     atomic.Int64
}

// New creates a Conn for proto. Unset cfg fields take their defaults.
func New(proto Protocol, cfg stream.Config) *Conn {
	cfg = cfg.WithDefaults()
	c := &Conn{
		proto:   proto,
		cfg:     cfg,
		log:     cfg.Logger,
		subs:    make(map[string]*sub),
		routes:  make(map[string][]*sub),
		waiters: make(map[string]chan Inbound),
		stateCh: make(chan struct{}),
	}
	c.newLifeLocked()
	return c
}

// newLifeLocked prepares the per-life channels. The caller holds mu, or owns the
// Conn exclusively (New).
func (c *Conn) newLifeLocked() {
	c.closed = false
	c.fatal = nil
	c.shutdown = make(chan struct{})
	c.done = make(chan struct{})
	c.events = make(chan stream.Event, c.cfg.EventBuffer)
	c.evMu.Lock()
	c.evClosed = false
	c.evMu.Unlock()
	if c.cfg.OnEvent != nil {
		events, handler := c.events, c.cfg.OnEvent
		go func() { // not tracked by wg: it ends when events is closed
			for ev := range events {
				handler(ev)
			}
		}()
	}
}

func (c *Conn) setStateLocked(s stream.State) {
	c.state = s
	close(c.stateCh)
	c.stateCh = make(chan struct{})
}

// State returns the current lifecycle state.
func (c *Conn) State() stream.State {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// Events returns the lifecycle event channel of the current life. It is closed
// when the Conn closes. When a handler was configured (stream.WithEventHandler)
// the channel is already closed.
func (c *Conn) Events() <-chan stream.Event {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.cfg.OnEvent != nil {
		return closedEvents
	}
	return c.events
}

// Done is closed when the Conn reaches StateClosed. A later Connect starts a new
// life with a new Done channel.
func (c *Conn) Done() <-chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.done
}

// Err returns the fatal error that closed the Conn, or nil for a requested Close
// or while it is running.
func (c *Conn) Err() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.fatal
}

// Generation identifies the most recent connection; it increases with every new
// connection.
func (c *Conn) Generation() uint64 { return c.gen.Load() }

// Stats returns a snapshot of the connection counters.
func (c *Conn) Stats() stream.Stats {
	c.mu.RLock()
	st := stream.Stats{
		State:         c.state,
		Reconnects:    c.reconnects,
		Subscriptions: len(c.subs),
	}
	if c.state == stream.StateConnected {
		st.ConnectedSince = c.readyAt
	}
	c.mu.RUnlock()
	st.Generation = c.gen.Load()
	st.FramesReceived = c.frames.Load()
	st.PushesDropped = c.pushesDropped.Load()
	st.DecodeErrors = c.decodeErrors.Load()
	if ns := c.lastFrame.Load(); ns != 0 {
		st.LastFrame = time.Unix(0, ns)
	}
	return st
}

// closedErrLocked is the error for an operation on a closed Conn.
func (c *Conn) closedErrLocked() error {
	if c.fatal != nil {
		return fmt.Errorf("%w: %w", stream.ErrClosed, c.fatal)
	}
	return stream.ErrClosed
}

// goTracked runs fn on a goroutine counted by the shutdown wait group. It
// refuses once the Conn is closing, so Add can never race with Wait.
func (c *Conn) goTracked(fn func()) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	c.wg.Add(1)
	c.mu.Unlock()
	go func() {
		defer c.wg.Done()
		fn()
	}()
	return true
}

// Connect dials, waits for the welcome message, runs the protocol's
// authentication and starts the supervisor that keeps the connection alive. It
// fails with stream.ErrAlreadyConnected while a connection exists and with
// stream.ErrReconnecting while one is being restored. A Conn that was closed may
// be connected again; its previous subscriptions are gone.
func (c *Conn) Connect(ctx context.Context) error {
	c.mu.Lock()
	switch c.state {
	case stream.StateConnected, stream.StateConnecting:
		c.mu.Unlock()
		return stream.ErrAlreadyConnected
	case stream.StateReconnecting:
		c.mu.Unlock()
		return stream.ErrReconnecting
	case stream.StateClosing:
		c.mu.Unlock()
		return stream.ErrClosed
	case stream.StateClosed:
		c.newLifeLocked()
	}
	c.setStateLocked(stream.StateConnecting)
	shutdown := c.shutdown
	c.mu.Unlock()

	ctx, stop := withShutdown(ctx, shutdown)
	s, err := c.connectWithRetry(ctx, shutdown)
	stop()
	if err != nil {
		c.mu.Lock()
		if c.state == stream.StateConnecting {
			c.setStateLocked(stream.StateIdle)
		}
		closed := c.closed
		c.mu.Unlock()
		if closed && !errors.Is(err, stream.ErrClosed) {
			err = fmt.Errorf("%w: %w", stream.ErrClosed, err)
		}
		return err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		s.fail(stream.ErrClosed)
		return stream.ErrClosed
	}
	c.cur = s
	c.readyAt = time.Now()
	s.readyAt = c.readyAt
	c.setStateLocked(stream.StateConnected)
	c.wg.Add(1)
	c.mu.Unlock()
	go c.supervise(s, shutdown)

	c.log.Info("kucoin: ws connected", "protocol", c.proto.Name(), "generation", s.gen)
	c.emit(stream.Event{Type: stream.EventConnected, Generation: s.gen})
	return nil
}

func (c *Conn) connectWithRetry(ctx context.Context, shutdown <-chan struct{}) (*session, error) {
	var last error
	for attempt := 1; attempt <= c.cfg.InitialConnectAttempts; attempt++ {
		if attempt > 1 {
			if !sleepCtx(ctx, backoffDelay(c.cfg.Reconnect, attempt-1)) {
				return nil, ctx.Err()
			}
		}
		attemptCtx, cancel := context.WithTimeout(ctx, c.cfg.ConnectTimeout)
		s, err := c.dialSession(attemptCtx)
		cancel()
		if err == nil {
			return s, nil
		}
		last = err
		if stream.IsPermanent(err) || ctx.Err() != nil || isClosedChan(shutdown) {
			break
		}
		c.log.Warn("kucoin: ws connect failed", "protocol", c.proto.Name(), "attempt", attempt, "error", err)
	}
	return nil, last
}

// supervise watches the live session and, when it dies unexpectedly, runs the
// reconnect loop. It owns the lifetime of every connection after the first.
func (c *Conn) supervise(s *session, shutdown <-chan struct{}) {
	defer c.wg.Done()
	attempt := 0
	for {
		select {
		case <-s.done:
		case <-shutdown:
			return
		}
		if isClosedChan(shutdown) {
			return
		}

		cause := s.causeErr()
		giveUp := c.cfg.Reconnect.Disabled || stream.IsPermanent(cause)

		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return
		}
		if c.cur == s {
			c.cur = nil
		}
		uptime := time.Since(s.readyAt)
		if !giveUp {
			// Dropping the session and entering Reconnecting is one step: a
			// Subscribe must never observe Connected without a live session.
			c.setStateLocked(stream.StateReconnecting)
		}
		c.mu.Unlock()

		c.log.Warn("kucoin: ws disconnected", "protocol", c.proto.Name(), "error", cause, "uptime", uptime.Round(time.Millisecond))
		if uptime >= c.cfg.Reconnect.StableAfter {
			attempt = 0
		}
		c.emit(stream.Event{Type: stream.EventDisconnected, Err: cause, Generation: s.gen})
		if giveUp {
			c.fatalClose(cause)
			return
		}

		next, ok := c.reconnect(&attempt, cause, shutdown)
		if !ok {
			return
		}
		s = next
	}
}

// reconnect retries until a session is established and its subscriptions are
// restored. It returns false when the Conn was closed or gave up.
func (c *Conn) reconnect(attempt *int, cause error, shutdown <-chan struct{}) (*session, bool) {
	policy := c.cfg.Reconnect
	last := cause
	// failed counts this outage's failed attempts. *attempt, which sets the
	// backoff, also carries over from earlier outages that were not stable for
	// long, but those must not use up this outage's budget: a connection that
	// came back and dropped again deserves its attempts.
	failed := 0
	for {
		if policy.MaxAttempts > 0 && failed >= policy.MaxAttempts {
			c.fatalClose(fmt.Errorf("kucoin: ws: giving up after %d reconnect attempts: %w", failed, last))
			return nil, false
		}
		*attempt++
		delay := backoffDelay(policy, *attempt)
		c.emit(stream.Event{Type: stream.EventReconnecting, Attempt: *attempt, Backoff: delay, Err: last})
		if !sleepShutdown(shutdown, delay) {
			return nil, false
		}

		ctx, stop := withShutdown(context.Background(), shutdown)
		attemptCtx, cancel := context.WithTimeout(ctx, c.cfg.ConnectTimeout)
		s, err := c.dialSession(attemptCtx)
		cancel()
		if err == nil {
			err = c.restore(ctx, s)
		}
		stop()
		if err != nil {
			if s != nil {
				s.fail(err)
			}
			if isClosedChan(shutdown) {
				return nil, false
			}
			last = err
			failed++
			c.log.Warn("kucoin: ws reconnect failed", "protocol", c.proto.Name(), "attempt", *attempt, "error", err)
			if stream.IsPermanent(err) {
				c.fatalClose(err)
				return nil, false
			}
			continue
		}
		c.log.Info("kucoin: ws reconnected", "protocol", c.proto.Name(), "generation", s.gen, "attempt", *attempt)
		c.emit(stream.Event{Type: stream.EventReconnected, Generation: s.gen, Attempt: *attempt})
		return s, true
	}
}

// fatalClose ends the Conn after an unrecoverable error: every subscription ends
// with err and the Conn reaches StateClosed.
func (c *Conn) fatalClose(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.fatal = err
	c.setStateLocked(stream.StateClosing)
	close(c.shutdown)
	s := c.cur
	c.cur = nil
	subs := c.takeSubsLocked()
	c.mu.Unlock()

	for _, sb := range subs {
		c.abortSub(sb, err)
	}
	if s != nil {
		s.fail(err)
	}
	c.finish(err)
}

// Close shuts the connection down gracefully; see Shutdown.
func (c *Conn) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownWait)
	defer cancel()
	return c.Shutdown(ctx)
}

// Shutdown stops reconnecting, ends every subscription (their channels close),
// sends a WebSocket close frame, closes the socket and waits until all of the
// connection's goroutines have exited or ctx expires. It is idempotent.
func (c *Conn) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	first := !c.closed
	done := c.done
	var s *session
	var subs []*sub
	if first {
		c.closed = true
		c.setStateLocked(stream.StateClosing)
		close(c.shutdown)
		s = c.cur
		c.cur = nil
		subs = c.takeSubsLocked()
	}
	c.mu.Unlock()

	for _, sb := range subs {
		c.abortSub(sb, nil)
	}
	if s != nil {
		s.closeGraceful()
	}

	waited := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(waited)
	}()
	var err error
	select {
	case <-waited:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if first {
		c.finish(nil)
	} else if err == nil {
		// Another call is closing the Conn: return only once it is closed.
		select {
		case <-done:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	return err
}

// finish publishes the terminal event and moves the Conn to StateClosed. The
// state stays StateClosing until the event stream is closed: a Connect that
// started a new life earlier would get this life's terminal event and have its
// fresh event channel closed under it.
func (c *Conn) finish(cause error) {
	c.mu.Lock()
	if c.state == stream.StateClosed {
		c.mu.Unlock()
		return
	}
	c.fatal = cause
	done := c.done
	c.mu.Unlock()

	c.emit(stream.Event{Type: stream.EventClosed, Err: cause, Generation: c.gen.Load()})
	c.closeEvents()

	c.mu.Lock()
	c.setStateLocked(stream.StateClosed)
	close(done)
	c.mu.Unlock()
}

// takeSubsLocked detaches every subscription from the registry.
func (c *Conn) takeSubsLocked() []*sub {
	subs := c.order
	c.order = nil
	c.subs = make(map[string]*sub)
	c.routes = make(map[string][]*sub)
	return subs
}

// emit publishes a lifecycle event without ever blocking: when the buffer is
// full the oldest event is discarded.
func (c *Conn) emit(ev stream.Event) {
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	c.evMu.Lock()
	defer c.evMu.Unlock()
	if c.evClosed {
		return
	}
	select {
	case c.events <- ev:
		return
	default:
	}
	select {
	case <-c.events:
	default:
	}
	select {
	case c.events <- ev:
	default:
	}
}

func (c *Conn) closeEvents() {
	c.evMu.Lock()
	defer c.evMu.Unlock()
	if !c.evClosed {
		c.evClosed = true
		close(c.events)
	}
}

// ReportDecodeError records a push that could not be decoded. The stream keeps
// running; the error is counted and published as an EventDecodeError.
func (c *Conn) ReportDecodeError(err error) {
	c.decodeErrors.Add(1)
	ev := stream.Event{Type: stream.EventDecodeError, Err: err, Generation: c.gen.Load()}
	var de *stream.DecodeError
	if errors.As(err, &de) {
		ev.Subscription = de.Channel
	}
	c.log.Warn("kucoin: ws decode failed", "error", err)
	c.emit(ev)
}

// backoffDelay returns the jittered delay before reconnect attempt n (1-based).
func backoffDelay(p stream.ReconnectPolicy, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(p.MinDelay) * math.Pow(p.Factor, float64(attempt-1))
	if max := float64(p.MaxDelay); d > max || math.IsInf(d, 0) || math.IsNaN(d) {
		d = max
	}
	d -= d * p.Jitter * mrand.Float64() //nolint:gosec // jitter does not need a CSPRNG
	return time.Duration(d)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func sleepShutdown(shutdown <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-shutdown:
		return false
	}
}

func isClosedChan(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// withShutdown returns a context that is cancelled when parent is, or when
// shutdown is closed. The caller must call the returned cancel function.
func withShutdown(parent context.Context, shutdown <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-shutdown:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// newID returns a random request ID. It only uses characters KuCoin accepts in
// public-channel IDs and stays well below their 40-character limit.
func newID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		binaryFallback(b[:])
	}
	return hex.EncodeToString(b[:])
}

func binaryFallback(b []byte) {
	n := uint64(time.Now().UnixNano())
	for i := range b {
		b[i] = byte(n >> (8 * uint(i%8)))
	}
}

// compile-time check that session satisfies Requester.
var _ Requester = (*session)(nil)

// SetMinPingInterval lowers the one-ping-per-second floor so that other
// packages' tests can exercise heartbeats at millisecond scale. It must only be
// called from test initialisation, before any connection is created.
func SetMinPingInterval(d time.Duration) { minPingInterval = d }
