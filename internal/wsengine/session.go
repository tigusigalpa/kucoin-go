package wsengine

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/tigusigalpa/kucoin-go/stream"
)

// session is one WebSocket connection ("generation"). Its goroutines — a reader
// and a heartbeat — end as soon as done is closed, which fail does exactly once.
type session struct {
	c    *Conn
	conn *websocket.Conn
	gen  uint64

	wmu sync.Mutex // gorilla permits a single concurrent writer

	done     chan struct{}
	failOnce sync.Once
	cause    error // set before done is closed

	welcomed    chan struct{}
	welcomeOnce sync.Once
	welcomeIn   atomic.Pointer[Inbound]

	lastRx       atomic.Int64 // unix nanos of the last inbound frame
	pingInterval atomic.Int64 // nanoseconds
	pingTimeout  atomic.Int64
	readyAt      time.Time

	// lim paces requests to the server's message limit; nil when it has none.
	lim *sendWindow
}

func (c *Conn) newSession(conn *websocket.Conn) *session {
	s := &session{
		c:        c,
		conn:     conn,
		gen:      c.gen.Add(1),
		done:     make(chan struct{}),
		welcomed: make(chan struct{}),
	}
	s.lastRx.Store(time.Now().UnixNano())
	s.pingInterval.Store(int64(defaultPingInterval))
	s.pingTimeout.Store(int64(defaultPingTimeout))
	s.lim = c.messageBudget()
	return s
}

// messageBudget returns the pacing of one connection's requests: what the
// configuration says, else what the protocol declares, else none.
func (c *Conn) messageBudget() *sendWindow {
	limit, window := 0, time.Duration(0)
	if l, ok := c.proto.(MessageLimiter); ok {
		limit, window = l.MessageLimit()
	}
	if o := c.cfg.MessageLimit; o.Unlimited {
		return nil
	} else if o.Messages > 0 && o.Window > 0 {
		limit, window = o.Messages, o.Window
	}
	return newSendWindow(limit, window)
}

// fail marks the session dead with cause (only the first call counts), wakes
// everything waiting on it and closes the socket, which unblocks the reader.
func (s *session) fail(cause error) {
	s.failOnce.Do(func() {
		s.cause = cause
		close(s.done)
		_ = s.conn.Close()
	})
}

// causeErr returns why the session ended; it must only be called after done is
// closed.
func (s *session) causeErr() error {
	<-s.done
	return s.cause
}

// closeGraceful sends a normal-closure close frame, then drops the socket.
func (s *session) closeGraceful() {
	_ = s.conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(closeGrace))
	s.fail(stream.ErrClosed)
}

// write sends one text frame, serialised with every other writer and bounded by
// the write timeout. A failed write kills the session.
func (s *session) write(frame []byte) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	select {
	case <-s.done:
		return s.cause
	default:
	}
	_ = s.conn.SetWriteDeadline(time.Now().Add(s.c.cfg.WriteTimeout))
	if err := s.conn.WriteMessage(websocket.TextMessage, frame); err != nil {
		err = fmt.Errorf("kucoin: ws write: %w", err)
		s.fail(err)
		return err
	}
	return nil
}

// send writes a request frame once the server's message limit allows it. The
// wait ends early with the error of ctx, or with the cause of the session's death.
func (s *session) send(ctx context.Context, frame []byte) error {
	if err := s.lim.reserve(ctx, s.done, s.causeErr); err != nil {
		return err
	}
	return s.write(frame)
}

// Request implements Requester.
func (s *session) Request(ctx context.Context, id string, frame []byte) error {
	return s.request(ctx, id, frame, nil)
}

// request sends frame and waits for the reply to id. afterWrite, when set, is
// called exactly once as soon as the write has finished, successfully or not and
// before the wait for the reply, with the write's error.
func (s *session) request(ctx context.Context, id string, frame []byte, afterWrite func(error)) error {
	c := s.c
	reply := make(chan Inbound, 1)
	c.mu.Lock()
	c.waiters[id] = reply
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.waiters, id)
		c.mu.Unlock()
	}()

	err := s.send(ctx, frame)
	if afterWrite != nil {
		afterWrite(err)
	}
	if err != nil {
		return err
	}
	timer := time.NewTimer(c.cfg.AckTimeout)
	defer timer.Stop()
	select {
	case in := <-reply:
		if in.Kind == KindNack || in.Kind == KindError {
			if in.Err != nil {
				return in.Err
			}
			return &stream.ServerError{ID: id, Message: "request rejected"}
		}
		return nil
	case <-timer.C:
		return fmt.Errorf("%w (request %s)", stream.ErrAckTimeout, id)
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return s.cause
	}
}

// readLoop reads frames until the socket fails, classifying and dispatching
// each. It is the only goroutine that reads from the socket.
func (s *session) readLoop() {
	c := s.c
	for {
		_, raw, err := s.conn.ReadMessage()
		if err != nil {
			s.fail(classifyReadError(err))
			return
		}
		now := time.Now()
		s.lastRx.Store(now.UnixNano())
		c.lastFrame.Store(now.UnixNano())
		c.frames.Add(1)
		_ = s.conn.SetReadDeadline(now.Add(s.readWindow()))

		in := c.proto.Classify(raw)
		switch in.Kind {
		case KindWelcome:
			s.welcomeOnce.Do(func() {
				s.welcomeIn.Store(&in)
				close(s.welcomed)
			})
		case KindPong:
			// Liveness is tracked by lastRx for every frame; nothing else to do.
		case KindAck, KindNack, KindError:
			c.deliverReply(in)
		case KindPush:
			c.route(s, in, raw, now)
		default:
			c.log.Debug("kucoin: ws ignored frame", "protocol", c.proto.Name(), "bytes", len(raw))
		}
	}
}

// readWindow is how long the socket may stay silent before the read deadline
// kills it; the heartbeat normally detects a dead peer much sooner.
func (s *session) readWindow() time.Duration {
	return time.Duration(s.pingInterval.Load()) + time.Duration(s.pingTimeout.Load()) + readSlack
}

func classifyReadError(err error) error {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return fmt.Errorf("%w: no frame received before the read deadline: %w", stream.ErrPingTimeout, err)
	}
	return fmt.Errorf("kucoin: ws connection lost: %w", err)
}

// startHeartbeat resolves the effective heartbeat parameters and starts the ping
// goroutine.
func (s *session) startHeartbeat(ep Endpoint) {
	var wi, wt time.Duration
	if w := s.welcomeIn.Load(); w != nil {
		wi, wt = w.PingInterval, w.PingTimeout
	}
	send, timeout := resolveHeartbeat(s.c.cfg, ep, wi, wt)
	s.pingInterval.Store(int64(send))
	s.pingTimeout.Store(int64(timeout))
	if !s.c.goTracked(s.pingLoop) {
		s.fail(stream.ErrClosed)
	}
}

// resolveHeartbeat returns how often to ping and how long to wait for any sign of
// life afterwards. Precedence: explicit configuration, then the welcome frame
// (wi, wt), then the endpoint (token) response, then KuCoin's documented
// defaults of 18s and 10s.
func resolveHeartbeat(cfg stream.Config, ep Endpoint, wi, wt time.Duration) (send, timeout time.Duration) {
	advertised := firstPositive(cfg.PingInterval, wi, ep.PingInterval, defaultPingInterval)
	timeout = firstPositive(cfg.PingTimeout, wt, ep.PingTimeout, defaultPingTimeout)
	send = advertised
	if cfg.PingInterval == 0 {
		// KuCoin only requires one message per advertised interval, but a timer
		// that fires exactly on the limit loses to scheduling and GC jitter, so
		// ping at half of it. An explicit override is honoured as given.
		send = advertised / 2
	}
	if send < minPingInterval {
		send = minPingInterval
	}
	return send, timeout
}

func firstPositive(ds ...time.Duration) time.Duration {
	for _, d := range ds {
		if d > 0 {
			return d
		}
	}
	return 0
}

// pingLoop sends a heartbeat every ping interval and declares the connection
// dead when nothing at all — a pong or any other frame — arrives within the ping
// timeout after a ping was sent.
func (s *session) pingLoop() {
	interval := time.Duration(s.pingInterval.Load())
	timeout := time.Duration(s.pingTimeout.Load())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var (
		watch   *time.Timer
		watchC  <-chan time.Time
		pending int64 // unix nanos of the ping being watched
	)
	defer func() {
		if watch != nil {
			watch.Stop()
		}
	}()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			sentAt := time.Now().UnixNano()
			s.lim.note() // counted by the server, never delayed
			if err := s.write(s.c.proto.Ping(newID())); err != nil {
				return
			}
			if watchC == nil {
				pending = sentAt
				if watch == nil {
					watch = time.NewTimer(timeout)
				} else {
					watch.Reset(timeout)
				}
				watchC = watch.C
			}
		case <-watchC:
			watchC = nil
			if s.lastRx.Load() < pending {
				s.fail(fmt.Errorf("%w: nothing received within %s of a ping", stream.ErrPingTimeout, timeout))
				return
			}
		}
	}
}

// dialSession obtains an endpoint, dials it, waits for the welcome frame, starts
// the heartbeat and runs the protocol's post-welcome step. On success the session
// is live but not yet registered as the Conn's current session.
func (c *Conn) dialSession(ctx context.Context) (*session, error) {
	ep, err := c.proto.Endpoint(ctx)
	if err != nil {
		wrapped := fmt.Errorf("%w: %w", stream.ErrTokenUnavailable, err)
		if stream.IsPermanent(err) {
			wrapped = stream.Permanent(wrapped)
		}
		return nil, wrapped
	}
	conn, resp, err := c.cfg.Dialer.DialContext(ctx, ep.URL, c.cfg.Header)
	if err != nil {
		return nil, dialError(err, resp)
	}
	conn.SetReadLimit(c.cfg.ReadLimit)
	_ = conn.SetReadDeadline(time.Now().Add(c.cfg.ConnectTimeout + readSlack))

	s := c.newSession(conn)
	if !c.goTracked(s.readLoop) {
		_ = conn.Close()
		return nil, stream.ErrClosed
	}

	select {
	case <-s.welcomed:
	case <-s.done:
		return nil, s.cause
	case <-ctx.Done():
		err := ctx.Err()
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("%w: %w", stream.ErrWelcomeTimeout, err)
		}
		s.fail(err)
		return nil, err
	}

	s.startHeartbeat(ep)
	if err := c.proto.Welcomed(ctx, s); err != nil {
		s.fail(err)
		return nil, err
	}
	return s, nil
}

// dialError turns a failed WebSocket handshake into a typed error.
func dialError(err error, resp *http.Response) error {
	if resp != nil {
		switch resp.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return fmt.Errorf("kucoin: ws dial: %w (http %d): %w", stream.ErrTokenInvalid, resp.StatusCode, err)
		case http.StatusTooManyRequests:
			return fmt.Errorf("kucoin: ws dial: %w (http %d): %w", stream.ErrRateLimited, resp.StatusCode, err)
		case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout:
			return fmt.Errorf("kucoin: ws dial: %w (http %d): %w", stream.ErrServiceBusy, resp.StatusCode, err)
		}
		return fmt.Errorf("kucoin: ws dial (http %d): %w", resp.StatusCode, err)
	}
	return fmt.Errorf("kucoin: ws dial: %w", err)
}
