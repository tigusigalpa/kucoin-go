package stream

import (
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors. Match them with errors.Is; the errors returned by this
// module's WebSocket clients wrap them with additional context.
var (
	// ErrClosed is returned by operations on a client that was closed, and is
	// the Err of a subscription ended by Close.
	ErrClosed = errors.New("kucoin: ws: client closed")
	// ErrNotConnected is returned when an operation needs a live connection
	// and the client has none.
	ErrNotConnected = errors.New("kucoin: ws: not connected")
	// ErrAlreadyConnected is returned by Connect on a client that already owns
	// a live connection. One client owns exactly one WebSocket connection.
	ErrAlreadyConnected = errors.New("kucoin: ws: already connected")
	// ErrReconnecting is returned by Connect while the client is restoring an
	// interrupted connection on its own.
	ErrReconnecting = errors.New("kucoin: ws: reconnect in progress")
	// ErrAlreadySubscribed is returned when a subscription with the same
	// identity (topic or channel parameters) is already active on the
	// connection.
	ErrAlreadySubscribed = errors.New("kucoin: ws: already subscribed")
	// ErrWelcomeTimeout is returned when KuCoin did not send its welcome
	// message in time.
	ErrWelcomeTimeout = errors.New("kucoin: ws: timed out waiting for welcome message")
	// ErrAckTimeout is returned when KuCoin did not acknowledge a request in
	// time.
	ErrAckTimeout = errors.New("kucoin: ws: timed out waiting for acknowledgement")
	// ErrPingTimeout is the cause reported when KuCoin did not answer a
	// heartbeat in time and the connection was considered dead.
	ErrPingTimeout = errors.New("kucoin: ws: no response within the ping timeout")
	// ErrAuthFailed is returned when KuCoin rejects the signed authentication
	// of a private connection.
	ErrAuthFailed = errors.New("kucoin: ws: authentication failed")
	// ErrIncompleteCredentials is returned before any frame is written when a
	// private connection is configured without a complete key set.
	ErrIncompleteCredentials = errors.New("kucoin: ws: complete API credentials are required")
	// ErrSlowConsumer ends a subscription whose consumer could not keep up when
	// the OverflowPolicy is FailSubscription.
	ErrSlowConsumer = errors.New("kucoin: ws: subscriber too slow")
	// ErrTokenUnavailable wraps a failure to obtain the connection token or
	// endpoint needed to (re)connect.
	ErrTokenUnavailable = errors.New("kucoin: ws: could not obtain a connection token")
	// ErrResyncFailed is the cause reported when a sequenced stream (an order
	// book) could not be resynchronised.
	ErrResyncFailed = errors.New("kucoin: ws: resynchronisation failed")

	// ErrTopicInvalid corresponds to KuCoin code 400 on subscribe.
	ErrTopicInvalid = errors.New("kucoin: ws: topic is invalid")
	// ErrLoginRequired corresponds to KuCoin code 403 on subscribe: a private
	// topic needs a private connection.
	ErrLoginRequired = errors.New("kucoin: ws: login is required")
	// ErrTopicNotFound corresponds to KuCoin code 404 on subscribe.
	ErrTopicNotFound = errors.New("kucoin: ws: topic does not exist")
	// ErrTopicRequired corresponds to KuCoin code 406 on subscribe.
	ErrTopicRequired = errors.New("kucoin: ws: topic is required")
	// ErrTokenInvalid corresponds to KuCoin code 401 on connect.
	ErrTokenInvalid = errors.New("kucoin: ws: token is invalid")
	// ErrSubscriptionLimit corresponds to KuCoin code 509 "exceed max
	// subscription count limitation" (100 topics per request, 300 per
	// session).
	ErrSubscriptionLimit = errors.New("kucoin: ws: subscription limit exceeded")
	// ErrSessionLimit corresponds to KuCoin code 509 "exceed max session count
	// limitation of 50".
	ErrSessionLimit = errors.New("kucoin: ws: session limit exceeded")
	// ErrRateLimited corresponds to KuCoin code 509 "exceed max permits per
	// second" and to UTA gateway codes 429xxx.
	ErrRateLimited = errors.New("kucoin: ws: request rate limit exceeded")
	// ErrServiceBusy corresponds to KuCoin code 509 "service busy" and to the
	// UTA gateway codes 503000/504000.
	ErrServiceBusy = errors.New("kucoin: ws: service busy")
	// ErrBadCommand corresponds to KuCoin code 415 "command type is invalid".
	ErrBadCommand = errors.New("kucoin: ws: command type is invalid")
)

// ServerError is an error reported by KuCoin itself: a negative reply to a
// subscribe, unsubscribe or authentication request, or an unsolicited error
// frame. Use errors.As to read the code and message and errors.Is with the
// sentinel errors (ErrTopicNotFound, ErrSubscriptionLimit, ...) to classify
// it.
type ServerError struct {
	// Code is the numeric error code KuCoin reported (404, 509, 400003, ...);
	// zero when KuCoin reported only a message.
	Code int
	// Message is the human-readable text KuCoin reported.
	Message string
	// ID is the request ID the error answers, when known.
	ID string
}

// Error implements error.
func (e *ServerError) Error() string {
	switch {
	case e.Code != 0 && e.Message != "":
		return fmt.Sprintf("kucoin: ws: server error %d: %s", e.Code, e.Message)
	case e.Code != 0:
		return fmt.Sprintf("kucoin: ws: server error %d", e.Code)
	case e.Message != "":
		return "kucoin: ws: server error: " + e.Message
	default:
		return "kucoin: ws: server error"
	}
}

// Is lets errors.Is match a *ServerError against the sentinel errors above.
func (e *ServerError) Is(target error) bool {
	if e == nil {
		return false
	}
	return target == e.sentinel()
}

func (e *ServerError) sentinel() error {
	msg := strings.ToLower(e.Message)
	switch e.Code {
	case 400:
		if strings.Contains(msg, "ping timeout") {
			return ErrPingTimeout
		}
		return ErrTopicInvalid
	case 401:
		return ErrTokenInvalid
	case 403:
		return ErrLoginRequired
	case 404:
		return ErrTopicNotFound
	case 406:
		return ErrTopicRequired
	case 415:
		return ErrBadCommand
	case 509:
		switch {
		case strings.Contains(msg, "subscription"):
			return ErrSubscriptionLimit
		case strings.Contains(msg, "session"):
			return ErrSessionLimit
		case strings.Contains(msg, "permit"):
			return ErrRateLimited
		case strings.Contains(msg, "busy"):
			return ErrServiceBusy
		}
	case 429000, 429001, 429002:
		return ErrRateLimited
	case 503000, 504000:
		return ErrServiceBusy
	case 400002, 400003, 400004, 400005, 400007, 400008, 400009, 400010, 400011, 400012:
		return ErrAuthFailed
	}
	if e.Code == 0 && strings.Contains(msg, "auth") {
		return ErrAuthFailed
	}
	return nil
}

// DecodeError reports that an inbound push could not be decoded into the typed
// payload of its channel. It never ends a subscription; it is delivered as an
// EventDecodeError.
type DecodeError struct {
	// Channel is the topic or channel the push belongs to.
	Channel string
	// Raw is the offending frame.
	Raw []byte
	// Err is the underlying decoding error.
	Err error
}

// Error implements error.
func (e *DecodeError) Error() string {
	return fmt.Sprintf("kucoin: ws: decode %s push: %v", e.Channel, e.Err)
}

// Unwrap returns the underlying error.
func (e *DecodeError) Unwrap() error { return e.Err }

// permanentError marks an error that retrying cannot fix.
type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent wraps err so that the reconnect loop gives up instead of retrying.
// Use it for failures a retry cannot fix, such as missing credentials or a
// rejected API key. A nil err yields nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

// IsPermanent reports whether err, or an error it wraps, was marked with
// Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}
