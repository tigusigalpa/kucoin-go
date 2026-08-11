// Package transport implements the shared HTTP request executor, response
// metadata capture, and error hierarchy used by every KuCoin service.
package transport

import (
	"errors"
	"fmt"
)

// KucoinError is a structured error from a KuCoin REST response,
// preserving the exact business code/message KuCoin returned alongside
// the HTTP status and raw body.
//
// Docs: https://www.kucoin.com/docs-new/error-code/http
type KucoinError struct {
	HTTPStatus int
	Code       string
	Message    string
	RequestID  string
	Raw        []byte
}

func (e *KucoinError) Error() string {
	return fmt.Sprintf("kucoin: api error: http=%d code=%s message=%s", e.HTTPStatus, e.Code, e.Message)
}

// Sentinel errors, mapped from HTTP status per KuCoin's documented HTTP
// error-code catalog (business-level per-domain codes vary by service —
// inspect the wrapped *KucoinError's Code/Message for those). Match with
// errors.Is.
//
// Docs: https://www.kucoin.com/docs-new/error-code/http
var (
	ErrBadRequest         = errors.New("kucoin: bad request: invalid request format")
	ErrUnauthorized       = errors.New("kucoin: unauthorized: invalid API key/signature/passphrase/timestamp")
	ErrForbiddenOrLimited = errors.New("kucoin: forbidden or rate limited")
	ErrNotFound           = errors.New("kucoin: resource not found")
	ErrMethodNotAllowed   = errors.New("kucoin: method not allowed")
	ErrUnsupportedMedia   = errors.New("kucoin: unsupported media type")
	ErrServerError        = errors.New("kucoin: internal server error")
	ErrServiceUnavailable = errors.New("kucoin: service unavailable")
	ErrRateLimited        = errors.New("kucoin: rate limit exceeded")
	ErrResponseTooLarge   = errors.New("kucoin: response body exceeds size limit")
)

// MapHTTPStatus maps an HTTP status code to a sentinel error, per KuCoin's
// documented HTTP error-code catalog. Returns nil for 2xx.
//
// Docs: https://www.kucoin.com/docs-new/error-code/http
func MapHTTPStatus(status int) error {
	switch status {
	case 200, 201, 202, 204:
		return nil
	case 400:
		return ErrBadRequest
	case 401:
		return ErrUnauthorized
	case 403:
		return ErrForbiddenOrLimited
	case 404:
		return ErrNotFound
	case 405:
		return ErrMethodNotAllowed
	case 415:
		return ErrUnsupportedMedia
	case 429:
		return ErrRateLimited
	case 500:
		return ErrServerError
	case 503:
		return ErrServiceUnavailable
	default:
		if status >= 500 {
			return ErrServerError
		}
		if status >= 400 {
			return ErrBadRequest
		}
		return nil
	}
}
