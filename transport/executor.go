package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tigusigalpa/kucoin-go/auth"
)

// successCode is KuCoin's business-level success code, consistent across
// both the Classic and UTA REST APIs.
//
// Docs: https://www.kucoin.com/docs-new/error-code/http
const (
	successCode          = "200000"
	maxResponseBodyBytes = 10 << 20
)

// ErrCredentialsRequired is returned locally (no network call is made)
// when a private endpoint is called without complete API credentials.
var ErrCredentialsRequired = errors.New("kucoin: this endpoint requires complete API credentials")

// Executor is the shared HTTP transport for one REST base URL. It signs
// private requests, decodes KuCoin's response envelope, captures response
// metadata, and retries idempotent (GET) calls per its RetryPolicy.
type Executor struct {
	cfg    ExecutorConfig
	signer *auth.Signer
}

// NewExecutor creates an Executor. Unset fields in cfg fall back to safe
// defaults (SystemClock, NoopLogger, NewDefaultRetryPolicy, &http.Client{}).
func NewExecutor(cfg ExecutorConfig) *Executor {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 15 * time.Second}
	}
	if cfg.Clock == nil {
		cfg.Clock = SystemClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = NoopLogger{}
	}
	if cfg.RetryPolicy == nil {
		cfg.RetryPolicy = NewDefaultRetryPolicy()
	}
	if cfg.SiteType == "" {
		cfg.SiteType = SiteTypeGlobal
	}
	return &Executor{cfg: cfg, signer: auth.NewSigner(cfg.Credentials.APISecret)}
}

// DoPublic issues an unauthenticated request. No KC-API-* headers are
// sent.
func (e *Executor) DoPublic(ctx context.Context, method, path string, query map[string]string, result interface{}) (*ResponseMeta, error) {
	return e.do(ctx, method, path, query, nil, false, result)
}

// Do issues an authenticated request, signing it with the configured
// credentials. Returns ErrCredentialsRequired locally (no network call)
// if complete credentials were not configured.
func (e *Executor) Do(ctx context.Context, method, path string, query map[string]string, body interface{}, result interface{}) (*ResponseMeta, error) {
	if !e.cfg.Credentials.isComplete() {
		return nil, ErrCredentialsRequired
	}
	return e.do(ctx, method, path, query, body, true, result)
}

func (e *Executor) do(ctx context.Context, method, path string, query map[string]string, body interface{}, signed bool, result interface{}) (*ResponseMeta, error) {
	endpoint := path
	if len(query) > 0 {
		values := url.Values{}
		for k, v := range query {
			if v != "" {
				values.Set(k, v)
			}
		}
		if encoded := values.Encode(); encoded != "" {
			endpoint += "?" + encoded
		}
	}

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("kucoin: marshal request body: %w", err)
		}
	}

	retryable := strings.EqualFold(method, http.MethodGet)
	policy := e.cfg.RetryPolicy
	maxAttempts := 1
	if retryable && policy != nil && policy.MaxAttempts > 1 {
		maxAttempts = policy.MaxAttempts
	}

	start := e.cfg.Clock.Now()
	var lastErr error
	var lastMeta *ResponseMeta

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			delay := policy.delayForAttempt(attempt - 1)
			if policy.MaxElapsed > 0 && e.cfg.Clock.Now().Sub(start)+delay > policy.MaxElapsed {
				break
			}
			if policy.OnRetry != nil {
				policy.OnRetry(attempt-1, delay, lastErr)
			}
			select {
			case <-ctx.Done():
				return lastMeta, ctx.Err()
			case <-time.After(delay):
			}
		}

		meta, err := e.attempt(ctx, method, endpoint, bodyBytes, signed, result)
		if meta != nil {
			meta.Attempts = attempt
		}
		lastMeta, lastErr = meta, err
		if err == nil {
			return meta, nil
		}
		if !retryable || !isRetryable(meta, err) {
			return meta, err
		}
	}

	return lastMeta, lastErr
}

func isRetryable(meta *ResponseMeta, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	if meta == nil {
		return isRetryableNetworkError(err)
	}

	return meta.HTTPStatus == http.StatusTooManyRequests || meta.HTTPStatus >= 500
}

func isRetryableNetworkError(err error) bool {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return true
		}
		if isRetryableNetworkCause(urlErr.Err) {
			return true
		}
		return false
	}

	return isRetryableNetworkCause(err)
}

func isRetryableNetworkCause(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
		var temporary interface{ Temporary() bool }
		if errors.As(err, &temporary) && temporary.Temporary() {
			return true
		}
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	return false
}

func (e *Executor) attempt(ctx context.Context, method, endpoint string, bodyBytes []byte, signed bool, result interface{}) (*ResponseMeta, error) {
	fullURL := e.cfg.BaseURL + endpoint

	var reader io.Reader
	if len(bodyBytes) > 0 {
		reader = bytes.NewReader(bodyBytes)
	}

	req, err := http.NewRequestWithContext(ctx, strings.ToUpper(method), fullURL, reader)
	if err != nil {
		return nil, fmt.Errorf("kucoin: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if e.cfg.SiteType != "" {
		req.Header.Set("X-SITE-TYPE", e.cfg.SiteType)
	}
	if e.cfg.EnableNS {
		req.Header.Set("kc-enable-ns", "true")
	}

	if signed {
		timestamp := auth.TimestampMillis(e.cfg.Clock.Now())
		bodyForSign := ""
		if len(bodyBytes) > 0 {
			bodyForSign = string(bodyBytes)
		}
		signature := e.signer.Sign(timestamp, method, endpoint, bodyForSign)
		req.Header.Set("KC-API-KEY", e.cfg.Credentials.APIKey)
		req.Header.Set("KC-API-SIGN", signature)
		req.Header.Set("KC-API-TIMESTAMP", timestamp)
		req.Header.Set("KC-API-PASSPHRASE", e.signer.SignPassphrase(e.cfg.Credentials.APIPassphrase))
		if e.cfg.Credentials.APIKeyVersion != "" {
			req.Header.Set("KC-API-KEY-VERSION", e.cfg.Credentials.APIKeyVersion)
		}
	}

	e.cfg.Logger.Debug("kucoin: request", "method", method, "path", endpoint, "signed", signed)

	resp, err := e.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kucoin: do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("kucoin: read response body: %w", err)
	}
	if len(respBody) > maxResponseBodyBytes {
		return nil, fmt.Errorf("%w (%d bytes)", ErrResponseTooLarge, maxResponseBodyBytes)
	}

	meta := &ResponseMeta{
		HTTPStatus:         resp.StatusCode,
		RawBody:            respBody,
		RequestID:          resp.Header.Get("X-Request-Id"),
		RateLimitLimit:     resp.Header.Get("gw-ratelimit-limit"),
		RateLimitRemaining: resp.Header.Get("gw-ratelimit-remaining"),
		RateLimitReset:     resp.Header.Get("gw-ratelimit-reset"),
		InTime:             resp.Header.Get("x-in-time"),
		OutTime:            resp.Header.Get("x-out-time"),
		Attempts:           1,
	}

	if sentinel := MapHTTPStatus(resp.StatusCode); sentinel != nil {
		kucoinErr := &KucoinError{HTTPStatus: resp.StatusCode, RequestID: meta.RequestID, Raw: respBody}
		var envelope struct {
			Code string `json:"code"`
			Msg  string `json:"msg"`
		}
		if json.Unmarshal(respBody, &envelope) == nil {
			kucoinErr.Code = envelope.Code
			kucoinErr.Message = envelope.Msg
			meta.Code = envelope.Code
			meta.Message = envelope.Msg
		}
		return meta, fmt.Errorf("%w: %w", sentinel, kucoinErr)
	}

	var envelope struct {
		Code string          `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	// Successful DELETE endpoints may legitimately return 204 without a
	// KuCoin response envelope.
	if len(bytes.TrimSpace(respBody)) == 0 {
		return meta, nil
	}
	if err := json.Unmarshal(respBody, &envelope); err != nil {
		return meta, fmt.Errorf("kucoin: decode response envelope: %w", err)
	}
	meta.Code = envelope.Code
	meta.Message = envelope.Msg

	if envelope.Code != "" && envelope.Code != successCode {
		return meta, &KucoinError{HTTPStatus: resp.StatusCode, Code: envelope.Code, Message: envelope.Msg, RequestID: meta.RequestID, Raw: respBody}
	}

	if result != nil && len(envelope.Data) > 0 {
		if err := json.Unmarshal(envelope.Data, result); err != nil {
			return meta, fmt.Errorf("kucoin: decode response data: %w", err)
		}
	}

	return meta, nil
}
