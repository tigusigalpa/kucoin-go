package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestExecutor(t *testing.T, handler http.HandlerFunc, creds Credentials) (*Executor, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	exec := NewExecutor(ExecutorConfig{
		BaseURL:     server.URL,
		Credentials: creds,
		RetryPolicy: NoRetry(),
	})
	return exec, server
}

func TestDo_SetsAuthHeaders(t *testing.T) {
	var gotHeaders http.Header
	exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{}}`))
	}, Credentials{APIKey: "key", APISecret: "secret", APIPassphrase: "pass", APIKeyVersion: "3"})

	_, err := exec.Do(context.Background(), http.MethodGet, "/api/ua/v1/account/ledger", nil, nil, nil)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}

	if got := gotHeaders.Get("KC-API-KEY"); got != "key" {
		t.Errorf("KC-API-KEY = %q, want key", got)
	}
	if got := gotHeaders.Get("KC-API-KEY-VERSION"); got != "3" {
		t.Errorf("KC-API-KEY-VERSION = %q, want 3", got)
	}
	if gotHeaders.Get("KC-API-SIGN") == "" {
		t.Error("KC-API-SIGN is empty")
	}
	if gotHeaders.Get("KC-API-TIMESTAMP") == "" {
		t.Error("KC-API-TIMESTAMP is empty")
	}
	if gotHeaders.Get("KC-API-PASSPHRASE") == "" {
		t.Error("KC-API-PASSPHRASE is empty")
	}
}

func TestDoPublic_DoesNotSetAuthHeaders(t *testing.T) {
	var gotHeaders http.Header
	exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":[]}`))
	}, Credentials{})

	_, err := exec.DoPublic(context.Background(), http.MethodGet, "/api/ua/v1/market/ticker", map[string]string{"tradeType": "SPOT"}, nil)
	if err != nil {
		t.Fatalf("DoPublic: %v", err)
	}
	if gotHeaders.Get("KC-API-KEY") != "" {
		t.Error("KC-API-KEY should be empty for public requests")
	}
}

func TestDoPublic_EncodesQueryParameters(t *testing.T) {
	exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Query().Get("symbol"), "BTC/USDT + test"; got != want {
			t.Errorf("symbol = %q, want %q", got, want)
		}
		if r.URL.Query().Has("empty") {
			t.Error("empty query value should be omitted")
		}
		_, _ = w.Write([]byte(`{"code":"200000","data":{}}`))
	}, Credentials{})

	if _, err := exec.DoPublic(context.Background(), http.MethodGet, "/api/market", map[string]string{
		"symbol": "BTC/USDT + test",
		"empty":  "",
	}, nil); err != nil {
		t.Fatalf("DoPublic: %v", err)
	}
}

func TestDoPublicValues_PreservesRepeatedQueryParameters(t *testing.T) {
	exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Query()["currencies"], []string{"BTC", "ETH"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Errorf("currencies = %v, want %v", got, want)
		}
		if got, want := r.URL.Query().Get("base"), "USD"; got != want {
			t.Errorf("base = %q, want %q", got, want)
		}
		_, _ = w.Write([]byte(`{"code":"200000","data":{}}`))
	}, Credentials{})

	query := url.Values{"base": {"USD"}, "currencies": {"BTC", "ETH"}}
	if _, err := exec.DoPublicValues(context.Background(), http.MethodGet, "/api/market", query, nil); err != nil {
		t.Fatalf("DoPublicValues: %v", err)
	}
}

func TestDoPublic_AppendsQueryParametersToPathQuery(t *testing.T) {
	exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Query().Get("existing"), "value"; got != want {
			t.Errorf("existing = %q, want %q", got, want)
		}
		if got, want := r.URL.Query().Get("symbol"), "BTC-USDT"; got != want {
			t.Errorf("symbol = %q, want %q", got, want)
		}
		_, _ = w.Write([]byte(`{"code":"200000","data":{}}`))
	}, Credentials{})

	if _, err := exec.DoPublic(context.Background(), http.MethodGet, "/api/market?existing=value", map[string]string{
		"symbol": "BTC-USDT",
	}, nil); err != nil {
		t.Fatalf("DoPublic: %v", err)
	}
}

func TestDo_ReturnsErrCredentialsRequiredLocally(t *testing.T) {
	for _, creds := range []Credentials{
		{},
		{APIKey: "key"},
		{APIKey: "key", APISecret: "secret"},
	} {
		called := false
		exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
			called = true
		}, creds)

		_, err := exec.Do(context.Background(), http.MethodGet, "/api/ua/v1/account/ledger", nil, nil, nil)
		if !errors.Is(err, ErrCredentialsRequired) {
			t.Fatalf("expected ErrCredentialsRequired for %+v, got %v", creds, err)
		}
		if called {
			t.Errorf("no network call should have been made for %+v", creds)
		}
	}
}

func TestDo_DecodesResultData(t *testing.T) {
	exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"uid":"12345"}}`))
	}, Credentials{APIKey: "k", APISecret: "s", APIPassphrase: "p"})

	var result struct {
		UID string `json:"uid"`
	}
	_, err := exec.Do(context.Background(), http.MethodGet, "/api/ua/v1/user/api-key", nil, nil, &result)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if result.UID != "12345" {
		t.Errorf("UID = %q, want 12345", result.UID)
	}
}

func TestDo_MapsBusinessErrorCode(t *testing.T) {
	exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"400100","msg":"Parameter Error"}`))
	}, Credentials{APIKey: "k", APISecret: "s", APIPassphrase: "p"})

	_, err := exec.Do(context.Background(), http.MethodGet, "/api/ua/v1/account/ledger", nil, nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	var kucoinErr *KucoinError
	if !errors.As(err, &kucoinErr) {
		t.Fatalf("expected *KucoinError, got %T: %v", err, err)
	}
	if kucoinErr.Code != "400100" || kucoinErr.Message != "Parameter Error" {
		t.Errorf("unexpected KucoinError: %+v", kucoinErr)
	}
}

func TestDo_MapsHTTPStatusSentinel(t *testing.T) {
	exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":"401","msg":"Invalid API Key"}`))
	}, Credentials{APIKey: "k", APISecret: "s", APIPassphrase: "p"})

	_, err := exec.Do(context.Background(), http.MethodGet, "/api/ua/v1/account/ledger", nil, nil, nil)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
}

func TestDo_CapturesRateLimitAndTimingHeaders(t *testing.T) {
	exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("gw-ratelimit-limit", "4000")
		w.Header().Set("gw-ratelimit-remaining", "3999")
		w.Header().Set("gw-ratelimit-reset", "30000")
		w.Header().Set("x-in-time", "1700000000000000")
		w.Header().Set("x-out-time", "1700000000001000")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":"200000","data":{}}`))
	}, Credentials{APIKey: "k", APISecret: "s", APIPassphrase: "p"})

	meta, err := exec.Do(context.Background(), http.MethodGet, "/api/ua/v1/account/ledger", nil, nil, nil)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if meta.RateLimitLimit != "4000" || meta.RateLimitRemaining != "3999" || meta.RateLimitReset != "30000" {
		t.Errorf("unexpected rate-limit metadata: %+v", meta)
	}
	if meta.InTime == "" || meta.OutTime == "" {
		t.Error("expected InTime/OutTime to be captured")
	}
}

func TestDo_RetriesOnlyGETRequests(t *testing.T) {
	var getAttempts, postAttempts int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			getAttempts++
		} else {
			postAttempts++
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code":"500","msg":"error"}`))
	}))
	defer server.Close()

	exec := NewExecutor(ExecutorConfig{
		BaseURL:     server.URL,
		Credentials: Credentials{APIKey: "k", APISecret: "s", APIPassphrase: "p"},
		RetryPolicy: &RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond, MaxElapsed: time.Second},
	})

	meta, _ := exec.Do(context.Background(), http.MethodGet, "/path", nil, nil, nil)
	_, _ = exec.Do(context.Background(), http.MethodPost, "/path", nil, map[string]string{"a": "b"}, nil)

	if meta == nil || meta.Attempts != 3 {
		t.Errorf("response attempts = %+v, want 3", meta)
	}
	if getAttempts != 3 {
		t.Errorf("GET attempts = %d, want 3 (retried)", getAttempts)
	}
	if postAttempts != 1 {
		t.Errorf("POST attempts = %d, want 1 (never retried)", postAttempts)
	}
}

func TestDo_RetriesRateLimitedGETAndReturnsSuccessfulResult(t *testing.T) {
	attempts := 0
	exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"code":"429000","msg":"Too many requests"}`))
			return
		}
		_, _ = w.Write([]byte(`{"code":"200000","data":{"value":"ok"}}`))
	}, Credentials{})
	exec.cfg.RetryPolicy = &RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    time.Millisecond,
		MaxElapsed:  time.Second,
	}

	var result struct {
		Value string `json:"value"`
	}
	meta, err := exec.DoPublic(context.Background(), http.MethodGet, "/api/market", nil, &result)
	if err != nil {
		t.Fatalf("DoPublic: %v", err)
	}
	if attempts != 3 || meta.Attempts != 3 || result.Value != "ok" {
		t.Errorf("attempts=%d meta=%+v result=%+v, want 3 attempts and decoded result", attempts, meta, result)
	}
}

func TestDo_RejectsOversizedResponse(t *testing.T) {
	exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, maxResponseBodyBytes+1))
	}, Credentials{})

	_, err := exec.DoPublic(context.Background(), http.MethodGet, "/api/ua/v1/market/ticker", nil, nil)
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("expected ErrResponseTooLarge, got %v", err)
	}
}

func TestDoPublic_AcceptsNoContentResponse(t *testing.T) {
	exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}, Credentials{})

	meta, err := exec.DoPublic(context.Background(), http.MethodDelete, "/api/resource", nil, nil)
	if err != nil {
		t.Fatalf("DoPublic: %v", err)
	}
	if meta == nil || meta.HTTPStatus != http.StatusNoContent {
		t.Fatalf("response metadata = %+v, want HTTP 204", meta)
	}
}

func TestDo_DoesNotRetryPermanentBuildRequestError(t *testing.T) {
	retries := 0
	exec := NewExecutor(ExecutorConfig{
		BaseURL: "//bad-base-url",
		RetryPolicy: &RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   time.Millisecond,
			MaxDelay:    time.Millisecond,
			MaxElapsed:  time.Second,
			OnRetry: func(attempt int, delay time.Duration, err error) {
				retries++
			},
		},
	})

	_, err := exec.DoPublic(context.Background(), http.MethodGet, "/api/ua/v1/market/ticker", nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if retries != 0 {
		t.Fatalf("retries = %d, want 0 for permanent local build error", retries)
	}
}

func TestMapHTTPStatus_Success(t *testing.T) {
	if err := MapHTTPStatus(200); err != nil {
		t.Errorf("expected nil for 200, got %v", err)
	}
}

func TestMapHTTPStatus(t *testing.T) {
	tests := []struct {
		status int
		want   error
	}{
		{http.StatusCreated, nil},
		{http.StatusAccepted, nil},
		{http.StatusNoContent, nil},
		{http.StatusBadRequest, ErrBadRequest},
		{http.StatusUnauthorized, ErrUnauthorized},
		{http.StatusForbidden, ErrForbiddenOrLimited},
		{http.StatusNotFound, ErrNotFound},
		{http.StatusMethodNotAllowed, ErrMethodNotAllowed},
		{http.StatusUnsupportedMediaType, ErrUnsupportedMedia},
		{http.StatusTooManyRequests, ErrRateLimited},
		{http.StatusInternalServerError, ErrServerError},
		{http.StatusServiceUnavailable, ErrServiceUnavailable},
		{http.StatusBadGateway, ErrServerError},
		{http.StatusTeapot, ErrBadRequest},
		{http.StatusMultipleChoices, nil},
	}

	for _, test := range tests {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			if got := MapHTTPStatus(test.status); !errors.Is(got, test.want) {
				t.Errorf("MapHTTPStatus(%d) = %v, want %v", test.status, got, test.want)
			}
		})
	}
}

func TestCredentialsAndRetryPolicyDefaults(t *testing.T) {
	if !(Credentials{}).IsZero() {
		t.Fatal("empty credentials should be zero")
	}
	if (Credentials{APIKey: "key"}).IsZero() {
		t.Fatal("non-empty credentials should not be zero")
	}

	policy := NewDefaultRetryPolicy()
	if policy.MaxAttempts != 3 || policy.BaseDelay != 250*time.Millisecond || policy.MaxDelay != 5*time.Second || policy.MaxElapsed != 20*time.Second {
		t.Fatalf("unexpected default retry policy: %+v", policy)
	}

	NoopLogger{}.Debug("debug")
	NoopLogger{}.Info("info")
	NoopLogger{}.Warn("warn")
	NoopLogger{}.Error("error")
}

func TestRetryPolicyDelayForAttemptHandlesDisabledBackoff(t *testing.T) {
	if got := (&RetryPolicy{}).delayForAttempt(1); got != 0 {
		t.Errorf("delay without a base delay = %v, want 0", got)
	}
	if got := (&RetryPolicy{BaseDelay: time.Millisecond}).delayForAttempt(1); got != 0 {
		t.Errorf("delay without a maximum delay = %v, want 0", got)
	}
}

func TestDoOptional_SignsOnlyWhenCredentialsAreConfigured(t *testing.T) {
	var gotHeaders http.Header
	handler := func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		_, _ = w.Write([]byte(`{"code":"200000","data":{"v":1}}`))
	}
	signed, _ := newTestExecutor(t, handler, Credentials{APIKey: "key", APISecret: "secret", APIPassphrase: "pass"})
	var out struct{ V int }
	if _, err := signed.DoOptional(context.Background(), http.MethodGet, "/api/v1/trade-statistics", nil, &out); err != nil {
		t.Fatalf("DoOptional with credentials: %v", err)
	}
	if gotHeaders.Get("KC-API-KEY") != "key" || gotHeaders.Get("KC-API-SIGN") == "" || out.V != 1 {
		t.Fatalf("the request must be signed when credentials exist: %v %+v", gotHeaders, out)
	}

	anonymous, _ := newTestExecutor(t, handler, Credentials{})
	gotHeaders = nil
	if _, err := anonymous.DoOptional(context.Background(), http.MethodGet, "/api/v1/trade-statistics", nil, nil); err != nil {
		t.Fatalf("DoOptional without credentials must not fail locally: %v", err)
	}
	if gotHeaders == nil || gotHeaders.Get("KC-API-KEY") != "" || gotHeaders.Get("KC-API-SIGN") != "" {
		t.Fatalf("the request must be sent unsigned without credentials: %v", gotHeaders)
	}
}

func TestDoOptional_SurfacesKuCoinsOwnErrorWithoutCredentials(t *testing.T) {
	exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":"400001","msg":"Please check the header of your request for KC-API-KEY, KC-API-SIGN, KC-API-TIMESTAMP, KC-API-PASSPHRASE."}`))
	}, Credentials{})
	_, err := exec.DoOptional(context.Background(), http.MethodGet, "/api/v1/trade-statistics", nil, nil)
	var kerr *KucoinError
	if errors.Is(err, ErrCredentialsRequired) || !errors.As(err, &kerr) || kerr.Code != "400001" || !errors.Is(err, ErrBadRequest) {
		t.Fatalf("error = %v, want KuCoin's own 400001", err)
	}
}

func TestExecutor_IsSafeForConcurrentUse(t *testing.T) {
	var served atomic.Int64
	exec, _ := newTestExecutor(t, func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		_, _ = w.Write([]byte(`{"code":"200000","data":{"path":"` + r.URL.Path + `"}}`))
	}, Credentials{APIKey: "k", APISecret: "s", APIPassphrase: "p"})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var out struct{ Path string }
			path := "/api/v1/p" + strconv.Itoa(i)
			var err error
			switch i % 3 {
			case 0:
				_, err = exec.DoPublic(context.Background(), http.MethodGet, path, map[string]string{"q": "v"}, &out)
			case 1:
				_, err = exec.Do(context.Background(), http.MethodPost, path, nil, map[string]string{"a": "b"}, &out)
			default:
				_, err = exec.DoOptional(context.Background(), http.MethodGet, path, nil, &out)
			}
			if err != nil || out.Path != path {
				t.Errorf("request %d: %v %+v", i, err, out)
			}
		}(i)
	}
	wg.Wait()
	if served.Load() != 32 {
		t.Fatalf("served %d requests", served.Load())
	}
}

func TestDo_ContextCancellationInterruptsTheRetryBackoff(t *testing.T) {
	exec := NewExecutor(ExecutorConfig{
		BaseURL: func() string {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"code":"500","msg":"down"}`))
			}))
			t.Cleanup(s.Close)
			return s.URL
		}(),
		RetryPolicy: &RetryPolicy{MaxAttempts: 5, BaseDelay: 10 * time.Second, MaxDelay: 10 * time.Second, MaxElapsed: time.Minute},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := exec.DoPublic(ctx, http.MethodGet, "/x", nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want the context error", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("cancellation took %v; the backoff sleep must be interruptible", time.Since(start))
	}
}
