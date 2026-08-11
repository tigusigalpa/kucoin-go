package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
