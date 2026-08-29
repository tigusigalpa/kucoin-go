package kucoin

import (
	"net/http"
	"testing"
	"time"
)

type testClock struct{ now time.Time }

func (c testClock) Now() time.Time { return c.now }

type testLogger struct{}

func (testLogger) Debug(string, ...any) {}
func (testLogger) Info(string, ...any)  {}
func (testLogger) Warn(string, ...any)  {}
func (testLogger) Error(string, ...any) {}

func TestNewConfigAppliesOptions(t *testing.T) {
	credentials := Credentials{APIKey: "key", APISecret: "secret", APIPassphrase: "pass"}
	httpClient := &http.Client{Timeout: 3 * time.Second}
	clock := testClock{now: time.Unix(1, 0)}
	logger := testLogger{}
	retryPolicy := NoRetry()

	cfg := newConfig(
		WithCredentials(credentials),
		WithUTABaseURL("https://uta.example"),
		WithClassicBaseURL("https://classic.example"),
		WithClassicFuturesBaseURL("https://futures.example"),
		WithHTTPClient(httpClient),
		WithTimeout(time.Second),
		WithSiteType(SiteTypeAustralia),
		WithEnableNS(true),
		WithClock(clock),
		WithLogger(logger),
		WithRetryPolicy(retryPolicy),
	)

	if cfg.Credentials != credentials || cfg.UTABaseURL != "https://uta.example" || cfg.ClassicBaseURL != "https://classic.example" || cfg.ClassicFuturesBaseURL != "https://futures.example" {
		t.Fatalf("base configuration was not applied: %+v", cfg)
	}
	if cfg.HTTPClient != httpClient || cfg.Timeout != time.Second || cfg.SiteType != SiteTypeAustralia || !cfg.EnableNS || cfg.Clock != clock || cfg.Logger != logger || cfg.RetryPolicy != retryPolicy {
		t.Fatalf("options were not applied: %+v", cfg)
	}
}

func TestNewConfigDefaultsAndNewClient(t *testing.T) {
	cfg := newConfig()
	if cfg.HTTPClient == nil || cfg.HTTPClient.Timeout != 15*time.Second || cfg.RetryPolicy == nil || cfg.Clock == nil || cfg.Logger == nil {
		t.Fatalf("default configuration is incomplete: %+v", cfg)
	}

	client := NewClient(WithHTTPClient(&http.Client{}))
	if client.utaExecutor == nil || client.classicExecutor == nil || client.classicFuturesExecutor == nil {
		t.Fatal("NewClient did not initialize all executors")
	}
	if client.UTA.Market == nil || client.UTA.Account == nil || client.UTA.Orders == nil || client.UTA.Positions == nil || client.UTA.Leverage == nil || client.UTA.Ws == nil {
		t.Fatal("NewClient did not initialize all UTA services")
	}
	if client.Classic.Spot.Market == nil || client.Classic.Spot.Orders == nil || client.Classic.Spot.Ws == nil || client.Classic.Futures.Orders == nil || client.Classic.Futures.Positions == nil || client.Classic.Futures.Ws == nil || client.Classic.Margin.Market == nil || client.Classic.Margin.Orders == nil || client.Classic.Margin.Debit == nil {
		t.Fatal("NewClient did not initialize all Classic services")
	}
}
