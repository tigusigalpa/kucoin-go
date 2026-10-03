package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The generated documents under docs/ must always equal what the manifests
// produce. CI runs `go run ./internal/gendocs && git diff --exit-code`; this test
// makes `go test ./...` catch the same drift before a push.
func TestGeneratedDocsAreCurrent(t *testing.T) {
	endpoints, channels, err := render(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"ENDPOINTS.md": endpoints, "CHANNELS.md": channels} {
		got, err := os.ReadFile(filepath.Join("..", "..", "docs", name))
		if err != nil {
			t.Fatalf("read docs/%s: %v (run `go run ./internal/gendocs`)", name, err)
		}
		if string(got) != want {
			t.Errorf("docs/%s is out of date; run `go run ./internal/gendocs`", name)
		}
	}
}

func TestRenderChannelsGroupsRowsAndListsGaps(t *testing.T) {
	doc := renderChannels(channelManifest{
		Channels: []channel{
			{Session: "UTA.V2.Stream", Method: "SubscribeTicker", Market: "Futures", Access: "Public", Topic: "ticker", Payload: "Ticker", Status: "Supported", Test: "x_test.go: TestX", DocURL: "https://example.test/a"},
			{Session: "Classic.Futures.Stream", Method: "SubscribeTrades", Market: "Futures", Access: "Public", Topic: "/t", Payload: "Trade", Status: "Supported", Test: "y_test.go: TestY", DocURL: "https://example.test/b"},
			{Session: "Mystery.Stream", Method: "SubscribeX"},
		},
		NotImplemented: []notImplemented{{Name: "WS order entry", Reason: "trading", DocURL: "https://example.test/c"}},
	})
	futures, uta := strings.Index(doc, "## Classic Futures"), strings.Index(doc, "## UTA WebSocket v2")
	if futures < 0 || uta < 0 || futures > uta {
		t.Fatalf("groups missing or out of order:\n%s", doc)
	}
	for _, want := range []string{"`SubscribeTrades`", "`SubscribeTicker`", "Unlisted session group \"Mystery.Stream\"", "WS order entry"} {
		if !strings.Contains(doc, want) {
			t.Errorf("document lacks %q", want)
		}
	}
	if none := renderChannels(channelManifest{}); !strings.Contains(none, "None.") {
		t.Errorf("an empty manifest must say so:\n%s", none)
	}
}

func TestCellEscapesPipesExactlyOnce(t *testing.T) {
	for in, want := range map[string]string{
		"a | b":              `a \| b`,
		`already \| escaped`: `already \| escaped`,
		"none":               "none",
		"a|b|c":              `a\|b\|c`,
	} {
		if got := cell(in); got != want {
			t.Errorf("cell(%q) = %q, want %q", in, got, want)
		}
	}
	doc := renderChannels(channelManifest{Channels: []channel{{Session: "Classic.Futures.Stream", Method: "SubscribePositions", Market: "Futures", Access: "Private",
		Topic: "/contract/positionAll | /contract/position:{symbol}", Payload: "PositionEvent", Status: "Supported", Test: "x_test.go: TestX", DocURL: "https://example.test/p"}}})
	for _, line := range strings.Split(doc, "\n") {
		if strings.Contains(line, "SubscribePositions") {
			// 8 columns => 9 column separators once the escaped pipes are discounted
			if n := strings.Count(strings.ReplaceAll(line, `\|`, ""), "|"); n != 9 {
				t.Fatalf("the row has %d column separators, want 9: %s", n, line)
			}
		}
	}
}

func TestRenderEndpointsListsAbandonedAndCoverage(t *testing.T) {
	doc := renderEndpoints(manifest{
		Endpoints: []endpoint{{Method: "A.B", HTTPMethod: "GET", Path: "/p", DocURL: "https://example.test"}},
		Abandoned: []abandoned{{Method: "Old", Reason: "gone", DocURL: "https://example.test/old"}},
	})
	for _, want := range []string{"`A.B`", "## Not Supported / Abandoned", "`Old`", "CHANNELS.md"} {
		if !strings.Contains(doc, want) {
			t.Errorf("document lacks %q", want)
		}
	}
	if none := renderEndpoints(manifest{}); !strings.Contains(none, "None tracked yet") {
		t.Errorf("an empty manifest must say so")
	}
}
