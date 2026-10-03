package kucoin_test

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	kucoin "github.com/tigusigalpa/kucoin-go"
)

// The manifests under internal/ are the source of truth for the coverage
// documents. These tests make sure they cannot claim more than the code
// implements: every row must resolve to a real method on kucoin.Client, every
// referenced test must exist, and every REST method of a service listed in the
// manifest must have a row.

type endpointRow struct {
	Method string `yaml:"method"`
	Path   string `yaml:"path"`
	Test   string `yaml:"test"`
	DocURL string `yaml:"doc_url"`
	Status string `yaml:"status"`
}

type channelRow struct {
	Session string `yaml:"session"`
	Method  string `yaml:"method"`
	Topic   string `yaml:"topic"`
	Payload string `yaml:"payload"`
	Test    string `yaml:"test"`
	DocURL  string `yaml:"doc_url"`
	Status  string `yaml:"status"`
	Access  string `yaml:"access"`
}

func readManifest(t *testing.T, name string, out any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("internal", name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	if err := yaml.Unmarshal(raw, out); err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
}

// walk follows exported fields from v along path; the final element is returned
// as a reflect.Value of the last field.
func walk(v reflect.Value, path []string) (reflect.Value, bool) {
	for _, seg := range path {
		for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		if v.Kind() != reflect.Struct {
			return reflect.Value{}, false
		}
		v = v.FieldByName(seg)
		if !v.IsValid() {
			return reflect.Value{}, false
		}
	}
	return v, true
}

func resolveMethod(client *kucoin.Client, dotted string) (reflect.Value, bool) {
	parts := strings.Split(dotted, ".")
	if len(parts) < 2 {
		return reflect.Value{}, false
	}
	owner, ok := walk(reflect.ValueOf(client), parts[:len(parts)-1])
	if !ok {
		return reflect.Value{}, false
	}
	m := owner.MethodByName(parts[len(parts)-1])
	return m, m.IsValid()
}

var testRef = regexp.MustCompile(`^([\w./-]+\.go): (Test\w+)`)

func checkTestRef(t *testing.T, label, ref string) {
	t.Helper()
	m := testRef.FindStringSubmatch(ref)
	if m == nil {
		t.Errorf("%s: test reference %q must look like \"path/file_test.go: TestName\"", label, ref)
		return
	}
	src, err := os.ReadFile(filepath.FromSlash(m[1]))
	if err != nil {
		t.Errorf("%s: test file %s: %v", label, m[1], err)
		return
	}
	if !regexp.MustCompile(`(?m)^func ` + regexp.QuoteMeta(m[2]) + `\(`).Match(src) {
		t.Errorf("%s: %s has no func %s", label, m[1], m[2])
	}
}

func TestEndpointManifestMatchesTheImplementation(t *testing.T) {
	var manifest struct {
		Endpoints []endpointRow `yaml:"endpoints"`
	}
	readManifest(t, "endpoints.yaml", &manifest)
	client := kucoin.NewClient()
	seen := map[string]bool{}
	for _, row := range manifest.Endpoints {
		if seen[row.Method] {
			t.Errorf("duplicate manifest row for %s", row.Method)
		}
		seen[row.Method] = true
		if _, ok := resolveMethod(client, row.Method); !ok {
			t.Errorf("manifest row %s does not resolve to a method on kucoin.Client", row.Method)
		}
		if !strings.HasPrefix(row.DocURL, "https://www.kucoin.com/docs-new/") {
			t.Errorf("%s: doc_url %q is not a KuCoin docs link", row.Method, row.DocURL)
		}
		if row.Test != "" && !strings.HasPrefix(row.Test, "n/a") {
			checkTestRef(t, row.Method, row.Test)
		}
	}
}

// restServices lists the service roots whose every public API method must be in the
// endpoints manifest.
var restServices = []string{
	"Classic.Futures.Market",
}

func TestEveryRESTMethodOfTrackedServicesHasAManifestRow(t *testing.T) {
	var manifest struct {
		Endpoints []endpointRow `yaml:"endpoints"`
	}
	readManifest(t, "endpoints.yaml", &manifest)
	rows := map[string]bool{}
	for _, row := range manifest.Endpoints {
		rows[row.Method] = true
	}
	client := kucoin.NewClient()
	for _, service := range restServices {
		v, ok := walk(reflect.ValueOf(client), strings.Split(service, "."))
		if !ok {
			t.Errorf("service %s does not exist on kucoin.Client", service)
			continue
		}
		typ := v.Type()
		for i := 0; i < typ.NumMethod(); i++ {
			name := typ.Method(i).Name
			if !rows[service+"."+name] {
				t.Errorf("%s.%s is exported but has no row in internal/endpoints.yaml", service, name)
			}
		}
	}
}

// channelSessions maps a manifest "session" to the dial methods that produce its
// session type.
var channelSessions = map[string][]string{
	"Classic.Futures.Stream": {"DialPublic", "DialPrivate"},
	"Classic.Spot.Stream":    {"DialPublic", "DialPrivate"},
	"Classic.Margin.Stream":  {"DialPublic", "DialPrivate"},
	"UTA.V2.Stream":          {"DialFutures", "DialSpot", "DialPrivate"},
}

// sessionType returns the Session type produced by a service's dial methods.
func sessionType(client *kucoin.Client, session string) (reflect.Type, bool) {
	owner, ok := walk(reflect.ValueOf(client), strings.Split(session, "."))
	if !ok {
		return nil, false
	}
	for _, dial := range channelSessions[session] {
		m := owner.MethodByName(dial)
		if m.IsValid() && m.Type().NumOut() == 2 {
			return m.Type().Out(0), true
		}
	}
	return nil, false
}

func TestChannelManifestMatchesTheImplementation(t *testing.T) {
	var manifest struct {
		Channels []channelRow `yaml:"channels"`
	}
	readManifest(t, "channels.yaml", &manifest)
	client := kucoin.NewClient()
	documented := map[string]map[string]bool{}
	for _, row := range manifest.Channels {
		typ, ok := sessionType(client, row.Session)
		if !ok {
			t.Errorf("%s.%s: session %q does not exist on kucoin.Client", row.Session, row.Method, row.Session)
			continue
		}
		if _, ok := typ.MethodByName(row.Method); !ok {
			t.Errorf("%s: session type %v has no method %s", row.Session, typ, row.Method)
		}
		if documented[row.Session] == nil {
			documented[row.Session] = map[string]bool{}
		}
		if documented[row.Session][row.Method] {
			t.Errorf("duplicate channel row for %s.%s", row.Session, row.Method)
		}
		documented[row.Session][row.Method] = true
		if !strings.HasPrefix(row.DocURL, "https://www.kucoin.com/docs-new/") {
			t.Errorf("%s.%s: doc_url %q is not a KuCoin docs link", row.Session, row.Method, row.DocURL)
		}
		if row.Topic == "" || row.Payload == "" || row.Status == "" || (row.Access != "Public" && row.Access != "Private") {
			t.Errorf("%s.%s: topic, payload, status and access (Public|Private) are required: %+v", row.Session, row.Method, row)
		}
		checkTestRef(t, row.Session+"."+row.Method, row.Test)
	}
	// The reverse direction: no Subscribe* method may exist without documentation.
	// A method a session merely inherits from an embedded session (a Margin session
	// embeds the Spot one) is documented with the type that declares it; the Margin
	// channels that are served by an inherited method have rows of their own because
	// KuCoin documents them under Margin.
	for session := range channelSessions {
		typ, ok := sessionType(client, session)
		if !ok {
			t.Errorf("session %s is not wired into kucoin.Client", session)
			continue
		}
		for i := 0; i < typ.NumMethod(); i++ {
			name := typ.Method(i).Name
			if strings.HasPrefix(name, "Subscribe") && !documented[session][name] && !inheritedFromEmbedded(typ, name) {
				t.Errorf("%s.%s exists but has no row in internal/channels.yaml", session, name)
			}
		}
	}
}

// inheritedFromEmbedded reports whether method name of typ is promoted from an
// embedded field rather than declared on typ itself.
func inheritedFromEmbedded(typ reflect.Type, name string) bool {
	st := typ
	for st.Kind() == reflect.Pointer {
		st = st.Elem()
	}
	if st.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < st.NumField(); i++ {
		if f := st.Field(i); f.Anonymous {
			if _, ok := f.Type.MethodByName(name); ok {
				return true
			}
		}
	}
	return false
}
