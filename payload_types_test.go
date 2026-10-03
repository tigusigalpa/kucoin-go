package kucoin_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	kucoin "github.com/tigusigalpa/kucoin-go"
)

// The point of the typed streaming packages is that an application never parses
// JSON. This test makes that machine-checked: it walks the payload type of every
// Subscribe… method of every streaming session, and fails when a payload can hand
// the consumer something it would have to decode itself — a json.RawMessage, a
// []byte, an empty interface, a function or a channel.

var errorType = reflect.TypeOf((*error)(nil)).Elem()
var rawMessageType = reflect.TypeOf(json.RawMessage(nil))

// payloadOf returns the element type T of a *stream.Subscription[T], looking
// through types that embed one (the managed order books).
func payloadOf(t reflect.Type) (reflect.Type, bool) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, false
	}
	if strings.HasPrefix(t.Name(), "Subscription[") {
		if f, ok := t.FieldByName("out"); ok && f.Type.Kind() == reflect.Chan {
			return f.Type.Elem(), true
		}
		return nil, false
	}
	for i := 0; i < t.NumField(); i++ {
		if f := t.Field(i); f.Anonymous {
			if p, ok := payloadOf(f.Type); ok {
				return p, true
			}
		}
	}
	return nil, false
}

// rawJSONPaths lists the places inside t where undecoded data could reach a
// consumer.
func rawJSONPaths(t reflect.Type, path string, seen map[reflect.Type]bool, out *[]string) {
	switch {
	case t == rawMessageType:
		*out = append(*out, path+": json.RawMessage")
		return
	case t == errorType:
		return
	}
	switch t.Kind() {
	case reflect.Interface:
		*out = append(*out, fmt.Sprintf("%s: %v (an interface)", path, t))
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		*out = append(*out, fmt.Sprintf("%s: %v", path, t))
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 && t.Kind() == reflect.Slice {
			*out = append(*out, fmt.Sprintf("%s: %v (raw bytes)", path, t))
			return
		}
		rawJSONPaths(t.Elem(), path+"[]", seen, out)
	case reflect.Pointer:
		rawJSONPaths(t.Elem(), path, seen, out)
	case reflect.Map:
		rawJSONPaths(t.Key(), path+"{key}", seen, out)
		rawJSONPaths(t.Elem(), path+"{}", seen, out)
	case reflect.Struct:
		if seen[t] {
			return
		}
		seen[t] = true
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			rawJSONPaths(f.Type, path+"."+f.Name, seen, out)
		}
	}
}

func TestSubscriptionPayloadsNeverExposeRawJSON(t *testing.T) {
	client := kucoin.NewClient()
	checked := 0
	for session := range channelSessions {
		typ, ok := sessionType(client, session)
		if !ok {
			t.Errorf("session %s is not wired into kucoin.Client", session)
			continue
		}
		for i := 0; i < typ.NumMethod(); i++ {
			m := typ.Method(i)
			if !strings.HasPrefix(m.Name, "Subscribe") || m.Type.NumOut() != 2 {
				continue
			}
			payload, ok := payloadOf(m.Type.Out(0))
			if !ok {
				t.Errorf("%s.%s returns %v, which is not a typed subscription", session, m.Name, m.Type.Out(0))
				continue
			}
			checked++
			var found []string
			rawJSONPaths(payload, payload.Name(), map[reflect.Type]bool{}, &found)
			for _, f := range found {
				t.Errorf("%s.%s exposes undecoded data to its consumer: %s", session, m.Name, f)
			}
		}
	}
	if checked < 16 {
		t.Errorf("only %d subscription methods were examined; the walk is broken", checked)
	}
}

// The guard is only worth something if it fires; check it against a payload that
// breaks every rule, and one that breaks none.
func TestRawJSONWalkFlagsEveryKindOfUndecodedData(t *testing.T) {
	type nested struct{ Items []any }
	type bad struct {
		A json.RawMessage
		B []byte
		C any
		D map[string]any
		E nested
		F func()
		G *struct{ H interface{ M() } }
	}
	var found []string
	rawJSONPaths(reflect.TypeOf(bad{}), "bad", map[reflect.Type]bool{}, &found)
	for _, want := range []string{"bad.A", "bad.B", "bad.C", "bad.D{}", "bad.E.Items[]", "bad.F", "bad.G.H"} {
		hit := false
		for _, f := range found {
			hit = hit || strings.HasPrefix(f, want+":")
		}
		if !hit {
			t.Errorf("no finding for %s in %v", want, found)
		}
	}

	type fine struct {
		Name  string
		Sizes []int64
		Inner struct{ Prices map[string]string }
		Err   error
	}
	found = nil
	rawJSONPaths(reflect.TypeOf(fine{}), "fine", map[reflect.Type]bool{}, &found)
	if len(found) != 0 {
		t.Errorf("a fully typed payload was flagged: %v", found)
	}
}
