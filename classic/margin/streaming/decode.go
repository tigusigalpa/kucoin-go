package streaming

import (
	"encoding/json"
	"errors"
	"strings"

	spotstreaming "github.com/tigusigalpa/kucoin-go/classic/spot/streaming"
	"github.com/tigusigalpa/kucoin-go/websocket/classic"
)

// symbolFromTopic returns the symbol part of a topic such as
// "/indicator/index:USDT-BTC"; "" when the topic has none.
func symbolFromTopic(topic string) string {
	if i := strings.LastIndexByte(topic, ':'); i >= 0 {
		return topic[i+1:]
	}
	return ""
}

// decodePlain builds a decoder that unmarshals the push payload into T and then
// lets fill add the fields that live outside the payload (topic, subject, ...).
func decodePlain[T any](fill func(v *T, m *classic.Message)) classic.DecodeFunc[T] {
	return func(m *classic.Message) (T, bool, error) {
		var v T
		if len(m.Data) == 0 {
			return v, false, errors.New("push carries no data")
		}
		if err := json.Unmarshal(m.Data, &v); err != nil {
			return v, false, err
		}
		if fill != nil {
			fill(&v, m)
		}
		return v, true, nil
	}
}

func envelope(m *classic.Message) spotstreaming.PrivateEnvelope {
	return spotstreaming.PrivateEnvelope{Subject: m.Subject, UserID: m.UserID, ChannelType: m.ChannelType}
}

var (
	decodeIndexPrice = decodePlain(func(v *IndexPrice, m *classic.Message) {
		if v.Symbol == "" {
			v.Symbol = symbolFromTopic(m.Topic)
		}
	})
	decodeMarkPrice = decodePlain(func(v *MarkPrice, m *classic.Message) {
		if v.Symbol == "" {
			v.Symbol = symbolFromTopic(m.Topic)
		}
	})
	decodeCrossMarginPosition = decodePlain(func(v *CrossMarginPositionEvent, m *classic.Message) {
		v.PrivateEnvelope = envelope(m)
	})
	decodeIsolatedMarginPosition = decodePlain(func(v *IsolatedMarginPositionEvent, m *classic.Message) {
		v.PrivateEnvelope = envelope(m)
	})
)
