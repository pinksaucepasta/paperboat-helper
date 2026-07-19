package activity

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
)

var ErrSignalInvalid = errors.New("invalid signed activity signal")

type SignalEnvelope struct {
	KeyID     string `json:"key_id"`
	HelperID  string `json:"helper_id"`
	Event     Event  `json:"event"`
	Signature string `json:"signature_base64url"`
}

type SignalKeySource interface {
	Lookup(string) (ed25519.PublicKey, bool)
}

type SignalVerifier struct {
	HelperID string
	Keys     SignalKeySource
}

func SignSignal(keyID, helperID string, event Event, sign func([]byte) []byte) (SignalEnvelope, error) {
	if keyID == "" || helperID == "" || sign == nil || event.Source != AgentSignal {
		return SignalEnvelope{}, ErrSignalInvalid
	}
	payload, err := signalPayload(keyID, helperID, event)
	if err != nil {
		return SignalEnvelope{}, err
	}
	signature := sign(payload)
	if len(signature) != ed25519.SignatureSize {
		return SignalEnvelope{}, ErrSignalInvalid
	}
	return SignalEnvelope{KeyID: keyID, HelperID: helperID, Event: event, Signature: base64.RawURLEncoding.EncodeToString(signature)}, nil
}

func (v SignalVerifier) Verify(envelope SignalEnvelope) (Event, error) {
	if v.Keys == nil || v.HelperID == "" || envelope.HelperID != v.HelperID || envelope.KeyID == "" || envelope.Event.Source != AgentSignal {
		return Event{}, ErrSignalInvalid
	}
	key, ok := v.Keys.Lookup(envelope.KeyID)
	if !ok || len(key) != ed25519.PublicKeySize {
		return Event{}, ErrSignalInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(envelope.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return Event{}, ErrSignalInvalid
	}
	payload, err := signalPayload(envelope.KeyID, envelope.HelperID, envelope.Event)
	if err != nil || !ed25519.Verify(key, payload, signature) {
		return Event{}, ErrSignalInvalid
	}
	return envelope.Event, nil
}

func (v SignalVerifier) Record(collector *Collector, envelope SignalEnvelope) (RecordResult, error) {
	if collector == nil {
		return RecordResult{}, ErrSignalInvalid
	}
	event, err := v.Verify(envelope)
	if err != nil {
		return RecordResult{}, err
	}
	return collector.Record(event, true)
}

func signalPayload(keyID, helperID string, event Event) ([]byte, error) {
	return json.Marshal(struct {
		Version  int    `json:"version"`
		KeyID    string `json:"key_id"`
		HelperID string `json:"helper_id"`
		Event    Event  `json:"event"`
	}{1, keyID, helperID, event})
}
