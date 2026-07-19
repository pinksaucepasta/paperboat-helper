package activity

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"
)

type signalKeys map[string]ed25519.PublicKey

func (k signalKeys) Lookup(id string) (ed25519.PublicKey, bool) { key, ok := k[id]; return key, ok }

func TestSignedSignalRecordsOnlyExactAuthenticatedEvent(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	collector, _ := New(Config{Clock: &fixedClock{now: now}})
	event := Event{EnvironmentID: "env_1", SourceID: "herdr_1", SessionID: "session_1", ProcessID: "process_1", Source: AgentSignal, Sequence: 1, OccurredAt: now}
	envelope, err := SignSignal("key_1", "helper_1", event, func(payload []byte) []byte { return ed25519.Sign(private, payload) })
	if err != nil {
		t.Fatal(err)
	}
	verifier := SignalVerifier{HelperID: "helper_1", Keys: signalKeys{"key_1": public}}
	result, err := verifier.Record(collector, envelope)
	if err != nil || !result.ExtendsIdle {
		t.Fatalf("result=%+v err=%v", result, err)
	}

	tampered := envelope
	tampered.Event.Sequence = 2
	if _, err := verifier.Record(collector, tampered); !errors.Is(err, ErrSignalInvalid) {
		t.Fatalf("tampered err=%v", err)
	}
	wrongHelper := verifier
	wrongHelper.HelperID = "helper_2"
	if _, err := wrongHelper.Record(collector, envelope); !errors.Is(err, ErrSignalInvalid) {
		t.Fatalf("wrong helper err=%v", err)
	}
	if _, err := verifier.Record(collector, envelope); !errors.Is(err, ErrInvalidSequence) {
		t.Fatalf("replay err=%v", err)
	}
}
