package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/activity"
)

type dispatcherSignalKeys map[string]ed25519.PublicKey

func (k dispatcherSignalKeys) Lookup(id string) (ed25519.PublicKey, bool) {
	key, ok := k[id]
	return key, ok
}

type activityClock struct{ now time.Time }

func (c activityClock) Now() time.Time { return c.now }

func TestDispatcherRequiresSignatureForAgentSignal(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	collector, _ := activity.New(activity.Config{Clock: activityClock{now}})
	verifier := &activity.SignalVerifier{HelperID: "helper_1", Keys: dispatcherSignalKeys{"key_1": public}}
	dispatcher := &Dispatcher{config: DispatcherConfig{Activity: collector, SignalVerifier: verifier}}
	event := activity.Event{EnvironmentID: "env_1", SourceID: "herdr_1", Source: activity.AgentSignal, Sequence: 1, OccurredAt: now}
	plain, _ := json.Marshal(event)
	if outcome := dispatcher.Handle(context.Background(), Authorization{EnvironmentID: "env_1"}, "activity.v1", plain); outcome.ErrorCode != "unauthorized" {
		t.Fatalf("plain error=%q", outcome.ErrorCode)
	}
	envelope, _ := activity.SignSignal("key_1", "helper_1", event, func(payload []byte) []byte { return ed25519.Sign(private, payload) })
	signed, _ := json.Marshal(envelope)
	if outcome := dispatcher.Handle(context.Background(), Authorization{EnvironmentID: "env_1"}, "activity.v1", signed); outcome.ErrorCode != "" {
		t.Fatalf("signed error=%q", outcome.ErrorCode)
	}
}
