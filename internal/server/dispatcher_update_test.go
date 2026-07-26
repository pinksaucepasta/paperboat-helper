package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pinksaucepasta/paperboat-helper/internal/bootstrap"
)

type recordingUpdateClient struct{ calls int }

func (c *recordingUpdateClient) Activate(_ context.Context, worker, host bootstrap.ArtifactManifest) (string, error) {
	c.calls++
	return worker.Version, nil
}

func TestDispatcherForwardsOnlyTypedPairedUpdateManifests(t *testing.T) {
	client := &recordingUpdateClient{}
	dispatcher := &Dispatcher{config: DispatcherConfig{Updates: client}}
	capabilities := dispatcher.Capabilities()
	found := false
	for _, capability := range capabilities {
		found = found || capability == "update.signed.v1"
	}
	if !found {
		t.Fatal("signed update capability was not advertised")
	}
	payload, _ := json.Marshal(signedUpdateRequest{
		WorkerArtifact:      bootstrap.ArtifactManifest{Kind: bootstrap.ArtifactKindWorker, Version: "2026.07.25.5"},
		HostServiceArtifact: bootstrap.ArtifactManifest{Kind: bootstrap.ArtifactKindHostService, Version: "2026.07.25.5"},
	})
	result := dispatcher.Handle(context.Background(), Authorization{}, "update.signed.v1", payload)
	if result.ErrorCode != "" || client.calls != 1 {
		t.Fatalf("result=%+v calls=%d", result, client.calls)
	}
	hostile := append(payload[:len(payload)-1], []byte(`,"path":"/tmp/pbh"}`)...)
	result = dispatcher.Handle(context.Background(), Authorization{}, "update.signed.v1", hostile)
	if result.ErrorCode != "invalid_request" || client.calls != 1 {
		t.Fatalf("hostile result=%+v calls=%d", result, client.calls)
	}
}
