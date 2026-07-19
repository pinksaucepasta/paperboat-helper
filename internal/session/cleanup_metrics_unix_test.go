//go:build darwin || linux

package session

import (
	"context"
	"errors"
	"testing"
)

type sessionCleanupMetric struct{ result string }

func (m *sessionCleanupMetric) Record(_ string, _ float64, labels map[string]string) error {
	m.result = labels["result"]
	return nil
}

func TestDeleteEmitsCleanupOutcome(t *testing.T) {
	manager, root, shell := realManager(t)
	metric := &sessionCleanupMetric{}
	manager.config.Metrics = metric
	created, err := manager.Create(context.Background(), CreateRequest{Name: "cleanup", Command: shellCommand(shell, root, "sleep 10")})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Delete(created.ID); !errors.Is(err, ErrSessionRunning) || metric.result != "preserved" {
		t.Fatalf("running delete err=%v metric=%q", err, metric.result)
	}
	if _, err := manager.Close(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Delete(created.ID); err != nil || metric.result != "removed" {
		t.Fatalf("closed delete err=%v metric=%q", err, metric.result)
	}
}
