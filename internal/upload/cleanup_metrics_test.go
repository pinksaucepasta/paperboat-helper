package upload

import (
	"context"
	"testing"
	"time"
)

type cleanupMetric struct {
	name   string
	value  float64
	labels map[string]string
}

func (m *cleanupMetric) Record(name string, value float64, labels map[string]string) error {
	m.name, m.value, m.labels = name, value, labels
	return nil
}

func TestCleanupEmitsPreservedMetricWhenNothingExpires(t *testing.T) {
	metric := &cleanupMetric{}
	stager, err := New(Config{Root: t.TempDir(), Clock: &fixedClock{now: time.Now()}, Metrics: metric})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stager.Cleanup(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if metric.name != "paperboat_helper_cleanup_total" || metric.value != 1 || metric.labels["kind"] != "upload" || metric.labels["result"] != "preserved" {
		t.Fatalf("metric = %+v", metric)
	}
}
