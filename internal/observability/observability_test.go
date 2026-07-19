package observability

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/health"
)

func TestLoggerAcceptsOnlySafeStructuredFields(t *testing.T) {
	var output bytes.Buffer
	logger, _ := NewLogger(slog.New(slog.NewJSONHandler(&output, nil)))
	event := Event{Component: "session", Operation: "attach", Result: "ok", CorrelationID: "req_123", ResourceID: "ses_123", Duration: time.Second, Bytes: 10}
	if err := logger.Log(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	encoded := output.String()
	if !strings.Contains(encoded, `"msg":"helper_event"`) || strings.Contains(encoded, "terminal bytes") {
		t.Fatalf("log=%s", encoded)
	}
	event.ResourceID = "Bearer secret token"
	if err := logger.Log(context.Background(), event); !errors.Is(err, ErrUnsafeValue) {
		t.Fatalf("err=%v", err)
	}
	if strings.Contains(output.String(), "secret token") {
		t.Fatal("unsafe value entered log")
	}
}

func TestMetricsRejectUnknownLabelsAndBoundSeries(t *testing.T) {
	registry, err := NewRegistry(DefaultDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{"component": "session", "result": "ok"}
	if err := registry.Record("paperboat_helper_operations_total", 1, labels); err != nil {
		t.Fatal(err)
	}
	if err := registry.Record("paperboat_helper_operations_total", 2, labels); err != nil {
		t.Fatal(err)
	}
	if got := registry.Snapshot(); len(got) != 1 || got[0].Value != 3 {
		t.Fatalf("series=%#v", got)
	}
	if err := registry.Record("paperboat_helper_operations_total", 1, map[string]string{"component": "session", "result": "customer_id"}); !errors.Is(err, ErrInvalidLabels) {
		t.Fatalf("err=%v", err)
	}
	if err := registry.Record("dynamic_customer_metric", 1, nil); !errors.Is(err, ErrUnknownMetric) {
		t.Fatalf("err=%v", err)
	}
}

func TestDefaultMetricVocabularyHasFixedCardinality(t *testing.T) {
	registry, err := NewRegistry(DefaultDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	valid := []struct {
		name   string
		labels map[string]string
	}{
		{"paperboat_helper_operations_total", map[string]string{"component": "activity", "result": "replayed"}},
		{"paperboat_helper_connector_retries_total", map[string]string{"transport": "quic", "result": "connected"}},
		{"paperboat_helper_terminal_events_total", map[string]string{"event": "slow_consumer"}},
		{"paperboat_helper_delivery_total", map[string]string{"kind": "preview", "result": "failed"}},
		{"paperboat_helper_cleanup_total", map[string]string{"kind": "upload", "result": "preserved"}},
	}
	for _, metric := range valid {
		if err := registry.Record(metric.name, 1, metric.labels); err != nil {
			t.Fatalf("metric=%s err=%v", metric.name, err)
		}
	}
	if len(registry.Snapshot()) != len(valid) {
		t.Fatalf("series=%#v", registry.Snapshot())
	}
	if err := registry.Record("paperboat_helper_delivery_total", 1, map[string]string{"kind": "customer_123", "result": "failed"}); !errors.Is(err, ErrInvalidLabels) {
		t.Fatalf("unbounded label err=%v", err)
	}
}

func TestDiagnosticsAreBoundedAndRejectPrivateFields(t *testing.T) {
	checked := time.Now().UTC()
	snapshot := health.Snapshot{Live: true, Version: "1.0.0", CheckedAt: checked, Capabilities: map[string]health.Capability{"terminal.v1": {State: health.Ready}}}
	encoded, err := BuildDiagnostics("1.0.0", "byod", snapshot, map[string]uint64{"activity_events": 2}, []string{"req_1"}, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "/Users/") {
		t.Fatalf("diagnostics=%s", encoded)
	}
	if _, err := BuildDiagnostics("1.0.0", "byod", snapshot, map[string]uint64{"private_path": 1}, nil, 4096); !errors.Is(err, ErrUnsafeValue) {
		t.Fatalf("err=%v", err)
	}
	if _, err := BuildDiagnostics("1.0.0", "byod", snapshot, nil, nil, 8); !errors.Is(err, ErrDiagnosticLimit) {
		t.Fatalf("err=%v", err)
	}
}
