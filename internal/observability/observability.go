package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/health"
)

var (
	ErrUnsafeValue     = errors.New("unsafe observability value")
	ErrUnknownMetric   = errors.New("unknown metric")
	ErrInvalidLabels   = errors.New("invalid metric labels")
	ErrDiagnosticLimit = errors.New("diagnostic size limit")
)

type Event struct {
	Component     string
	Operation     string
	Result        string
	ErrorCode     string
	CorrelationID string
	ResourceID    string
	Duration      time.Duration
	Bytes         uint64
	Count         uint64
}
type Logger struct{ logger *slog.Logger }

func NewLogger(logger *slog.Logger) (*Logger, error) {
	if logger == nil {
		return nil, ErrUnsafeValue
	}
	return &Logger{logger: logger}, nil
}
func (l *Logger) Log(ctx context.Context, event Event) error {
	if event.Component == "" || event.Operation == "" || event.Result == "" {
		return ErrUnsafeValue
	}
	for _, value := range []string{event.Component, event.Operation, event.Result, event.ErrorCode, event.CorrelationID, event.ResourceID} {
		if value != "" && !safeValue(value) {
			return ErrUnsafeValue
		}
	}
	attributes := []any{"component", event.Component, "operation", event.Operation, "result", event.Result, "duration_ms", event.Duration.Milliseconds(), "bytes", event.Bytes, "count", event.Count}
	if event.ErrorCode != "" {
		attributes = append(attributes, "error_code", event.ErrorCode)
	}
	if event.CorrelationID != "" {
		attributes = append(attributes, "correlation_id", event.CorrelationID)
	}
	if event.ResourceID != "" {
		attributes = append(attributes, "resource_id", event.ResourceID)
	}
	l.logger.InfoContext(ctx, "helper_event", attributes...)
	return nil
}

type Kind string

const (
	Counter Kind = "counter"
	Gauge   Kind = "gauge"
)

type Descriptor struct {
	Name   string
	Kind   Kind
	Labels map[string]map[string]bool
}
type Series struct {
	Name   string
	Labels map[string]string
	Value  float64
}
type Registry struct {
	mu             sync.Mutex
	descriptors    map[string]Descriptor
	series         map[string]Series
	terminalFrames [2]atomic.Uint64
	terminalBytes  [2]atomic.Uint64
	terminalNanos  [2]atomic.Uint64
}

func NewRegistry(descriptors []Descriptor) (*Registry, error) {
	registry := &Registry{descriptors: make(map[string]Descriptor), series: make(map[string]Series)}
	for _, descriptor := range descriptors {
		if !safeMetricName(descriptor.Name) || descriptor.Kind != Counter && descriptor.Kind != Gauge || registry.descriptors[descriptor.Name].Name != "" {
			return nil, ErrUnknownMetric
		}
		for label, values := range descriptor.Labels {
			if !safeMetricName(label) || len(values) == 0 {
				return nil, ErrInvalidLabels
			}
			for value := range values {
				if !safeValue(value) {
					return nil, ErrInvalidLabels
				}
			}
		}
		registry.descriptors[descriptor.Name] = descriptor
	}
	return registry, nil
}
func (r *Registry) Record(name string, value float64, labels map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	descriptor, ok := r.descriptors[name]
	if !ok {
		return ErrUnknownMetric
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return ErrInvalidLabels
	}
	if len(labels) != len(descriptor.Labels) {
		return ErrInvalidLabels
	}
	keys := make([]string, 0, len(labels))
	for label, labelValue := range labels {
		allowed, ok := descriptor.Labels[label]
		if !ok || !allowed[labelValue] {
			return ErrInvalidLabels
		}
		keys = append(keys, label)
	}
	sort.Strings(keys)
	var key strings.Builder
	key.WriteString(name)
	copied := make(map[string]string, len(labels))
	for _, label := range keys {
		key.WriteByte('|')
		key.WriteString(label)
		key.WriteByte('=')
		key.WriteString(labels[label])
		copied[label] = labels[label]
	}
	series := r.series[key.String()]
	series.Name = name
	series.Labels = copied
	if descriptor.Kind == Counter {
		if value < 0 {
			return ErrInvalidLabels
		}
		series.Value += value
	} else {
		series.Value = value
	}
	r.series[key.String()] = series
	return nil
}
func (r *Registry) Snapshot() []Series {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Series, 0, len(r.series))
	for _, series := range r.series {
		copyLabels := make(map[string]string, len(series.Labels))
		for key, value := range series.Labels {
			copyLabels[key] = value
		}
		series.Labels = copyLabels
		result = append(result, series)
	}
	for index, stage := range []string{"socket_to_pty", "pty_to_socket"} {
		frames := r.terminalFrames[index].Load()
		if frames == 0 {
			continue
		}
		direction := []string{"input", "output"}[index]
		result = append(result,
			Series{Name: "paperboat_helper_terminal_frames_total", Labels: map[string]string{"direction": direction}, Value: float64(frames)},
			Series{Name: "paperboat_helper_terminal_bytes_total", Labels: map[string]string{"direction": direction}, Value: float64(r.terminalBytes[index].Load())},
			Series{Name: "paperboat_helper_terminal_stage_nanoseconds_total", Labels: map[string]string{"stage": stage}, Value: float64(r.terminalNanos[index].Load())},
		)
	}
	sort.Slice(result, func(i, j int) bool { return seriesKey(result[i]) < seriesKey(result[j]) })
	return result
}

// RecordTerminalStage is allocation-free and lock-free for the terminal hot path.
func (r *Registry) RecordTerminalStage(stage string, duration time.Duration, bytes int) {
	index := -1
	switch stage {
	case "socket_to_pty":
		index = 0
	case "pty_to_socket":
		index = 1
	}
	if index < 0 || duration < 0 || bytes < 0 {
		return
	}
	r.terminalFrames[index].Add(1)
	r.terminalBytes[index].Add(uint64(bytes))
	r.terminalNanos[index].Add(uint64(duration))
}

func DefaultDescriptors() []Descriptor {
	return []Descriptor{
		{Name: "paperboat_helper_operations_total", Kind: Counter, Labels: map[string]map[string]bool{"component": set("protocol", "auth", "session", "upload", "preview", "connector", "update", "service", "storage"), "result": set("ok", "replayed", "rejected", "conflict", "canceled", "deadline", "unavailable")}},
		{Name: "paperboat_helper_active_resources", Kind: Gauge, Labels: map[string]map[string]bool{"kind": set("sessions", "attachments", "processes", "uploads", "previews", "connectors")}},
		{Name: "paperboat_helper_readiness", Kind: Gauge, Labels: map[string]map[string]bool{"capability": set("terminal", "upload", "preview", "health", "connector", "update"), "state": set("ready", "degraded", "unavailable")}},
		{Name: "paperboat_helper_connector_retries_total", Kind: Counter, Labels: map[string]map[string]bool{"transport": set("quic", "tcp_dedicated", "tcp_mux", "none"), "result": set("connected", "failed", "replaced", "canceled")}},
		{Name: "paperboat_helper_restart_total", Kind: Counter},
		{Name: "paperboat_helper_renewal_failures_total", Kind: Counter},
		{Name: "paperboat_helper_connector_recovery_seconds", Kind: Gauge},
		{Name: "paperboat_helper_update_rollbacks_total", Kind: Counter},
		{Name: "paperboat_helper_terminal_events_total", Kind: Counter, Labels: map[string]map[string]bool{"event": set("replay_gap", "slow_consumer", "input_uncertain", "helper_restart")}},
		{Name: "paperboat_helper_terminal_persistence_failures_total", Kind: Counter},
		{Name: "paperboat_helper_terminal_persistence_lag_bytes", Kind: Gauge},
		{Name: "paperboat_helper_terminal_frames_total", Kind: Counter, Labels: map[string]map[string]bool{"direction": set("input", "output")}},
		{Name: "paperboat_helper_terminal_bytes_total", Kind: Counter, Labels: map[string]map[string]bool{"direction": set("input", "output")}},
		{Name: "paperboat_helper_terminal_stage_nanoseconds_total", Kind: Counter, Labels: map[string]map[string]bool{"stage": set("socket_to_pty", "pty_to_socket")}},
		{Name: "paperboat_helper_delivery_total", Kind: Counter, Labels: map[string]map[string]bool{"kind": set("runtime", "preview"), "result": set("delivered", "failed", "canceled")}},
		{Name: "paperboat_helper_cleanup_total", Kind: Counter, Labels: map[string]map[string]bool{"kind": set("upload", "update", "session"), "result": set("removed", "preserved", "failed")}},
	}
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		writer.Header().Set("Cache-Control", "no-store")
		for _, series := range r.Snapshot() {
			_, _ = writer.Write([]byte(series.Name))
			if len(series.Labels) > 0 {
				_, _ = writer.Write([]byte("{"))
				keys := make([]string, 0, len(series.Labels))
				for key := range series.Labels {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				for index, key := range keys {
					if index > 0 {
						_, _ = writer.Write([]byte(","))
					}
					_, _ = writer.Write([]byte(key + "=" + strconv.Quote(series.Labels[key])))
				}
				_, _ = writer.Write([]byte("}"))
			}
			_, _ = writer.Write([]byte(" " + strconv.FormatFloat(series.Value, 'g', -1, 64) + "\n"))
		}
	})
}

type Diagnostics struct {
	Version        string                       `json:"version"`
	Profile        string                       `json:"profile"`
	CheckedAt      time.Time                    `json:"checked_at"`
	Live           bool                         `json:"live"`
	Capabilities   map[string]health.Capability `json:"capabilities"`
	Queues         map[string]uint64            `json:"queues"`
	CorrelationIDs []string                     `json:"correlation_ids,omitempty"`
}

func BuildDiagnostics(version, profile string, snapshot health.Snapshot, queues map[string]uint64, correlationIDs []string, maxBytes int) ([]byte, error) {
	if !safeValue(version) || !safeValue(profile) || maxBytes < 1 {
		return nil, ErrUnsafeValue
	}
	allowedQueues := map[string]bool{"attachment_bytes": true, "cleanup_backlog": true, "connector_retries": true, "update_pending": true}
	copyQueues := make(map[string]uint64, len(queues))
	for key, value := range queues {
		if !allowedQueues[key] {
			return nil, ErrUnsafeValue
		}
		copyQueues[key] = value
	}
	ids := make([]string, 0, len(correlationIDs))
	for _, id := range correlationIDs {
		if !safeValue(id) {
			return nil, ErrUnsafeValue
		}
		ids = append(ids, id)
	}
	capabilities := make(map[string]health.Capability, len(snapshot.Capabilities))
	for name, capability := range snapshot.Capabilities {
		validState := capability.State == health.Ready || capability.State == health.Degraded || capability.State == health.Unavailable
		if !safeValue(name) || !validState || capability.Reason != "" && !safeValue(capability.Reason) {
			return nil, ErrUnsafeValue
		}
		capabilities[name] = capability
	}
	diagnostics := Diagnostics{Version: version, Profile: profile, CheckedAt: snapshot.CheckedAt, Live: snapshot.Live, Capabilities: capabilities, Queues: copyQueues, CorrelationIDs: ids}
	encoded, err := json.Marshal(diagnostics)
	if err != nil {
		return nil, err
	}
	if len(encoded) > maxBytes {
		return nil, ErrDiagnosticLimit
	}
	return encoded, nil
}

func safeValue(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("_.:-", character)) {
			return false
		}
	}
	return true
}
func safeMetricName(value string) bool {
	if !safeValue(value) {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character == '_') {
			return false
		}
	}
	return true
}
func set(values ...string) map[string]bool {
	result := make(map[string]bool, len(values))
	for _, value := range values {
		result[value] = true
	}
	return result
}
func seriesKey(series Series) string {
	encoded, _ := json.Marshal(series.Labels)
	return fmt.Sprintf("%s:%s", series.Name, encoded)
}
