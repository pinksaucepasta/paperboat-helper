package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/health"
	"github.com/pinksaucepasta/paperboat-helper/internal/testleak"
)

type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

type service struct {
	name                  string
	recorder              *recorder
	startErr, shutdownErr error
}

type dynamicHealthService struct {
	service
	capability health.Capability
}

func (s *dynamicHealthService) CapabilityHealth() health.Capability { return s.capability }

func (s *service) Start(context.Context) error { s.recorder.add("start:" + s.name); return s.startErr }
func (s *service) Shutdown(context.Context) error {
	s.recorder.add("stop:" + s.name)
	return s.shutdownErr
}

func TestRuntimeStartsInOrderAndStopsInReverse(t *testing.T) {
	rec := &recorder{}
	runtime, err := NewRuntime(Config{Version: "1", Components: []Component{{"storage", true, &service{"storage", rec, nil, nil}}, {"terminal", true, &service{"terminal", rec, nil, nil}}, {"admission", true, &service{"admission", rec, nil, nil}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"start:storage", "start:terminal", "start:admission", "stop:admission", "stop:terminal", "stop:storage"}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.events) != len(want) {
		t.Fatalf("events=%v", rec.events)
	}
	for i := range want {
		if rec.events[i] != want[i] {
			t.Fatalf("events=%v", rec.events)
		}
	}
}

func TestRequiredFailureRollsBackButOptionalFailureDegradesOnlyCapability(t *testing.T) {
	rec := &recorder{}
	runtime, _ := NewRuntime(Config{Version: "1", Components: []Component{{"storage", true, &service{"storage", rec, nil, nil}}, {"preview", false, &service{"preview", rec, errors.New("offline"), nil}}, {"terminal", true, &service{"terminal", rec, nil, nil}}}})
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := runtime.Health()
	if !snapshot.Live || snapshot.Capabilities["preview"].State != "unavailable" || snapshot.Capabilities["terminal"].State != "ready" {
		t.Fatalf("health=%#v", snapshot)
	}
	_ = runtime.Shutdown(context.Background())
	rec = &recorder{}
	runtime, _ = NewRuntime(Config{Version: "1", Components: []Component{{"storage", true, &service{"storage", rec, nil, nil}}, {"terminal", true, &service{"terminal", rec, errors.New("boom"), nil}}}})
	if err := runtime.Start(context.Background()); err == nil {
		t.Fatal("expected start failure")
	}
	if runtime.State() != Failed || runtime.Health().Live {
		t.Fatalf("state=%s health=%#v", runtime.State(), runtime.Health())
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	want := []string{"start:storage", "start:terminal", "stop:terminal", "stop:storage"}
	for i := range want {
		if rec.events[i] != want[i] {
			t.Fatalf("events=%v", rec.events)
		}
	}
}

func TestRuntimeHealthSourceUsesCurrentComponentHealth(t *testing.T) {
	rec := &recorder{}
	connector := &dynamicHealthService{
		service:    service{name: "edge", recorder: rec},
		capability: health.Capability{State: health.Ready},
	}
	components := []Component{{Capability: "edge", Required: true, Service: connector}}
	runtime, err := NewRuntime(Config{Version: "1", Components: components})
	if err != nil {
		t.Fatal(err)
	}
	source := &runtimeHealthSource{}
	source.set(runtime, components)
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	connector.capability = health.Capability{State: health.Unavailable, Reason: "connector_unavailable", RetryAfterMs: 1000}
	got := source.Snapshot().Capabilities["edge"]
	if got.State != health.Unavailable || got.Reason != "connector_unavailable" || got.RetryAfterMs != 1000 {
		t.Fatalf("edge health = %#v", got)
	}
}

func TestRuntimeCanBeConstructedStartedAndStoppedRepeatedly(t *testing.T) {
	baseline, err := testleak.Take()
	if err != nil {
		t.Skipf("leak accounting unavailable: %v", err)
	}
	for i := 0; i < 100; i++ {
		rec := &recorder{}
		runtime, err := NewRuntime(Config{Version: "1", ShutdownTimeout: time.Second, Components: []Component{{"health", true, &service{"health", rec, nil, nil}}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := runtime.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := runtime.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := testleak.WaitForBaseline(baseline, 2*time.Second); err != nil {
		t.Fatal(err)
	}
}
