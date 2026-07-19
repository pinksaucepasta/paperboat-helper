package observability

import (
	"context"
	"testing"
	"time"
)

type resourceSource map[string]uint64

func (s resourceSource) ResourceCounts() map[string]uint64 { return s }

type notifyingResourceSource chan struct{}

func (s notifyingResourceSource) ResourceCounts() map[string]uint64 {
	s <- struct{}{}
	return map[string]uint64{"sessions": 1}
}

func TestResourceSamplerRecordsAndStops(t *testing.T) {
	registry, err := NewRegistry(DefaultDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	sampler, err := NewResourceSampler(ResourceSamplerConfig{Sources: []ResourceSource{resourceSource{"sessions": 2, "attachments": 3}, resourceSource{"connectors": 1, "customer_id": 99}}, Metrics: registry, Interval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := sampler.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := sampler.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"sessions": 2, "attachments": 3, "processes": 0, "uploads": 0, "previews": 0, "connectors": 1}
	for _, series := range registry.Snapshot() {
		if series.Name == "paperboat_helper_active_resources" {
			if series.Value != want[series.Labels["kind"]] {
				t.Fatalf("series = %+v", series)
			}
			delete(want, series.Labels["kind"])
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing series: %v", want)
	}
}

func TestResourceSamplerOwnsLifetimeAfterStart(t *testing.T) {
	registry, _ := NewRegistry(DefaultDescriptors())
	calls := make(notifyingResourceSource, 2)
	sampler, _ := NewResourceSampler(ResourceSamplerConfig{Sources: []ResourceSource{calls}, Metrics: registry, Interval: time.Millisecond})
	startCtx, cancelStart := context.WithCancel(context.Background())
	if err := sampler.Start(startCtx); err != nil {
		t.Fatal(err)
	}
	<-calls
	cancelStart()
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("sampler stopped with startup context")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := sampler.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
