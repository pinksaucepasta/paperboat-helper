package health

import (
	"testing"
	"time"
)

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func TestSnapshotSeparatesLivenessFromCapabilityReadiness(t *testing.T) {
	when := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	r := New("1.0.0", []string{"terminal.v1", "health.v1"}, fixedClock{when})
	r.Set("health.v1", Ready, "", 0)
	if got := r.Snapshot(); !got.Live {
		t.Fatal("capability startup must not make process dead")
	}
	r.Set("terminal.v1", Ready, "", 0)
	got := r.Snapshot()
	if !got.Live || !got.CheckedAt.Equal(when) {
		t.Fatalf("snapshot = %#v", got)
	}
	r.SetLive(false)
	if r.Snapshot().Live {
		t.Fatal("explicit shutdown must be non-live")
	}
}
