package connector

import (
	"errors"
	"testing"
	"time"
)

func TestRunIDResumeRequiresLiveSameGeneration(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	state := RunIDState{RunID: "run_1", Generation: 3, ExpiresAt: now.Add(time.Minute)}
	if err := state.Resume("run_1", 3, now); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		run     string
		gen     uint64
		at      time.Time
		revoked bool
		want    error
	}{
		{"different run", "run_2", 3, now, false, ErrRunIDMismatch},
		{"different generation", "run_1", 4, now, false, ErrRunIDGeneration},
		{"expired", "run_1", 3, now.Add(time.Minute), false, ErrRunIDExpired},
		{"revoked", "run_1", 3, now, true, ErrRunIDRevoked},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := state
			candidate.Revoked = test.revoked
			if err := candidate.Resume(test.run, test.gen, test.at); !errors.Is(err, test.want) {
				t.Fatalf("err=%v want=%v", err, test.want)
			}
		})
	}
}
