package connector

import (
	"errors"
	"time"
)

var (
	ErrRunIDInvalid    = errors.New("invalid connector run id")
	ErrRunIDRevoked    = errors.New("connector run id revoked")
	ErrRunIDExpired    = errors.New("connector run id expired")
	ErrRunIDMismatch   = errors.New("connector run id mismatch")
	ErrRunIDGeneration = errors.New("connector run id generation mismatch")
)

// RunIDState is the edge-owned policy needed to resume frp control reconnects.
// It contains no credential or route data.
type RunIDState struct {
	RunID      string
	Generation uint64
	ExpiresAt  time.Time
	Revoked    bool
}

func (s RunIDState) Resume(runID string, generation uint64, now time.Time) error {
	if s.RunID == "" || runID == "" || s.Generation == 0 || generation == 0 {
		return ErrRunIDInvalid
	}
	if s.Revoked {
		return ErrRunIDRevoked
	}
	if !s.ExpiresAt.After(now) {
		return ErrRunIDExpired
	}
	if generation != s.Generation {
		return ErrRunIDGeneration
	}
	if runID != s.RunID {
		return ErrRunIDMismatch
	}
	return nil
}
