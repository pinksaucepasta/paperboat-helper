// Package configapply defines the configuration-application boundary.
package configapply

import (
	"context"
	"errors"
)

var (
	ErrInvalidRequest   = errors.New("invalid config application request")
	ErrRevisionConflict = errors.New("config revision conflict")
)

type Request struct {
	Action           string
	AssignmentID     string
	ExpectedRevision string
	ObservedRevision string
}

type Result struct {
	AssignmentID string `json:"assignment_id"`
	Revision     string `json:"revision"`
	Applied      bool   `json:"applied"`
}

// Handler is implemented by the configuration subsystem. Callers may
// inject ConformanceHandler to exercise the protocol without writing files.
type Handler interface {
	Handle(context.Context, Request) (Result, error)
}

// ConformanceHandler validates revision behavior and deliberately never writes.
type ConformanceHandler struct{}

func (ConformanceHandler) Handle(_ context.Context, request Request) (Result, error) {
	if request.Action != "pull" && request.Action != "apply" && request.Action != "report" {
		return Result{}, ErrInvalidRequest
	}
	if request.AssignmentID == "" || request.ExpectedRevision == "" || request.ObservedRevision == "" {
		return Result{}, ErrInvalidRequest
	}
	if request.ExpectedRevision != request.ObservedRevision {
		return Result{}, ErrRevisionConflict
	}
	return Result{AssignmentID: request.AssignmentID, Revision: request.ObservedRevision, Applied: false}, nil
}

// SyncHandler binds authenticated config.apply requests to the hosted config
// restore operation while preserving immutable assignment revision checks.
type SyncHandler struct {
	Apply func(context.Context) error
}

func (h SyncHandler) Handle(ctx context.Context, request Request) (Result, error) {
	if request.Action != "pull" && request.Action != "apply" && request.Action != "report" {
		return Result{}, ErrInvalidRequest
	}
	if request.AssignmentID == "" || request.ExpectedRevision == "" || request.ObservedRevision == "" {
		return Result{}, ErrInvalidRequest
	}
	if request.ExpectedRevision != request.ObservedRevision {
		return Result{}, ErrRevisionConflict
	}
	applied := false
	if request.Action != "report" {
		if h.Apply == nil {
			return Result{}, ErrInvalidRequest
		}
		if err := h.Apply(ctx); err != nil {
			return Result{}, err
		}
		applied = true
	}
	return Result{AssignmentID: request.AssignmentID, Revision: request.ObservedRevision, Applied: applied}, nil
}
