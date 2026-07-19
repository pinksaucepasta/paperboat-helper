// Package configapply defines the Phase 2 configuration-application boundary.
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

// Handler is implemented by the Phase 7 configuration subsystem. Phase 2 may
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
