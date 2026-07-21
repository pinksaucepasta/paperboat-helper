package configapply

import (
	"context"
	"errors"
	"testing"
)

func TestConformanceHandler(t *testing.T) {
	handler := ConformanceHandler{}
	result, err := handler.Handle(context.Background(), Request{Action: "apply", AssignmentID: "asg_test_01", ExpectedRevision: "rev_2", ObservedRevision: "rev_2"})
	if err != nil || result.Applied || result.Revision != "rev_2" {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	_, err = handler.Handle(context.Background(), Request{Action: "apply", AssignmentID: "asg_test_01", ExpectedRevision: "rev_1", ObservedRevision: "rev_2"})
	if !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("err = %v, want revision conflict", err)
	}
}

func TestSyncHandlerAppliesMatchingRevision(t *testing.T) {
	calls := 0
	handler := SyncHandler{Apply: func(context.Context) error { calls++; return nil }}
	result, err := handler.Handle(context.Background(), Request{Action: "apply", AssignmentID: "asg_1", ExpectedRevision: "rev_1", ObservedRevision: "rev_1"})
	if err != nil || !result.Applied || calls != 1 {
		t.Fatalf("result=%+v calls=%d err=%v", result, calls, err)
	}
	if _, err := handler.Handle(context.Background(), Request{Action: "apply", AssignmentID: "asg_1", ExpectedRevision: "rev_2", ObservedRevision: "rev_1"}); !errors.Is(err, ErrRevisionConflict) || calls != 1 {
		t.Fatalf("conflict err=%v calls=%d", err, calls)
	}
	reported, err := handler.Handle(context.Background(), Request{Action: "report", AssignmentID: "asg_1", ExpectedRevision: "rev_1", ObservedRevision: "rev_1"})
	if err != nil || reported.Applied || calls != 1 {
		t.Fatalf("report=%+v calls=%d err=%v", reported, calls, err)
	}
}
