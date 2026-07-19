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
