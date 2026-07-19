package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pinksaucepasta/paperboat-helper/internal/configapply"
)

func TestDispatcherConfigApplyConformance(t *testing.T) {
	dispatcher := &Dispatcher{config: DispatcherConfig{ConfigApply: configapply.ConformanceHandler{}}}
	capabilities := dispatcher.Capabilities()
	if !contains(capabilities, "config.apply.v1") {
		t.Fatalf("capabilities = %v", capabilities)
	}

	authorization := Authorization{ResourceID: "asg_test_01"}
	stale := dispatcher.Handle(context.Background(), authorization, "config.apply.v1", json.RawMessage(`{"action":"apply","assignment_id":"asg_test_01","expected_revision":"rev_1","observed_revision":"rev_2"}`))
	if stale.ErrorCode != "config_revision_conflict" {
		t.Fatalf("error = %q", stale.ErrorCode)
	}

	accepted := dispatcher.Handle(context.Background(), authorization, "config.apply.v1", json.RawMessage(`{"action":"apply","assignment_id":"asg_test_01","expected_revision":"rev_2","observed_revision":"rev_2"}`))
	if accepted.ErrorCode != "" || !json.Valid(accepted.Result) {
		t.Fatalf("outcome = %+v", accepted)
	}
	var result configapply.Result
	if err := json.Unmarshal(accepted.Result, &result); err != nil || result.Applied {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
}

func TestDispatcherConfigApplyHidesOtherAssignments(t *testing.T) {
	dispatcher := &Dispatcher{config: DispatcherConfig{ConfigApply: configapply.ConformanceHandler{}}}
	outcome := dispatcher.Handle(context.Background(), Authorization{ResourceID: "asg_other"}, "config.apply.v1", json.RawMessage(`{"action":"apply","assignment_id":"asg_test_01","expected_revision":"rev_2","observed_revision":"rev_2"}`))
	if outcome.ErrorCode != "not_found_or_forbidden" {
		t.Fatalf("error = %q", outcome.ErrorCode)
	}
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
