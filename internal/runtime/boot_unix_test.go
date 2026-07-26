//go:build darwin || linux

package runtime

import (
	"path/filepath"
	"testing"
)

func TestWorkerBootGenerationDistinguishesRestartAndReboot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	first, reason, err := recordWorkerBoot(root)
	if err != nil || reason != "helper_restart" || first.Generation != 1 {
		t.Fatalf("first=%+v reason=%q err=%v", first, reason, err)
	}
	second, reason, err := recordWorkerBoot(root)
	if err != nil || reason != "helper_restart" || second.Generation != 2 || second.OSBootID != first.OSBootID {
		t.Fatalf("second=%+v reason=%q err=%v", second, reason, err)
	}
	second.OSBootID = "different-boot-id"
	if err := writeWorkerBoot(filepath.Join(root, "runtime", "worker-boot.json"), second); err != nil {
		t.Fatal(err)
	}
	third, reason, err := recordWorkerBoot(root)
	if err != nil || reason != "machine_reboot" || third.Generation != 3 || third.OSBootID != first.OSBootID {
		t.Fatalf("third=%+v reason=%q err=%v", third, reason, err)
	}
}
