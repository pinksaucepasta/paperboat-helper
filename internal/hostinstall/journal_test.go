//go:build darwin || linux

package hostinstall

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRollbackFilesHandlesEveryActivationBoundary(t *testing.T) {
	for _, test := range []struct {
		name                 string
		hadWorker, hadHost   bool
		workerCurrent        string
		workerRollback       string
		hostCurrent          string
		hostRollback         string
		wantWorker, wantHost string
	}{
		{name: "prepared only", hadWorker: true, hadHost: true, workerCurrent: "old-worker", hostCurrent: "old-host", wantWorker: "old-worker", wantHost: "old-host"},
		{name: "worker activated", hadWorker: true, hadHost: true, workerCurrent: "new-worker", workerRollback: "old-worker", hostCurrent: "old-host", wantWorker: "old-worker", wantHost: "old-host"},
		{name: "both activated", hadWorker: true, hadHost: true, workerCurrent: "new-worker", workerRollback: "old-worker", hostCurrent: "new-host", hostRollback: "old-host", wantWorker: "old-worker", wantHost: "old-host"},
		{name: "fresh activation", workerCurrent: "new-worker", hostCurrent: "new-host"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			paths := installPaths{worker: filepath.Join(root, "worker"), host: filepath.Join(root, "host"), workerRollback: filepath.Join(root, "worker.rollback"), hostRollback: filepath.Join(root, "host.rollback"), workerNext: filepath.Join(root, "worker.next"), hostNext: filepath.Join(root, "host.next"), journal: filepath.Join(root, "journal")}
			write := func(path, value string) {
				if value != "" {
					if err := os.WriteFile(path, []byte(value), 0o755); err != nil {
						t.Fatal(err)
					}
				}
			}
			write(paths.worker, test.workerCurrent)
			write(paths.workerRollback, test.workerRollback)
			write(paths.host, test.hostCurrent)
			write(paths.hostRollback, test.hostRollback)
			if err := rollbackFiles(paths, installJournal{HadWorker: test.hadWorker, HadHost: test.hadHost}); err != nil {
				t.Fatal(err)
			}
			assertFile := func(path, want string) {
				body, err := os.ReadFile(path)
				if want == "" {
					if !os.IsNotExist(err) {
						t.Fatalf("%s remains: %q err=%v", path, body, err)
					}
					return
				}
				if err != nil || string(body) != want {
					t.Fatalf("%s=%q err=%v want=%q", path, body, err, want)
				}
			}
			assertFile(paths.worker, test.wantWorker)
			assertFile(paths.host, test.wantHost)
		})
	}
}
