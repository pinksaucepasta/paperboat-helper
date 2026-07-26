//go:build darwin || linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/health"
	"github.com/pinksaucepasta/paperboat-helper/internal/hostservice"
)

type doctorCheck struct {
	Code     string `json:"code"`
	State    string `json:"state"`
	Message  string `json:"message"`
	Recovery string `json:"recovery,omitempty"`
}

type doctorReport struct {
	Schema           string                `json:"schema"`
	OK               bool                  `json:"ok"`
	ServiceScope     string                `json:"service_scope"`
	WorkerGeneration uint64                `json:"worker_generation,omitempty"`
	OSBootID         string                `json:"os_boot_id,omitempty"`
	Availability     *hostservice.Response `json:"availability,omitempty"`
	Connector        *health.Capability    `json:"connector,omitempty"`
	Checks           []doctorCheck         `json:"checks"`
}

func runDoctor(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("doctor", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonOutput := flags.Bool("json", false, "print JSON")
	stateRoot := flags.String("state-root", "", "helper state directory")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return errors.New("doctor accepts --json and --state-root only")
	}
	if *stateRoot == "" {
		*stateRoot = os.Getenv("PAPERBOAT_HELPER_STATE_ROOT")
	}
	if *stateRoot == "" {
		root, err := os.UserConfigDir()
		if err != nil {
			return err
		}
		*stateRoot = filepath.Join(root, "paperboat", "helper")
	}
	report := collectDoctor(ctx, *stateRoot)
	if *jsonOutput {
		if err := json.NewEncoder(stdout).Encode(report); err != nil {
			return err
		}
		if !report.OK {
			return errors.New("one or more Paperboat diagnostics failed")
		}
		return nil
	}
	for _, check := range report.Checks {
		fmt.Fprintf(stdout, "%-12s %-28s %s\n", strings.ToUpper(check.State), check.Code, check.Message)
		if check.Recovery != "" {
			fmt.Fprintf(stdout, "             Recovery: %s\n", check.Recovery)
		}
	}
	if !report.OK {
		return errors.New("one or more Paperboat diagnostics failed")
	}
	return nil
}

func collectDoctor(ctx context.Context, stateRoot string) doctorReport {
	report := doctorReport{Schema: "paperboat.doctor/v1", OK: true, Checks: make([]doctorCheck, 0, 4)}
	add := func(code, state, message, recovery string) {
		report.Checks = append(report.Checks, doctorCheck{Code: code, State: state, Message: message, Recovery: recovery})
		if state == "error" {
			report.OK = false
		}
	}

	scope, err := systemServiceScope(ctx)
	report.ServiceScope = scope
	if err != nil {
		add("boot_service", "error", "The boot-level worker service is not active.", "Run pbh bootstrap again or inspect the system service logs.")
	} else {
		add("boot_service", "ready", "The worker is active as a system boot service.", "")
	}

	var boot struct {
		Schema     string    `json:"schema"`
		OSBootID   string    `json:"os_boot_id"`
		Generation uint64    `json:"generation"`
		StartedAt  time.Time `json:"started_at"`
	}
	if err := decodeStrictFile(filepath.Join(stateRoot, "runtime", "worker-boot.json"), 4096, &boot); err != nil || boot.Schema != "paperboat.worker-boot/v1" || boot.OSBootID == "" || boot.Generation < 1 {
		add("worker_generation", "error", "Worker boot generation is unavailable.", "Check ownership of the helper state directory, then restart the system service.")
	} else {
		report.WorkerGeneration, report.OSBootID = boot.Generation, boot.OSBootID
		add("worker_generation", "ready", fmt.Sprintf("Worker generation %d is recorded for this OS boot.", boot.Generation), "")
	}

	healthCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	listenAddress := workerListenAddress(stateRoot)
	request, _ := http.NewRequestWithContext(healthCtx, http.MethodGet, "http://"+listenAddress+"/healthz", nil)
	response, err := (&http.Client{Timeout: 3 * time.Second}).Do(request)
	var snapshot health.Snapshot
	if err != nil || response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&snapshot) != nil || !snapshot.Live {
		if response != nil {
			response.Body.Close()
		}
		add("worker_health", "error", "The local helper health endpoint is unavailable.", "Inspect journalctl -u paperboat-helper.service and restart the service.")
	} else {
		response.Body.Close()
		add("worker_health", "ready", "The local helper health endpoint is live.", "")
		if connector, ok := snapshot.Capabilities["edge"]; ok {
			copy := connector
			report.Connector = &copy
			if connector.State == health.Ready {
				add("connector_recovery", "ready", "The outbound connector is ready.", "")
			} else {
				add("connector_recovery", "warning", "The connector is retrying: "+connector.Reason+".", "Restore DNS/control-plane/tunnel reachability; recovery is automatic.")
			}
		}
	}

	host, err := hostDiagnostics(ctx)
	if err != nil {
		add("availability", "error", "The privileged host service is unavailable.", "Inspect the paperboat-host-service system logs; pbh service uninstall restores original power settings.")
	} else {
		report.Availability = &host
		state := "ready"
		if host.Status == "error" || host.ErrorCode != "" {
			state = "error"
		}
		add("availability", state, fmt.Sprintf("Desired %s version %d; observed %s version %d (%s).", host.DesiredMode, host.DesiredVersion, host.ObservedMode, host.ObservedVersion, host.Status), "Run pbh service uninstall to restore the original local power configuration.")
	}
	return report
}

func systemServiceScope(ctx context.Context) (string, error) {
	if runtime.GOOS == "linux" {
		output, err := exec.CommandContext(ctx, "/usr/bin/systemctl", "is-active", "paperboat-helper.service").Output()
		if err != nil || strings.TrimSpace(string(output)) != "active" {
			return "system", errors.New("inactive")
		}
		return "system", nil
	}
	output, err := exec.CommandContext(ctx, "/bin/launchctl", "print", "system/com.pinksaucepasta.paperboat-helper").Output()
	if err != nil || len(output) == 0 {
		return "system", errors.New("inactive")
	}
	return "system", nil
}

func hostDiagnostics(ctx context.Context) (hostservice.Response, error) {
	dialer := net.Dialer{Timeout: 3 * time.Second}
	connection, err := dialer.DialContext(ctx, "unix", "/var/run/paperboat/host-service.sock")
	if err != nil {
		return hostservice.Response{}, err
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
	if err := json.NewEncoder(connection).Encode(hostservice.Request{Schema: hostservice.ProtocolV1, Operation: "diagnostics"}); err != nil {
		return hostservice.Response{}, err
	}
	if closer, ok := connection.(interface{ CloseWrite() error }); !ok || closer.CloseWrite() != nil {
		return hostservice.Response{}, errors.New("invalid host diagnostics connection")
	}
	var response hostservice.Response
	decoder := json.NewDecoder(io.LimitReader(connection, 16<<10))
	decoder.DisallowUnknownFields()
	var extra any
	if decoder.Decode(&response) != nil || decoder.Decode(&extra) != io.EOF || response.Schema != hostservice.ProtocolV1 || response.HostServiceVersion == "" || response.Scope != "system" {
		return hostservice.Response{}, errors.New("invalid host diagnostics")
	}
	return response, nil
}

func workerListenAddress(stateRoot string) string {
	var local struct {
		Schema        string `json:"schema"`
		ListenAddress string `json:"listen_address"`
	}
	if decodeStrictFile(filepath.Join(stateRoot, "runtime", "worker-local.json"), 4096, &local) == nil && local.Schema == "paperboat.worker-local/v1" && strings.HasPrefix(local.ListenAddress, "127.0.0.1:") {
		return local.ListenAddress
	}
	return "127.0.0.1:8080"
}

func decodeStrictFile(path string, limit int64, target any) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 1 || info.Size() > limit {
		return errors.New("invalid file")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, limit+1))
	decoder.DisallowUnknownFields()
	var extra any
	if decoder.Decode(target) != nil || decoder.Decode(&extra) != io.EOF {
		return errors.New("invalid file")
	}
	return nil
}
