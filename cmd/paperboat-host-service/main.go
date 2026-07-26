//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/pinksaucepasta/paperboat-helper/internal/buildinfo"
	"github.com/pinksaucepasta/paperboat-helper/internal/hostservice"
)

func main() {
	flags := flag.NewFlagSet("paperboat-host-service", flag.ExitOnError)
	uid := flags.Int("uid", -1, "enrolled user ID")
	gid := flags.Int("gid", -1, "enrolled group ID")
	artifactPublicKey := flags.String("artifact-public-key", "", "trusted release artifact public key")
	listenAddress := flags.String("listen-address", "", "worker loopback health address")
	_ = flags.Parse(os.Args[1:])
	if os.Geteuid() != 0 || *uid < 1 || *gid < 1 || *artifactPublicKey == "" || !validLoopbackAddress(*listenAddress) || flags.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "paperboat-host-service: invalid invocation")
		os.Exit(2)
	}
	stateRoot, installRoot, socketPath := "/var/lib/paperboat", "/usr/local/libexec/paperboat", "/var/run/paperboat/host-service.sock"
	if runtime.GOOS == "darwin" {
		stateRoot, installRoot, socketPath = "/Library/Application Support/Paperboat", "/Library/PrivilegedHelperTools/Paperboat", "/var/run/paperboat/host-service.sock"
	}
	applier := hostservice.NewPlatformApplier(stateRoot + "/power-baseline.json")
	updates, err := hostservice.NewUpdateManager(hostservice.UpdateConfig{StateRoot: stateRoot, WorkerPath: installRoot + "/pbh", HostPath: installRoot + "/paperboat-host-service", PublicKey: *artifactPublicKey, CurrentVersion: buildinfo.Version, ListenAddress: *listenAddress})
	if err != nil {
		fmt.Fprintln(os.Stderr, "paperboat-host-service:", err)
		os.Exit(1)
	}
	server, err := hostservice.New(hostservice.Config{SocketPath: socketPath, StatePath: stateRoot + "/availability-policy.json", UID: *uid, GID: *gid, Applier: applier, Version: buildinfo.Version, Updates: updates})
	if err != nil {
		fmt.Fprintln(os.Stderr, "paperboat-host-service:", err)
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := server.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "paperboat-host-service:", err)
		os.Exit(1)
	}
}

func validLoopbackAddress(value string) bool {
	host, port, ok := strings.Cut(value, ":")
	if !ok || host != "127.0.0.1" || port == "" {
		return false
	}
	for _, character := range port {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}
