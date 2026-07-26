//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/pinksaucepasta/paperboat-helper/internal/hostinstall"
)

func runServiceCommand(ctx context.Context, args []string, stdin io.Reader, _, _ io.Writer) error {
	if len(args) != 1 || args[0] != "install" && args[0] != "commit" && args[0] != "uninstall" && args[0] != "uninstall-persisted" {
		return errors.New("service requires install, commit, or uninstall")
	}
	if args[0] == "uninstall" && os.Geteuid() != 0 {
		if err := authorizePersistedUninstall(ctx); err != nil {
			return err
		}
		return removeSystemHelperCommand()
	}
	if os.Geteuid() != 0 {
		return hostinstall.ErrNotPrivileged
	}
	if args[0] == "uninstall-persisted" {
		return hostinstall.UninstallPersisted(ctx)
	}
	request, err := hostinstall.Decode(stdin)
	if err != nil {
		return err
	}
	if args[0] == "uninstall" {
		return hostinstall.Uninstall(ctx, request)
	}
	if args[0] == "commit" {
		return hostinstall.Commit(request)
	}
	return hostinstall.Install(ctx, request)
}

func authorizePersistedUninstall(ctx context.Context) error {
	executable := systemWorkerExecutable()
	command := exec.CommandContext(ctx, "/usr/bin/sudo", "--", executable, "service", "uninstall-persisted")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("administrator approval or service removal failed: %w: %s", err, stderr.String())
	}
	return nil
}

func systemWorkerExecutable() string {
	if runtime.GOOS == "darwin" {
		return "/Library/PrivilegedHelperTools/Paperboat/pbh"
	}
	return "/usr/local/libexec/paperboat/pbh"
}

func removeSystemHelperCommand() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	commandPath := filepath.Join(home, ".local", "bin", "pbh")
	info, err := os.Lstat(commandPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return nil
	}
	target, err := os.Readlink(commandPath)
	if err != nil {
		return err
	}
	if target != systemWorkerExecutable() {
		return nil
	}
	return os.Remove(commandPath)
}
