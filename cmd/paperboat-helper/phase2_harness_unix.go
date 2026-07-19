//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/buildinfo"
	helperruntime "github.com/pinksaucepasta/paperboat-helper/internal/runtime"
)

func runPhase2Harness(path string, stdout io.Writer) error {
	config, err := helperruntime.LoadPhase2HarnessConfig(path)
	if err != nil {
		return fmt.Errorf("load phase2 harness configuration: %w", err)
	}
	harness, err := helperruntime.NewPhase2Harness(context.Background(), config, buildinfo.Version)
	if err != nil {
		return fmt.Errorf("compose phase2 harness: %w", err)
	}
	if err := harness.Start(context.Background()); err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return errors.Join(fmt.Errorf("start phase2 harness: %w", err), harness.Shutdown(shutdownCtx))
	}
	fmt.Fprintf(stdout, "paperboat-helper phase2 harness listening on %s\n", harness.HTTP().Address())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	<-ctx.Done()
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return harness.Shutdown(shutdownCtx)
}
