//go:build darwin || linux

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/buildinfo"
	"github.com/pinksaucepasta/paperboat-helper/internal/runtime"
)

func runProduction(output io.Writer) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	helper, err := runtime.NewProductionHelper(ctx, buildinfo.Version, os.Getenv)
	if err != nil {
		return err
	}
	if err := helper.Start(ctx); err != nil {
		return err
	}
	fmt.Fprintln(output, "paperboat-helper ready")
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	return helper.Shutdown(shutdownCtx)
}
