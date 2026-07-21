//go:build darwin || linux

package hosted

import (
	"context"
	"os"
)

func ConfigSyncHooks(config Config, environ func(string) string) Hooks {
	command := "/usr/local/bin/paperboat-config-sync"
	if environ != nil {
		command = valueOr(environ("PAPERBOAT_CONFIG_SYNC_COMMAND"), command)
	}
	runner := ExecRunner{}
	run := func(ctx context.Context, action string) error {
		_, err := runner.Run(ctx, Command{Path: command, Args: []string{action}, Dir: config.CheckoutRoot, Env: os.Environ(), OutputLimit: config.MaxOutputBytes})
		return err
	}
	return Hooks{
		Restore: func(ctx context.Context, _ string) error { return run(ctx, "restore") },
		Flush:   func(ctx context.Context, _ string) error { return run(ctx, "save") },
	}
}
