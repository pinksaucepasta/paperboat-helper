//go:build darwin || linux

package hosted

import (
	"context"
	"os"
	"strings"
)

func ConfigSyncHooks(config Config, environ func(string) string) Hooks {
	command := "/usr/local/bin/paperboat-config-sync"
	tokenName := ""
	token := ""
	if environ != nil {
		command = valueOr(environ("PAPERBOAT_CONFIG_SYNC_COMMAND"), command)
		tokenName = strings.TrimSpace(environ("PAPERBOAT_GITHUB_TOKEN_ENV"))
		if tokenName == "" {
			tokenName = "PAPERBOAT_GITHUB_CONFIG_TOKEN"
		}
		if safeEnvironmentName(tokenName) {
			token = environ(tokenName)
		} else {
			tokenName = ""
		}
	}
	runner := ExecRunner{}
	run := func(ctx context.Context, action string) error {
		env := os.Environ()
		if tokenName != "" && token != "" {
			env = append(env, tokenName+"="+token)
		}
		_, err := runner.Run(ctx, Command{Path: command, Args: []string{action}, Dir: config.CheckoutRoot, Env: env, OutputLimit: config.MaxOutputBytes})
		return err
	}
	return Hooks{
		Restore: func(ctx context.Context, _ string) error { return run(ctx, "restore") },
		Flush:   func(ctx context.Context, _ string) error { return run(ctx, "save") },
	}
}
