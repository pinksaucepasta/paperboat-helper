//go:build darwin

package hostservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type pmsetBaseline struct {
	Schema string         `json:"schema"`
	Values map[string]int `json:"values"`
}

type platformApplier struct{ baselinePath string }

func NewPlatformApplier(baselinePath string) Applier {
	return &platformApplier{baselinePath: baselinePath}
}

func (a *platformApplier) Apply(ctx context.Context, mode string) error {
	if mode == KeepAwake {
		if err := a.captureBaseline(ctx); err != nil {
			return err
		}
		if err := fixedCommand(ctx, "/usr/bin/pmset", "-a", "disablesleep", "1"); err != nil {
			return err
		}
		values, err := readPMSet(ctx)
		if err != nil || !allValues(values, 1) {
			return errors.New("pmset did not apply keep_awake")
		}
		return nil
	}
	baseline, err := a.loadBaseline()
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, item := range []struct{ source, flag string }{{"Battery Power", "-b"}, {"AC Power", "-c"}, {"UPS Power", "-u"}} {
		value, ok := baseline.Values[item.source]
		if !ok {
			continue
		}
		if err := fixedCommand(ctx, "/usr/bin/pmset", item.flag, "disablesleep", strconv.Itoa(value)); err != nil {
			return err
		}
	}
	values, err := readPMSet(ctx)
	if err != nil {
		return err
	}
	for source, value := range baseline.Values {
		if values[source] != value {
			return errors.New("pmset baseline restoration failed")
		}
	}
	return os.Remove(a.baselinePath)
}

func (a *platformApplier) Close(context.Context) error { return nil }

func (a *platformApplier) captureBaseline(ctx context.Context) error {
	if _, err := os.Lstat(a.baselinePath); err == nil {
		_, err = a.loadBaseline()
		return err
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	values, err := readPMSet(ctx)
	if err != nil {
		return err
	}
	body, _ := json.Marshal(pmsetBaseline{Schema: ProtocolV1, Values: values})
	directory := filepath.Dir(a.baselinePath)
	if err := secureDirectory(directory, 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(a.baselinePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(body); err == nil {
		err = file.Sync()
	}
	return errors.Join(err, file.Close())
}

func (a *platformApplier) loadBaseline() (pmsetBaseline, error) {
	info, err := os.Lstat(a.baselinePath)
	if err != nil {
		return pmsetBaseline{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() > 4096 {
		return pmsetBaseline{}, ErrInvalidConfig
	}
	body, err := os.ReadFile(a.baselinePath)
	if err != nil {
		return pmsetBaseline{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var baseline pmsetBaseline
	if decoder.Decode(&baseline) != nil || baseline.Schema != ProtocolV1 || len(baseline.Values) == 0 {
		return pmsetBaseline{}, ErrInvalidConfig
	}
	return baseline, nil
}

func readPMSet(ctx context.Context) (map[string]int, error) {
	command := exec.CommandContext(ctx, "/usr/bin/pmset", "-g", "custom")
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		return nil, err
	}
	values := make(map[string]int)
	sections := make(map[string]bool)
	section := ""
	for _, line := range strings.Split(output.String(), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasSuffix(trimmed, ":") {
			candidate := strings.TrimSuffix(trimmed, ":")
			if candidate == "Battery Power" || candidate == "AC Power" || candidate == "UPS Power" {
				section = candidate
				sections[section] = true
			} else {
				section = ""
			}
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) == 2 && fields[0] == "disablesleep" {
			value, err := strconv.Atoi(fields[1])
			if err != nil || value != 0 && value != 1 {
				return nil, ErrInvalidConfig
			}
			values[section] = value
		}
	}
	for source := range sections {
		if _, ok := values[source]; !ok {
			values[source] = 0
		}
	}
	if len(values) == 0 {
		return nil, ErrInvalidConfig
	}
	return values, nil
}
func allValues(values map[string]int, want int) bool {
	if len(values) == 0 {
		return false
	}
	for _, value := range values {
		if value != want {
			return false
		}
	}
	return true
}
func fixedCommand(ctx context.Context, path string, args ...string) error {
	command := exec.CommandContext(ctx, path, args...)
	return command.Run()
}
