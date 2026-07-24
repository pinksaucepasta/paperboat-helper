package hosted

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func FromEnv(environ func(string) string) (Config, error) {
	if environ == nil {
		return Config{}, ErrInvalid
	}
	volume := valueOr(environ("PAPERBOAT_WORKSPACE"), "/workspace")
	repositoryURL := environ("PAPERBOAT_REPOSITORY_URL")
	projectDir, err := repositoryDirectory(repositoryURL, environ("PAPERBOAT_PROJECT_DIR"))
	if err != nil {
		return Config{}, err
	}
	operationTimeout, err := durationFromEnv(environ("PAPERBOAT_HOSTED_OPERATION_TIMEOUT_SECONDS"), 5*time.Minute)
	if err != nil {
		return Config{}, err
	}
	flushTimeout, err := durationFromEnv(environ("PAPERBOAT_CONFIG_SHUTDOWN_DEADLINE_SECONDS"), 30*time.Second)
	if err != nil {
		return Config{}, err
	}
	maxScriptBytes, err := integerFromEnv(environ("PAPERBOAT_HOSTED_MAX_SCRIPT_BYTES"), 1<<20)
	if err != nil {
		return Config{}, err
	}
	maxOutputBytes, err := integerFromEnv(environ("PAPERBOAT_HOSTED_MAX_OUTPUT_BYTES"), 256<<10)
	if err != nil {
		return Config{}, err
	}
	presets, err := loadPresets(valueOr(environ("PAPERBOAT_PRESET_DIR"), "/etc/paperboat/presets.d"), splitValues(environ("PAPERBOAT_PRESET_CODES")), maxScriptBytes)
	if err != nil {
		return Config{}, err
	}
	config := Config{
		VolumeRoot: volume, CheckoutRoot: filepath.Join(volume, projectDir), ProjectID: environ("PAPERBOAT_PROJECT_ID"),
		RepositoryURL: repositoryURL, Branch: valueOr(environ("PAPERBOAT_DEFAULT_BRANCH"), "main"),
		AllowedRepositoryHosts: splitValues(valueOr(environ("PAPERBOAT_REPOSITORY_HOSTS"), "github.com")),
		GitPath:                valueOr(environ("PAPERBOAT_GIT_PATH"), "/usr/bin/git"), ShellPath: valueOr(environ("PAPERBOAT_SHELL_PATH"), "/bin/sh"),
		Presets: presets, OperationTimeout: operationTimeout, FlushTimeout: flushTimeout,
		MaxScriptBytes: maxScriptBytes, MaxOutputBytes: maxOutputBytes,
	}
	if err := validate(config); err != nil {
		return Config{}, err
	}
	return config, nil
}

func loadPresets(directory string, codes []string, maxBytes int) ([]Script, error) {
	if !filepath.IsAbs(directory) {
		return nil, ErrInvalid
	}
	result := make([]Script, 0, len(codes))
	total := 0
	for _, code := range codes {
		if !safePresetName(code) {
			return nil, ErrInvalid
		}
		path := filepath.Join(directory, code+".sh")
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 || info.Size() < 1 || info.Size() > int64(maxBytes-total) {
			return nil, ErrInvalid
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		total += len(body)
		result = append(result, Script{Name: code, Body: string(body)})
	}
	return result, nil
}

func repositoryDirectory(rawURL, override string) (string, error) {
	if override != "" {
		if !safePresetName(override) || override == ".paperboat" {
			return "", ErrRepository
		}
		return override, nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", ErrRepository
	}
	base := strings.TrimSuffix(filepath.Base(strings.TrimSuffix(u.Path, "/")), ".git")
	if !safePresetName(base) || base == ".paperboat" {
		return "", ErrRepository
	}
	return base, nil
}
func safePresetName(value string) bool {
	if len(value) < 1 || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return value != "." && value != ".."
}
func safeEnvironmentName(value string) bool {
	if value == "" {
		return false
	}
	for index, r := range value {
		if !(r >= 'A' && r <= 'Z' || r == '_' || index > 0 && r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}
func splitValues(value string) []string {
	var result []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, strings.ToLower(item))
		}
	}
	return result
}
func valueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}
func integerFromEnv(value string, fallback int) (int, error) {
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("hosted integer: %w", ErrInvalid)
	}
	return parsed, nil
}
func durationFromEnv(value string, fallback time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	seconds, err := strconv.ParseInt(value, 10, 32)
	if err != nil || seconds <= 0 {
		return 0, fmt.Errorf("hosted duration: %w", ErrInvalid)
	}
	duration := time.Duration(seconds) * time.Second
	if duration <= 0 {
		return 0, errors.New("hosted duration overflow")
	}
	return duration, nil
}
