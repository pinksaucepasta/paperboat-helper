package service

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const Label = "com.pinksaucepasta.paperboat-helper"

var (
	ErrInvalidDefinition   = errors.New("invalid service definition")
	ErrUnsupportedPlatform = errors.New("unsupported service platform")
)

type Controller interface {
	Apply(context.Context, string, bool) error
	Remove(context.Context, string) error
}
type Config struct {
	Platform    string
	ConfigRoot  string
	Executable  string
	Arguments   []string
	Environment map[string]string
	Controller  Controller
}
type Installer struct {
	config         Config
	definitionPath string
}

func New(config Config) (*Installer, error) {
	if config.Controller == nil || !filepath.IsAbs(config.ConfigRoot) || !filepath.IsAbs(config.Executable) || len(config.Arguments) == 0 {
		return nil, ErrInvalidDefinition
	}
	if err := safeExecutable(config.Executable); err != nil {
		return nil, err
	}
	if !safeValues([]string{config.Executable}) {
		return nil, ErrInvalidDefinition
	}
	if !safeValues(config.Arguments) || !safeEnvironment(config.Environment) {
		return nil, ErrInvalidDefinition
	}
	var path string
	switch config.Platform {
	case "darwin":
		path = filepath.Join(config.ConfigRoot, "Library", "LaunchAgents", Label+".plist")
	case "linux":
		path = filepath.Join(config.ConfigRoot, "systemd", "user", "paperboat-helper.service")
	default:
		return nil, ErrUnsupportedPlatform
	}
	return &Installer{config: config, definitionPath: path}, nil
}

func (i *Installer) Install(ctx context.Context) error {
	if err := ensureRoot(i.config.ConfigRoot); err != nil {
		return err
	}
	definition, err := i.render()
	if err != nil {
		return err
	}
	info, statErr := os.Lstat(i.definitionPath)
	upgrading := statErr == nil
	if statErr == nil && (info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular()) {
		return ErrInvalidDefinition
	}
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	if err := atomicWrite(i.definitionPath, definition, 0o600); err != nil {
		return err
	}
	if i.config.Platform == "darwin" {
		logDirectory := filepath.Join(i.config.ConfigRoot, "Library", "Logs", "Paperboat")
		if err := os.MkdirAll(logDirectory, 0o700); err != nil {
			return err
		}
		if err := os.Chmod(logDirectory, 0o700); err != nil {
			return err
		}
	}
	return i.config.Controller.Apply(ctx, i.definitionPath, upgrading)
}
func (i *Installer) Uninstall(ctx context.Context) error {
	if err := i.config.Controller.Remove(ctx, i.definitionPath); err != nil {
		return err
	}
	if err := os.Remove(i.definitionPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(filepath.Dir(i.definitionPath))
}
func (i *Installer) DefinitionPath() string { return i.definitionPath }

func (i *Installer) render() ([]byte, error) {
	if i.config.Platform == "darwin" {
		return renderLaunchd(i.config)
	}
	return renderSystemd(i.config), nil
}

func renderLaunchd(config Config) ([]byte, error) {
	var output bytes.Buffer
	output.WriteString(xml.Header)
	output.WriteString(`<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">` + "\n")
	encoder := xml.NewEncoder(&output)
	encoder.Indent("", "  ")
	start := func(name string) error { return encoder.EncodeToken(xml.StartElement{Name: xml.Name{Local: name}}) }
	end := func(name string) error { return encoder.EncodeToken(xml.EndElement{Name: xml.Name{Local: name}}) }
	text := func(element, value string) error {
		if err := start(element); err != nil {
			return err
		}
		if err := encoder.EncodeToken(xml.CharData(value)); err != nil {
			return err
		}
		return end(element)
	}
	if err := encoder.EncodeToken(xml.StartElement{Name: xml.Name{Local: "plist"}, Attr: []xml.Attr{{Name: xml.Name{Local: "version"}, Value: "1.0"}}}); err != nil {
		return nil, err
	}
	if err := start("dict"); err != nil {
		return nil, err
	}
	logPath := filepath.Join(config.ConfigRoot, "Library", "Logs", "Paperboat", "paperboat-helper.log")
	pairs := [][2]string{{"Label", Label}, {"ProcessType", "Background"}, {"StandardOutPath", logPath}, {"StandardErrorPath", logPath}}
	for _, pair := range pairs {
		if err := text("key", pair[0]); err != nil {
			return nil, err
		}
		if err := text("string", pair[1]); err != nil {
			return nil, err
		}
	}
	if err := text("key", "ProgramArguments"); err != nil {
		return nil, err
	}
	if err := start("array"); err != nil {
		return nil, err
	}
	for _, argument := range append([]string{config.Executable}, config.Arguments...) {
		if err := text("string", argument); err != nil {
			return nil, err
		}
	}
	if err := end("array"); err != nil {
		return nil, err
	}
	if err := text("key", "EnvironmentVariables"); err != nil {
		return nil, err
	}
	if err := start("dict"); err != nil {
		return nil, err
	}
	keys := sortedKeys(config.Environment)
	for _, key := range keys {
		if err := text("key", key); err != nil {
			return nil, err
		}
		if err := text("string", config.Environment[key]); err != nil {
			return nil, err
		}
	}
	if err := end("dict"); err != nil {
		return nil, err
	}
	for _, key := range []string{"RunAtLoad", "KeepAlive"} {
		if err := text("key", key); err != nil {
			return nil, err
		}
		if err := start("true"); err != nil {
			return nil, err
		}
		if err := end("true"); err != nil {
			return nil, err
		}
	}
	if err := end("dict"); err != nil {
		return nil, err
	}
	if err := end("plist"); err != nil {
		return nil, err
	}
	if err := encoder.Flush(); err != nil {
		return nil, err
	}
	return bytes.ReplaceAll(output.Bytes(), []byte("<true></true>"), []byte("<true/>")), nil
}

func renderSystemd(config Config) []byte {
	var output strings.Builder
	output.WriteString("[Unit]\nDescription=Paperboat helper runtime\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\nExecStart=")
	output.WriteString(systemdEscape(config.Executable))
	for _, argument := range config.Arguments {
		output.WriteByte(' ')
		output.WriteString(systemdEscape(argument))
	}
	output.WriteByte('\n')
	for _, key := range sortedKeys(config.Environment) {
		output.WriteString("Environment=")
		output.WriteString(systemdEscape(key + "=" + config.Environment[key]))
		output.WriteByte('\n')
	}
	output.WriteString("Restart=on-failure\nRestartSec=5s\nNoNewPrivileges=true\nPrivateTmp=true\n\n[Install]\nWantedBy=default.target\n")
	return []byte(output.String())
}

func systemdEscape(value string) string {
	return `"` + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `"`, `\"`) + `"`
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrInvalidDefinition
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return err
	}
	info, err = os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return ErrInvalidDefinition
	}
	temporary, err := os.CreateTemp(directory, ".service-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(mode); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(directory)
}
func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
func safeExecutable(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 {
		return ErrInvalidDefinition
	}
	return nil
}
func safeValues(values []string) bool {
	for _, value := range values {
		if value == "" || strings.ContainsAny(value, "\x00\r\n") {
			return false
		}
	}
	return true
}
func safeEnvironment(environment map[string]string) bool {
	for key, value := range environment {
		if !safeEnvironmentKey(key) || !safeValues([]string{value}) {
			return false
		}
	}
	return true
}

func safeEnvironmentKey(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if !(char == '_' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}
func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func ensureRoot(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return err
		}
		info, err = os.Lstat(path)
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrInvalidDefinition
	}
	return nil
}
