package configsync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
)

const (
	ManifestContractVersion        = "paperboat-manifest-v1"
	DefaultManifestMaxBytes        = 256 << 10
	DefaultManifestMaxLines        = 4096
	DefaultManifestMaxPatternBytes = 1024
)

var (
	ErrManifestMissing    = errors.New("config manifest missing")
	ErrManifestInvalid    = errors.New("config manifest invalid")
	ErrManifestUnsafePath = errors.New("config manifest contains unsafe path")
)

type ManifestLimits struct {
	MaxBytes        int
	MaxLines        int
	MaxPatternBytes int
}

type ManifestRoot struct {
	Path      string `json:"path"`
	Directory bool   `json:"directory"`
}

type Manifest struct {
	Revision string
	Roots    []ManifestRoot
	patterns []gitignore.Pattern
}

func (m Manifest) Clone() Manifest {
	return Manifest{
		Revision: m.Revision,
		Roots:    append([]ManifestRoot(nil), m.Roots...),
		patterns: append([]gitignore.Pattern(nil), m.patterns...),
	}
}

func DefaultManifestLimits() ManifestLimits {
	return ManifestLimits{
		MaxBytes: DefaultManifestMaxBytes, MaxLines: DefaultManifestMaxLines,
		MaxPatternBytes: DefaultManifestMaxPatternBytes,
	}
}

func LoadManifest(repositoryRoot string, limits ManifestLimits) (Manifest, error) {
	if !canonicalAbsolutePath(repositoryRoot) {
		return Manifest{}, ErrManifestInvalid
	}
	include, err := readManifestFile(filepath.Join(repositoryRoot, ".pbinclude"), true, limits)
	if err != nil {
		return Manifest{}, err
	}
	ignore, err := readManifestFile(filepath.Join(repositoryRoot, ".pbignore"), false, limits)
	if err != nil {
		return Manifest{}, err
	}
	return ParseManifest(include, ignore, limits)
}

func readManifestFile(name string, required bool, limits ManifestLimits) ([]byte, error) {
	info, err := os.Lstat(name)
	if errors.Is(err, os.ErrNotExist) && !required {
		return nil, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrManifestMissing
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(ErrManifestInvalid, err)
	}
	if !validManifestLimits(limits) || info.Size() > int64(limits.MaxBytes) {
		return nil, ErrManifestInvalid
	}
	value, err := os.ReadFile(name)
	if err != nil {
		return nil, errors.Join(ErrManifestInvalid, err)
	}
	return value, nil
}

func ParseManifest(include, ignore []byte, limits ManifestLimits) (Manifest, error) {
	if !validManifestLimits(limits) || len(include) > limits.MaxBytes || len(ignore) > limits.MaxBytes ||
		!utf8.Valid(include) || !utf8.Valid(ignore) || bytes.IndexByte(include, 0) >= 0 || bytes.IndexByte(ignore, 0) >= 0 {
		return Manifest{}, ErrManifestInvalid
	}
	roots, err := parseInclude(include, limits)
	if err != nil {
		return Manifest{}, err
	}
	patterns, err := parseIgnore(ignore, limits)
	if err != nil {
		return Manifest{}, err
	}
	hash := sha256.New()
	_, _ = hash.Write(include)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(ignore)
	return Manifest{Revision: hex.EncodeToString(hash.Sum(nil)), Roots: roots, patterns: patterns}, nil
}

func (m Manifest) Manages(name string, isDir bool) bool {
	name = path.Clean(strings.ReplaceAll(name, "\\", "/"))
	if name == "." || !safeManifestPath(name) || manifestHardExcluded(name) {
		return false
	}
	selected := false
	for _, root := range m.Roots {
		if name == root.Path || root.Directory && strings.HasPrefix(name, root.Path+"/") {
			selected = true
			break
		}
	}
	if !selected {
		return false
	}
	parts := strings.Split(name, "/")
	ignored := false
	for _, pattern := range m.patterns {
		switch pattern.Match(parts, isDir) {
		case gitignore.Exclude:
			ignored = true
		case gitignore.Include:
			ignored = false
		}
	}
	return !ignored
}

func (m Manifest) MayManageDescendant(name string) bool {
	name = path.Clean(strings.ReplaceAll(name, "\\", "/"))
	if name == "." || !safeManifestPath(name) || manifestHardExcluded(name) {
		return false
	}
	for _, root := range m.Roots {
		if root.Directory && (root.Path == name || strings.HasPrefix(root.Path, name+"/") || strings.HasPrefix(name, root.Path+"/")) {
			return true
		}
	}
	return false
}

func parseInclude(value []byte, limits ManifestLimits) ([]ManifestRoot, error) {
	lines, err := manifestLines(value, limits)
	if err != nil {
		return nil, err
	}
	byPath := make(map[string]ManifestRoot)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		directory := strings.HasSuffix(line, "/")
		line = strings.TrimSuffix(line, "/")
		if strings.ContainsAny(line, "*?[") || !safeManifestPath(line) {
			return nil, fmt.Errorf("%w: %q", ErrManifestUnsafePath, line)
		}
		if manifestHardExcluded(line) {
			return nil, fmt.Errorf("%w: %q", ErrManifestUnsafePath, line)
		}
		if previous, exists := byPath[line]; !exists || directory && !previous.Directory {
			byPath[line] = ManifestRoot{Path: line, Directory: directory}
		}
	}
	roots := make([]ManifestRoot, 0, len(byPath))
	for _, root := range byPath {
		roots = append(roots, root)
	}
	sort.Slice(roots, func(i, j int) bool { return roots[i].Path < roots[j].Path })
	result := roots[:0]
	for _, candidate := range roots {
		redundant := false
		for _, root := range result {
			if root.Directory && strings.HasPrefix(candidate.Path, root.Path+"/") {
				redundant = true
				break
			}
		}
		if !redundant {
			result = append(result, candidate)
		}
	}
	return result, nil
}

func parseIgnore(value []byte, limits ManifestLimits) ([]gitignore.Pattern, error) {
	lines, err := manifestLines(value, limits)
	if err != nil {
		return nil, err
	}
	patterns := make([]gitignore.Pattern, 0, len(lines))
	for _, line := range lines {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		candidate := strings.TrimPrefix(line, "!")
		candidate = strings.TrimPrefix(candidate, "/")
		candidate = strings.TrimSuffix(candidate, "/")
		if candidate == "" || !safeIgnorePattern(candidate) {
			return nil, ErrManifestInvalid
		}
		patterns = append(patterns, gitignore.ParsePattern(line, nil))
	}
	return patterns, nil
}

func manifestLines(value []byte, limits ManifestLimits) ([]string, error) {
	value = bytes.ReplaceAll(value, []byte("\r\n"), []byte("\n"))
	if bytes.IndexByte(value, '\r') >= 0 {
		return nil, ErrManifestInvalid
	}
	lines := strings.Split(string(value), "\n")
	if len(lines) > limits.MaxLines+1 || len(lines) == limits.MaxLines+1 && lines[len(lines)-1] != "" {
		return nil, ErrManifestInvalid
	}
	for _, line := range lines {
		if len(line) > limits.MaxPatternBytes {
			return nil, ErrManifestInvalid
		}
	}
	return lines, nil
}

func validManifestLimits(limits ManifestLimits) bool {
	return limits.MaxBytes > 0 && limits.MaxBytes <= 4<<20 && limits.MaxLines > 0 && limits.MaxLines <= 65536 &&
		limits.MaxPatternBytes > 0 && limits.MaxPatternBytes <= 8192
}

func safeManifestPath(name string) bool {
	if name == "" || name != path.Clean(name) || strings.HasPrefix(name, "/") || strings.HasPrefix(name, "//") ||
		strings.Contains(name, "\\") || strings.ContainsRune(name, 0) || name == ".." || strings.HasPrefix(name, "../") ||
		strings.Contains(name, "/../") || filepath.IsAbs(name) {
		return false
	}
	first := strings.Split(name, "/")[0]
	return len(first) < 2 || first[1] != ':'
}

func safeIgnorePattern(pattern string) bool {
	if strings.ContainsRune(pattern, 0) || strings.Contains(pattern, "\\") || strings.HasPrefix(pattern, "//") {
		return false
	}
	for _, component := range strings.Split(pattern, "/") {
		if component == ".." {
			return false
		}
		if component == "**" {
			continue
		}
		if _, err := path.Match(component, component); err != nil {
			return false
		}
	}
	return true
}

func manifestHardExcluded(name string) bool {
	return mandatoryExcluded(path.Clean(name), RuntimePolicy{})
}
