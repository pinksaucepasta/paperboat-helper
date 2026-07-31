package configsync

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var (
	ErrSnapshotInvalid = errors.New("invalid config snapshot")
	ErrSourceChanged   = errors.New("config source changed while reading")
)

type FileState struct {
	Hash   string      `json:"hash"`
	Bytes  int64       `json:"bytes"`
	Mode   fs.FileMode `json:"mode"`
	Target string      `json:"target,omitempty"`
}

type Snapshot struct {
	Files   map[string]FileState
	Skipped []PathSummary
}

func TakeManifestSnapshot(root string, policy RuntimePolicy, manifest Manifest) (Snapshot, error) {
	if err := validateManifestRoots(root, manifest); err != nil {
		return Snapshot{Files: make(map[string]FileState)}, err
	}
	return takeSelectedSnapshot(root, policy, func(path string, isDir bool) bool {
		return !mandatoryExcluded(path, policy) && manifest.Manages(path, isDir)
	}, func(path string) bool {
		return !mandatoryExcluded(path, policy) && manifest.MayManageDescendant(path)
	}, len(manifest.Roots) == 0)
}

func takeSelectedSnapshot(
	root string,
	policy RuntimePolicy,
	manages func(string, bool) bool,
	mayManageDescendant func(string) bool,
	empty bool,
) (Snapshot, error) {
	result := Snapshot{Files: make(map[string]FileState)}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return result, ErrSnapshotInvalid
	}
	rootInfo, err := os.Lstat(root)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return result, errors.Join(ErrSnapshotInvalid, err)
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil || resolvedRoot != root {
		return result, errors.Join(ErrSnapshotInvalid, err)
	}
	if empty {
		return result, nil
	}
	var batchBytes int64
	err = filepath.WalkDir(root, func(full string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, relErr := filepath.Rel(root, full)
		if relErr != nil {
			return relErr
		}
		if rel == "." {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if !manages(rel, entry.IsDir()) {
			if entry.IsDir() && !mayManageDescendant(rel) {
				return filepath.SkipDir
			}
			return nil
		}
		info, statErr := os.Lstat(full)
		if statErr != nil {
			return statErr
		}
		if info.IsDir() {
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			target, targetErr := portableSymlinkTarget(root, full)
			if targetErr != nil {
				result.Skipped = append(result.Skipped, PathSummary{Path: rel, Reason: "unsafe_symlink"})
				return nil
			}
			state := FileState{Hash: hashSnapshotBytes([]byte("symlink:" + target)), Bytes: int64(len(target)), Mode: os.ModeSymlink, Target: target}
			if batchBytes+state.Bytes > policy.MaxBatchBytes {
				result.Skipped = append(result.Skipped, PathSummary{Path: rel, Bytes: state.Bytes, Reason: "max_batch_bytes"})
				return nil
			}
			batchBytes += state.Bytes
			result.Files[rel] = state
			return nil
		}
		if !info.Mode().IsRegular() {
			result.Skipped = append(result.Skipped, PathSummary{Path: rel, Reason: "special_file"})
			return nil
		}
		if info.Mode().Perm()&0o002 != 0 {
			result.Skipped = append(result.Skipped, PathSummary{Path: rel, Bytes: info.Size(), Reason: "unsafe_permissions"})
			return nil
		}
		if info.Size() > policy.MaxFileBytes {
			result.Skipped = append(result.Skipped, PathSummary{Path: rel, Bytes: info.Size(), Reason: "max_file_bytes"})
			return nil
		}
		if batchBytes+info.Size() > policy.MaxBatchBytes {
			result.Skipped = append(result.Skipped, PathSummary{Path: rel, Bytes: info.Size(), Reason: "max_batch_bytes"})
			return nil
		}
		state, stable, readErr := readStableFile(full, info)
		if readErr != nil {
			return readErr
		}
		if !stable {
			result.Skipped = append(result.Skipped, PathSummary{Path: rel, Bytes: info.Size(), Reason: "file_changing"})
			return nil
		}
		batchBytes += state.Bytes
		result.Files[rel] = state
		return nil
	})
	sort.Slice(result.Skipped, func(i, j int) bool { return result.Skipped[i].Path < result.Skipped[j].Path })
	if err != nil {
		return result, fmt.Errorf("%w: %v", ErrSnapshotInvalid, err)
	}
	return result, nil
}

func validateManifestRoots(root string, manifest Manifest) error {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return ErrSnapshotInvalid
	}
	for _, selected := range manifest.Roots {
		parts := strings.Split(selected.Path, "/")
		current := root
		for index, part := range parts {
			current = filepath.Join(current, filepath.FromSlash(part))
			info, err := os.Lstat(current)
			if errors.Is(err, os.ErrNotExist) {
				break
			}
			if err != nil {
				return errors.Join(ErrSnapshotInvalid, err)
			}
			if info.Mode()&os.ModeSymlink != 0 && (index < len(parts)-1 || selected.Directory) {
				return fmt.Errorf("%w: %s", ErrManifestUnsafePath, selected.Path)
			}
			if index < len(parts)-1 && !info.IsDir() {
				break
			}
		}
	}
	return nil
}

func readStableFile(path string, before fs.FileInfo) (FileState, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return FileState{}, false, err
	}
	hash := sha256.New()
	bytesRead, readErr := io.Copy(hash, file)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return FileState{}, false, errors.Join(readErr, closeErr)
	}
	after, err := os.Lstat(path)
	if err != nil {
		return FileState{}, false, err
	}
	stable := after.Mode().IsRegular() && before.Size() == after.Size() &&
		before.ModTime().Equal(after.ModTime()) && before.Mode() == after.Mode()
	return FileState{
		Hash: hex.EncodeToString(hash.Sum(nil)), Bytes: bytesRead, Mode: after.Mode().Perm(),
	}, stable, nil
}

func portableSymlinkTarget(root, path string) (string, error) {
	target, err := os.Readlink(path)
	if err != nil || filepath.IsAbs(target) {
		return "", ErrSnapshotInvalid
	}
	cleanTarget := filepath.Clean(filepath.Join(filepath.Dir(path), target))
	if !sameOrInsidePath(cleanTarget, root) {
		return "", ErrSnapshotInvalid
	}
	resolved, err := filepath.EvalSymlinks(cleanTarget)
	if err != nil || !sameOrInsidePath(resolved, root) {
		return "", ErrSnapshotInvalid
	}
	return filepath.ToSlash(target), nil
}

func sameOrInsidePath(path, root string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func hashSnapshotBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func ChangedPaths(baseline, current map[string]FileState) []string {
	paths := make(map[string]struct{}, len(baseline)+len(current))
	for path := range baseline {
		paths[path] = struct{}{}
	}
	for path := range current {
		paths[path] = struct{}{}
	}
	changed := make([]string, 0, len(paths))
	for path := range paths {
		if baseline[path] != current[path] {
			changed = append(changed, path)
		}
	}
	sort.Strings(changed)
	return changed
}
