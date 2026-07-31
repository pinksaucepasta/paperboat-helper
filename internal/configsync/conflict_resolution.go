package configsync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

var conflictRevisionPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type ConflictResolution struct {
	ID                     string `json:"id"`
	Path                   string `json:"path"`
	ConflictRevision       string `json:"conflict_revision"`
	ExpectedRemoteRevision string `json:"expected_remote_revision"`
	Scope                  string `json:"scope"`
	Action                 string `json:"action"`
}

func (r ConflictResolution) Valid() bool {
	if r.ID == "" || !safeConflictRevision(r.ConflictRevision) || r.ExpectedRemoteRevision == "" {
		return false
	}
	if r.Scope == "config" {
		return r.Path == "." && (r.Action == "force_pull" || r.Action == "force_push")
	}
	return r.Scope == "path" && safeRelativeStatusPath(r.Path) &&
		(r.Action == "keep_local" || r.Action == "keep_remote" || r.Action == "force_pull" || r.Action == "force_push")
}

type ConflictResolutionAuthority interface {
	Pending(context.Context) ([]ConflictResolution, error)
	Acknowledge(context.Context, string, string) error
}

type resolutionCommit struct {
	Format      string               `json:"format"`
	CommitID    string               `json:"commit_id"`
	Baseline    Baseline             `json:"baseline"`
	Resolutions []ConflictResolution `json:"resolutions"`
}

func writeResolutionCommit(path string, value resolutionCommit) error {
	if !canonicalAbsolutePath(path) || value.Format != "paperboat-config-resolution-commit-v1" ||
		value.CommitID == "" || !validBaseline(value.Baseline) || len(value.Resolutions) == 0 {
		return ErrBaselineInvalid
	}
	for _, resolution := range value.Resolutions {
		if !resolution.Valid() {
			return ErrBaselineInvalid
		}
	}
	plaintext, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return writePrivateAtomic(path, append(plaintext, '\n'))
}

func readResolutionCommit(path string) (resolutionCommit, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm()&0o077 != 0 || info.Size() > 64<<20 {
		return resolutionCommit{}, errors.Join(ErrBaselineInvalid, err)
	}
	file, err := os.Open(path)
	if err != nil {
		return resolutionCommit{}, err
	}
	defer file.Close()
	var value resolutionCommit
	decoder := json.NewDecoder(io.LimitReader(file, 64<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&value) != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) ||
		value.Format != "paperboat-config-resolution-commit-v1" || value.CommitID == "" ||
		!validBaseline(value.Baseline) || len(value.Resolutions) == 0 {
		return resolutionCommit{}, ErrBaselineInvalid
	}
	for _, resolution := range value.Resolutions {
		if !resolution.Valid() {
			return resolutionCommit{}, ErrBaselineInvalid
		}
	}
	return value, nil
}

func resolutionCommitPath(stateRoot string) string {
	return filepath.Join(stateRoot, "resolution-commit.json")
}

func safeConflictRevision(value string) bool {
	return conflictRevisionPattern.MatchString(value)
}

func identifyConflicts(repositoryID, assignmentID, _ string, baseline, local, remote map[string]FileState, conflicts []PathSummary) []PathSummary {
	result := append([]PathSummary(nil), conflicts...)
	for index := range result {
		path := result[index].Path
		hash := sha256.New()
		for _, value := range []string{
			repositoryID, assignmentID, path,
			stateIdentity(baseline, path), stateIdentity(local, path), stateIdentity(remote, path),
		} {
			_, _ = hash.Write([]byte(value))
			_, _ = hash.Write([]byte{0})
		}
		result[index].Revision = hex.EncodeToString(hash.Sum(nil))
	}
	return result
}

func stateIdentity(states map[string]FileState, path string) string {
	state, ok := states[path]
	if !ok {
		return "deleted"
	}
	return state.Hash
}

func setBaselineState(baseline map[string]FileState, path string, source map[string]FileState) {
	if state, ok := source[path]; ok {
		baseline[path] = state
	} else {
		delete(baseline, path)
	}
}
