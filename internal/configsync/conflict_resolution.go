package configsync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"filippo.io/age"
)

var conflictRevisionPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type ConflictResolution struct {
	ID                     string `json:"id"`
	Path                   string `json:"path"`
	ConflictRevision       string `json:"conflict_revision"`
	ExpectedRemoteRevision string `json:"expected_remote_revision"`
	Action                 string `json:"action"`
}

func (r ConflictResolution) Valid() bool {
	return r.ID != "" && safeRelativeStatusPath(r.Path) && safeConflictRevision(r.ConflictRevision) &&
		r.ExpectedRemoteRevision != "" &&
		(r.Action == "keep_local" || r.Action == "keep_remote" || r.Action == "externally_resolved")
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

func writeResolutionCommit(path, recipientValue string, value resolutionCommit) error {
	if !canonicalAbsolutePath(path) || value.Format != "paperboat-config-resolution-commit-v1" ||
		value.CommitID == "" || !validBaseline(value.Baseline) || len(value.Resolutions) == 0 {
		return ErrBaselineInvalid
	}
	for _, resolution := range value.Resolutions {
		if !resolution.Valid() {
			return ErrBaselineInvalid
		}
	}
	recipient, err := age.ParseX25519Recipient(strings.TrimSpace(recipientValue))
	if err != nil {
		return ErrBaselineInvalid
	}
	plaintext, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var encrypted bytes.Buffer
	writer, err := age.Encrypt(&encrypted, recipient)
	if err == nil {
		_, err = writer.Write(append(plaintext, '\n'))
	}
	if err == nil {
		err = writer.Close()
	}
	if err != nil {
		return err
	}
	return writePrivateAtomic(path, encrypted.Bytes())
}

func readResolutionCommit(path, identitiesValue string) (resolutionCommit, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm()&0o077 != 0 || info.Size() > 64<<20 {
		return resolutionCommit{}, errors.Join(ErrBaselineInvalid, err)
	}
	identities, err := age.ParseIdentities(strings.NewReader(identitiesValue))
	if err != nil || len(identities) < 1 || len(identities) > 2 {
		return resolutionCommit{}, ErrBaselineInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return resolutionCommit{}, err
	}
	defer file.Close()
	decrypted, err := age.Decrypt(file, identities...)
	if err != nil {
		return resolutionCommit{}, ErrBaselineInvalid
	}
	var value resolutionCommit
	decoder := json.NewDecoder(io.LimitReader(decrypted, 64<<20))
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
	return filepath.Join(stateRoot, "resolution-commit.age")
}

func safeConflictRevision(value string) bool {
	return conflictRevisionPattern.MatchString(value)
}

func identifyConflicts(repositoryID, assignmentID, remoteRevision string, baseline, local, remote map[string]FileState, conflicts []PathSummary) []PathSummary {
	result := append([]PathSummary(nil), conflicts...)
	for index := range result {
		path := result[index].Path
		hash := sha256.New()
		for _, value := range []string{
			repositoryID, assignmentID, remoteRevision, path,
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
