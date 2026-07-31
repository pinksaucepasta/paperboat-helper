package configsync

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var (
	ErrStatusInvalid = errors.New("invalid config sync status")
	safeStatusCode   = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

var canonicalStates = map[string]struct{}{
	"disabled": {}, "consent_required": {}, "restoring": {}, "watching": {},
	"pending": {}, "syncing": {}, "healthy": {}, "warning": {}, "conflict": {},
	"offline": {}, "revoked": {}, "error": {}, "sync_uncertain": {},
}

type PathSummary struct {
	Path     string `json:"path"`
	Bytes    int64  `json:"bytes,omitempty"`
	Reason   string `json:"reason"`
	Revision string `json:"revision,omitempty"`
}

type Status struct {
	State                 string         `json:"state"`
	Mode                  AssignmentMode `json:"mode"`
	RepositoryID          string         `json:"repository_id,omitempty"`
	AssignmentID          string         `json:"assignment_id,omitempty"`
	EnvironmentID         string         `json:"environment_id,omitempty"`
	HelperID              string         `json:"helper_id,omitempty"`
	HelperGeneration      int64          `json:"helper_generation,omitempty"`
	WarningRevision       string         `json:"warning_revision,omitempty"`
	PolicyRevision        string         `json:"policy_revision,omitempty"`
	SyncRevision          int64          `json:"sync_revision"`
	RemoteRevision        string         `json:"remote_revision,omitempty"`
	ManifestHealth        string         `json:"manifest_health,omitempty"`
	ManifestRevision      string         `json:"manifest_revision,omitempty"`
	ManagedPathCount      int            `json:"managed_path_count"`
	PendingCleanPathCount int            `json:"pending_clean_path_count"`
	LastAppliedRevision   string         `json:"last_applied_revision,omitempty"`
	LastPublishedRevision string         `json:"last_published_revision,omitempty"`
	LeaseID               string         `json:"lease_id,omitempty"`
	FencingToken          int64          `json:"fencing_token,omitempty"`

	LastAttemptAt    *time.Time    `json:"last_attempt_at,omitempty"`
	LastSuccessfulAt *time.Time    `json:"last_successful_sync_at,omitempty"`
	UpdatedAt        time.Time     `json:"updated_at"`
	Skipped          []PathSummary `json:"skipped,omitempty"`
	Conflicts        []PathSummary `json:"conflicts,omitempty"`
	ErrorCode        string        `json:"error_code,omitempty"`
	RecoveryActions  []string      `json:"recovery_actions,omitempty"`
}

func (s Status) Validate(summaryLimit int) error {
	if _, ok := canonicalStates[s.State]; !ok || summaryLimit < 1 || summaryLimit > 1000 ||
		s.SyncRevision < 0 || !s.Mode.Valid() || s.ManagedPathCount < 0 || s.PendingCleanPathCount < 0 ||
		s.HelperGeneration < 0 ||
		s.FencingToken < 0 || s.UpdatedAt.IsZero() || len(s.RemoteRevision) > 256 ||
		len(s.LeaseID) > 128 || len(s.ErrorCode) > 64 || len(s.ManifestRevision) > 64 ||
		len(s.LastAppliedRevision) > 256 || len(s.LastPublishedRevision) > 256 ||
		(s.ErrorCode != "" && !safeStatusCode.MatchString(s.ErrorCode)) ||
		len(s.Skipped) > summaryLimit || len(s.Conflicts) > summaryLimit ||
		len(s.RecoveryActions) > 8 {
		return ErrStatusInvalid
	}
	if s.ManifestHealth != "" && s.ManifestHealth != "healthy" && s.ManifestHealth != "empty" &&
		s.ManifestHealth != "missing" && s.ManifestHealth != "invalid" {
		return ErrStatusInvalid
	}
	for _, group := range [][]PathSummary{s.Skipped, s.Conflicts} {
		for _, item := range group {
			if !safeRelativeStatusPath(item.Path) || item.Bytes < 0 || !safeStatusCode.MatchString(item.Reason) ||
				(item.Revision != "" && !safeConflictRevision(item.Revision)) {
				return ErrStatusInvalid
			}
		}
	}
	for _, action := range s.RecoveryActions {
		if !safeStatusCode.MatchString(action) {
			return ErrStatusInvalid
		}
	}
	if (s.State == "conflict" && len(s.Conflicts) == 0) ||
		(s.State == "sync_uncertain" && s.ErrorCode != "sync_uncertain") ||
		(s.FencingToken > 0 && s.LeaseID == "") {
		return ErrStatusInvalid
	}
	return nil
}

func safeRelativeStatusPath(path string) bool {
	path = filepath.ToSlash(strings.TrimSpace(path))
	return path != "" && len(path) <= 512 && path != "." && !strings.HasPrefix(path, "/") &&
		!strings.Contains(path, "\x00") && filepath.ToSlash(filepath.Clean(path)) == path &&
		path != ".." && !strings.HasPrefix(path, "../")
}

func WriteStatus(path string, status Status, summaryLimit int) error {
	if !filepath.IsAbs(path) || status.Validate(summaryLimit) != nil {
		return ErrStatusInvalid
	}
	parent := filepath.Dir(path)
	info, err := os.Lstat(parent)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(ErrStatusInvalid, err)
	}
	data, err := json.Marshal(status)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(parent, ".config-sync-status-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err = temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(append(data, '\n'))
	}
	if err == nil {
		err = temporary.Sync()
	}
	err = errors.Join(err, temporary.Close())
	if err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func ReadStatus(path string, summaryLimit int) (Status, error) {
	if !filepath.IsAbs(path) {
		return Status{}, ErrStatusInvalid
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm()&0o077 != 0 || info.Size() > maxControlResponseBytes {
		return Status{}, errors.Join(ErrStatusInvalid, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Status{}, err
	}
	var status Status
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&status); err != nil {
		return Status{}, fmt.Errorf("%w: %v", ErrStatusInvalid, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Status{}, ErrStatusInvalid
	}
	if err := status.Validate(summaryLimit); err != nil {
		return Status{}, err
	}
	return status, nil
}
