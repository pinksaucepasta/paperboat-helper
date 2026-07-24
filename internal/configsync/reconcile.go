package configsync

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"filippo.io/age"
)

var ErrBaselineInvalid = errors.New("invalid config sync baseline")

type ReconcilePlan struct {
	PublishUpdates []string
	PublishDeletes []string
	ApplyRemote    []string
	DeleteLocal    []string
	Conflicts      []PathSummary
}

func PlanReconciliation(baseline, local, remote map[string]FileState) ReconcilePlan {
	all := make(map[string]struct{}, len(baseline)+len(local)+len(remote))
	for path := range baseline {
		all[path] = struct{}{}
	}
	for path := range local {
		all[path] = struct{}{}
	}
	for path := range remote {
		all[path] = struct{}{}
	}
	paths := make([]string, 0, len(all))
	for path := range all {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	var plan ReconcilePlan
	for _, path := range paths {
		base, baseOK := baseline[path]
		localState, localOK := local[path]
		remoteState, remoteOK := remote[path]
		localChanged := baseOK != localOK || base != localState
		remoteChanged := baseOK != remoteOK || base != remoteState
		switch {
		case localChanged && remoteChanged && (localOK != remoteOK || localState != remoteState):
			bytes := int64(0)
			if remoteOK {
				bytes = remoteState.Bytes
			}
			plan.Conflicts = append(plan.Conflicts, PathSummary{Path: path, Bytes: bytes, Reason: "concurrent_update"})
		case localChanged:
			if localOK {
				plan.PublishUpdates = append(plan.PublishUpdates, path)
			} else {
				plan.PublishDeletes = append(plan.PublishDeletes, path)
			}
		case remoteChanged:
			if remoteOK {
				plan.ApplyRemote = append(plan.ApplyRemote, path)
			} else {
				plan.DeleteLocal = append(plan.DeleteLocal, path)
			}
		}
	}
	return plan
}

type Baseline struct {
	Format         string               `json:"format"`
	RepositoryID   string               `json:"repository_id"`
	AssignmentID   string               `json:"assignment_id"`
	PolicyRevision string               `json:"policy_revision"`
	KeyVersion     int64                `json:"key_version"`
	RemoteRevision string               `json:"remote_revision"`
	Files          map[string]FileState `json:"files"`
}

func ReadBaseline(path, identitiesValue string) (Baseline, error) {
	if !canonicalAbsolutePath(path) {
		return Baseline{}, ErrBaselineInvalid
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm()&0o077 != 0 || info.Size() > 64<<20 {
		return Baseline{}, errors.Join(ErrBaselineInvalid, err)
	}
	identities, err := age.ParseIdentities(strings.NewReader(identitiesValue))
	if err != nil || len(identities) < 1 || len(identities) > 2 {
		return Baseline{}, ErrBaselineInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return Baseline{}, err
	}
	defer file.Close()
	decrypted, err := age.Decrypt(file, identities...)
	if err != nil {
		return Baseline{}, ErrBaselineInvalid
	}
	var baseline Baseline
	decoder := json.NewDecoder(io.LimitReader(decrypted, 64<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&baseline) != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) ||
		!validBaseline(baseline) {
		return Baseline{}, ErrBaselineInvalid
	}
	return baseline, nil
}

func WriteBaseline(path string, baseline Baseline, recipientValue string) error {
	if !canonicalAbsolutePath(path) || !validBaseline(baseline) {
		return ErrBaselineInvalid
	}
	recipient, err := age.ParseX25519Recipient(strings.TrimSpace(recipientValue))
	if err != nil {
		return ErrBaselineInvalid
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	plaintext, err := json.Marshal(baseline)
	if err != nil {
		return err
	}
	var encrypted bytes.Buffer
	writer, err := age.Encrypt(&encrypted, recipient)
	if err != nil {
		return err
	}
	if _, err := writer.Write(append(plaintext, '\n')); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	return writePrivateAtomic(path, encrypted.Bytes())
}

func validBaseline(baseline Baseline) bool {
	if baseline.Format != "paperboat-config-baseline-v1" || baseline.RepositoryID == "" ||
		baseline.AssignmentID == "" || baseline.PolicyRevision == "" || baseline.KeyVersion < 1 ||
		baseline.RemoteRevision == "" || baseline.Files == nil {
		return false
	}
	for path, state := range baseline.Files {
		if !safeRelativeStatusPath(path) || len(state.Hash) != 64 || state.Bytes < 0 ||
			(state.Mode&os.ModeSymlink != 0 && (state.Target == "" || filepath.IsAbs(state.Target))) {
			return false
		}
	}
	return true
}
