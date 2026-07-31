package configsync

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
)

var ErrBaselineInvalid = errors.New("invalid config sync baseline")

type AssignmentMode string

const (
	ModePullOnly      AssignmentMode = "pull_only"
	ModePushOnly      AssignmentMode = "push_only"
	ModeBidirectional AssignmentMode = "bidirectional"
)

func (m AssignmentMode) Valid() bool {
	return m == ModePullOnly || m == ModePushOnly || m == ModeBidirectional
}

type ReconcilePlan struct {
	PublishUpdates []string
	PublishDeletes []string
	ApplyRemote    []string
	DeleteLocal    []string
	Conflicts      []PathSummary
}

func PlanReconciliation(baseline, local, remote map[string]FileState) ReconcilePlan {
	return PlanReconciliationMode(baseline, local, remote, ModeBidirectional)
}

func PlanReconciliationMode(baseline, local, remote map[string]FileState, mode AssignmentMode) ReconcilePlan {
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
		statesDiffer := localOK != remoteOK || localState != remoteState
		switch {
		case !statesDiffer:
			continue
		case mode == ModePushOnly && remoteChanged:
			bytes := int64(0)
			if remoteOK {
				bytes = remoteState.Bytes
			}
			plan.Conflicts = append(plan.Conflicts, PathSummary{Path: path, Bytes: bytes, Reason: "remote_update"})
		case localChanged && remoteChanged:
			bytes := int64(0)
			if remoteOK {
				bytes = remoteState.Bytes
			}
			plan.Conflicts = append(plan.Conflicts, PathSummary{Path: path, Bytes: bytes, Reason: "concurrent_update"})
		case localChanged && mode != ModePullOnly:
			if localOK {
				plan.PublishUpdates = append(plan.PublishUpdates, path)
			} else {
				plan.PublishDeletes = append(plan.PublishDeletes, path)
			}
		case remoteChanged && mode != ModePushOnly:
			if remoteOK {
				plan.ApplyRemote = append(plan.ApplyRemote, path)
			} else {
				plan.DeleteLocal = append(plan.DeleteLocal, path)
			}
		}
	}
	return plan
}

func AcceptedBaseline(
	previous, local, remote map[string]FileState,
	mode AssignmentMode,
	writesEnabled bool,
) map[string]FileState {
	if mode == ModeBidirectional && writesEnabled || mode == ModePushOnly && writesEnabled {
		return cloneFileStates(local)
	}
	accepted := make(map[string]FileState, len(previous)+len(local))
	paths := make(map[string]struct{}, len(previous)+len(local)+len(remote))
	for name := range previous {
		paths[name] = struct{}{}
	}
	for name := range local {
		paths[name] = struct{}{}
	}
	for name := range remote {
		paths[name] = struct{}{}
	}
	for name := range paths {
		localState, localOK := local[name]
		remoteState, remoteOK := remote[name]
		if localOK == remoteOK && (!localOK || localState == remoteState) {
			if localOK {
				accepted[name] = localState
			}
			continue
		}
		if state, ok := previous[name]; ok {
			accepted[name] = state
		}
	}
	return accepted
}

func cloneFileStates(value map[string]FileState) map[string]FileState {
	result := make(map[string]FileState, len(value))
	for name, state := range value {
		result[name] = state
	}
	return result
}

func freezeConflictBaseline(accepted, previous map[string]FileState, conflicts []PathSummary) {
	for _, conflict := range conflicts {
		if state, ok := previous[conflict.Path]; ok {
			accepted[conflict.Path] = state
		} else {
			delete(accepted, conflict.Path)
		}
	}
}

type Baseline struct {
	Format           string                `json:"format"`
	RepositoryID     string                `json:"repository_id"`
	AssignmentID     string                `json:"assignment_id"`
	PolicyRevision   string                `json:"policy_revision"`
	ManifestRevision string                `json:"manifest_revision"`
	SelectedRoots    []ManifestRoot        `json:"selected_roots"`
	FrozenPaths      map[string]FrozenPath `json:"frozen_paths"`
	RemoteRevision   string                `json:"remote_revision"`
	Files            map[string]FileState  `json:"files"`
}

type FrozenPath struct {
	BaseRevision     string `json:"base_revision"`
	ConflictRevision string `json:"conflict_revision"`
}

func ReadBaseline(path string) (Baseline, error) {
	if !canonicalAbsolutePath(path) {
		return Baseline{}, ErrBaselineInvalid
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm()&0o077 != 0 || info.Size() > 64<<20 {
		return Baseline{}, errors.Join(ErrBaselineInvalid, err)
	}
	file, err := os.Open(path)
	if err != nil {
		return Baseline{}, err
	}
	defer file.Close()
	var baseline Baseline
	decoder := json.NewDecoder(io.LimitReader(file, 64<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&baseline) != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) ||
		!validBaseline(baseline) {
		return Baseline{}, ErrBaselineInvalid
	}
	return baseline, nil
}

func WriteBaseline(path string, baseline Baseline) error {
	if !canonicalAbsolutePath(path) || !validBaseline(baseline) {
		return ErrBaselineInvalid
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	plaintext, err := json.Marshal(baseline)
	if err != nil {
		return err
	}
	return writePrivateAtomic(path, append(plaintext, '\n'))
}

func validBaseline(baseline Baseline) bool {
	if baseline.Format != "paperboat-config-baseline-v1" || baseline.RepositoryID == "" ||
		baseline.AssignmentID == "" || baseline.PolicyRevision == "" ||
		len(baseline.ManifestRevision) != 64 || baseline.SelectedRoots == nil || baseline.FrozenPaths == nil ||
		baseline.RemoteRevision == "" || baseline.Files == nil {
		return false
	}
	for path, frozen := range baseline.FrozenPaths {
		if !safeRelativeStatusPath(path) || frozen.BaseRevision == "" || !safeConflictRevision(frozen.ConflictRevision) {
			return false
		}
	}
	for index, root := range baseline.SelectedRoots {
		if !safeManifestPath(root.Path) || manifestHardExcluded(root.Path) ||
			(index > 0 && baseline.SelectedRoots[index-1].Path >= root.Path) {
			return false
		}
	}
	for path, state := range baseline.Files {
		if !safeRelativeStatusPath(path) || len(state.Hash) != 64 || state.Bytes < 0 ||
			(state.Mode&os.ModeSymlink != 0 && (state.Target == "" || filepath.IsAbs(state.Target))) {
			return false
		}
	}
	return true
}
