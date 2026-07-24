package configsync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"filippo.io/age"
	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

var ErrWorkspaceReconcilerInvalid = errors.New("invalid config workspace reconciler")

type WorkspaceReconcilerConfig struct {
	HomeRoot       string
	StateRoot      string
	Descriptor     RuntimeDescriptor
	Classification ClassificationAuthority
	Resolutions    ConflictResolutionAuthority
	ChezmoiBinary  string
	Clock          func() time.Time
}

type pendingBaseline struct {
	CommitID    string
	Value       Baseline
	Resolutions []ConflictResolution
}

type ReconciliationDiagnostics struct {
	ClassifierPending []PathSummary
	Skipped           []PathSummary
	Conflicts         []PathSummary
}

type DiagnosticsSource interface {
	Diagnostics() ReconciliationDiagnostics
}

type EncryptedWorkspaceReconciler struct {
	homeRoot       string
	stateRoot      string
	baselinePath   string
	identityPath   string
	descriptor     RuntimeDescriptor
	classification ClassificationAuthority
	resolutions    ConflictResolutionAuthority
	chezmoiBinary  string
	clock          func() time.Time

	mu          sync.Mutex
	pending     *pendingBaseline
	diagnostics ReconciliationDiagnostics
}

func NewEncryptedWorkspaceReconciler(config WorkspaceReconcilerConfig) (*EncryptedWorkspaceReconciler, error) {
	if !canonicalAbsolutePath(config.HomeRoot) || !canonicalAbsolutePath(config.StateRoot) ||
		config.ChezmoiBinary == "" || config.Classification == nil || validateRuntimeDescriptor(config.Descriptor, Credential{
		EnvironmentID: config.Descriptor.EnvironmentID, HelperID: config.Descriptor.HelperID,
		AssignmentID: config.Descriptor.AssignmentID, WarningRevision: config.Descriptor.WarningRevision,
	}) != nil {
		return nil, ErrWorkspaceReconcilerInvalid
	}
	for _, root := range []string{config.HomeRoot, config.StateRoot} {
		info, err := os.Lstat(root)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.Join(ErrWorkspaceReconcilerInvalid, err)
		}
		resolved, err := filepath.EvalSymlinks(root)
		if err != nil || resolved != root {
			return nil, errors.Join(ErrWorkspaceReconcilerInvalid, err)
		}
	}
	if config.Clock == nil {
		config.Clock = func() time.Time { return time.Now().UTC() }
	}
	reconciler := &EncryptedWorkspaceReconciler{
		homeRoot: config.HomeRoot, stateRoot: config.StateRoot,
		baselinePath: filepath.Join(config.StateRoot, "baseline.age"),
		identityPath: filepath.Join(config.StateRoot, "identity.age"),
		descriptor:   config.Descriptor, classification: config.Classification,
		resolutions:   config.Resolutions,
		chezmoiBinary: config.ChezmoiBinary, clock: config.Clock,
	}
	if err := EnsureAgeIdentity(reconciler.identityPath, config.Descriptor.AgeIdentities); err != nil {
		return nil, err
	}
	if err := recoverApplyJournal(
		filepath.Join(config.StateRoot, "apply-journal.age"), config.HomeRoot,
		config.Descriptor.AgeIdentities, config.Descriptor.RepositoryID,
		config.Descriptor.AssignmentID, config.Descriptor.Policy.MaxBatchBytes,
	); err != nil {
		return nil, err
	}
	return reconciler, nil
}

func (r *EncryptedWorkspaceReconciler) Reconcile(ctx context.Context, repositoryRoot string, remote RemoteSnapshot) (PreparedPublication, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.recoverResolutionCommit(ctx, remote.Revision); err != nil {
		return PreparedPublication{}, err
	}
	r.pending = nil
	r.diagnostics = ReconciliationDiagnostics{}
	if !canonicalAbsolutePath(repositoryRoot) || remote.Revision == "" {
		return PreparedPublication{}, ErrWorkspaceReconcilerInvalid
	}
	repository, err := git.PlainOpen(repositoryRoot)
	if err != nil {
		return PreparedPublication{}, ErrWorkspaceReconcilerInvalid
	}
	remoteHash := plumbing.NewHash(remote.Revision)
	if remoteHash.IsZero() {
		return PreparedPublication{}, ErrRemoteRevisionChanged
	}
	worktree, err := repository.Worktree()
	if err != nil {
		return PreparedPublication{}, err
	}
	if err := worktree.Reset(&git.ResetOptions{Commit: remoteHash, Mode: git.HardReset}); err != nil {
		return PreparedPublication{}, ErrRemoteRevisionChanged
	}
	format, err := ReadEncryptedRepositoryFormat(repositoryRoot)
	if err != nil || format.KeyVersion > int64(r.descriptor.KeyVersion) {
		return PreparedPublication{}, errors.Join(ErrEncryptedRepositoryInvalid, err)
	}

	remoteHome, err := os.MkdirTemp(r.stateRoot, "remote-home-")
	if err != nil {
		return PreparedPublication{}, err
	}
	defer os.RemoveAll(remoteHome)
	remoteHome, err = filepath.EvalSymlinks(remoteHome)
	if err != nil {
		return PreparedPublication{}, err
	}
	remoteRuntime, err := os.MkdirTemp(r.stateRoot, "remote-runtime-")
	if err != nil {
		return PreparedPublication{}, err
	}
	defer os.RemoveAll(remoteRuntime)
	remoteRuntime, err = filepath.EvalSymlinks(remoteRuntime)
	if err != nil {
		return PreparedPublication{}, err
	}
	remoteSource, err := NewChezmoiSource(ChezmoiSourceConfig{
		Binary: r.chezmoiBinary, RuntimeRoot: remoteRuntime, SourceRoot: repositoryRoot,
		HomeRoot: remoteHome, IdentityPath: r.identityPath, Recipient: r.descriptor.AgeRecipient,
	})
	if err != nil {
		return PreparedPublication{}, err
	}
	if err := remoteSource.Apply(ctx); err != nil {
		return PreparedPublication{}, err
	}
	remoteFiles, err := TakeSnapshot(remoteHome, r.descriptor.Policy)
	if err != nil {
		return PreparedPublication{}, err
	}
	localFiles, err := TakeSnapshot(r.homeRoot, r.descriptor.Policy)
	if err != nil {
		return PreparedPublication{}, err
	}
	r.diagnostics.Skipped = boundPathSummaries(
		append(append([]PathSummary(nil), localFiles.Skipped...), remoteFiles.Skipped...),
		r.descriptor.Policy.SummaryLimit,
	)
	baselineFiles := map[string]FileState{}
	if _, statErr := os.Lstat(r.baselinePath); errors.Is(statErr, os.ErrNotExist) {
		// A missing baseline is an explicit first reconciliation. The empty
		// ancestor makes differing local and remote values conflict.
	} else if statErr != nil {
		return PreparedPublication{}, statErr
	} else if baseline, baselineErr := ReadBaseline(r.baselinePath, r.descriptor.AgeIdentities); baselineErr == nil {
		if baseline.RepositoryID != r.descriptor.RepositoryID || baseline.AssignmentID != r.descriptor.AssignmentID ||
			baseline.PolicyRevision != r.descriptor.Policy.Revision || baseline.KeyVersion > int64(r.descriptor.KeyVersion) {
			return PreparedPublication{}, ErrBaselineInvalid
		}
		baselineFiles = baseline.Files
	} else {
		return PreparedPublication{}, baselineErr
	}
	localChanged := ChangedPaths(baselineFiles, localFiles.Files)
	candidates, _, err := ClassificationCandidates(localFiles, localChanged)
	if err != nil {
		return PreparedPublication{}, err
	}
	if len(candidates) > 0 {
		response, classifyErr := r.classification.Classify(ctx, candidates)
		responseErr := ValidateClassificationResponse(response, candidates, r.descriptor.Policy)
		for _, candidate := range candidates {
			decision := "uncertain"
			reason := "provider_unavailable"
			if classifyErr == nil && responseErr == nil {
				for _, result := range response.Results {
					if result.Path == candidate.Path {
						decision = result.Decision
						reason = result.ReasonCode
						break
					}
				}
			}
			if decision == "portable" {
				continue
			}
			if decision == "uncertain" {
				r.diagnostics.ClassifierPending = append(r.diagnostics.ClassifierPending, PathSummary{
					Path: candidate.Path, Bytes: candidate.Size, Reason: reason,
				})
			}
			if previous, exists := baselineFiles[candidate.Path]; exists {
				localFiles.Files[candidate.Path] = previous
			} else {
				delete(localFiles.Files, candidate.Path)
			}
		}
	}
	plan := PlanReconciliation(baselineFiles, localFiles.Files, remoteFiles.Files)
	if len(plan.Conflicts) > 0 {
		plan.Conflicts = identifyConflicts(
			r.descriptor.RepositoryID, r.descriptor.AssignmentID, remote.Revision,
			baselineFiles, localFiles.Files, remoteFiles.Files, plan.Conflicts,
		)
		resolved := make([]ConflictResolution, 0)
		if r.resolutions != nil {
			resolutions, resolutionErr := r.resolutions.Pending(ctx)
			if resolutionErr != nil {
				return PreparedPublication{}, resolutionErr
			}
			for _, resolution := range resolutions {
				if !resolution.Valid() || resolution.ExpectedRemoteRevision != remote.Revision {
					continue
				}
				for _, conflict := range plan.Conflicts {
					if resolution.Path != conflict.Path || resolution.ConflictRevision != conflict.Revision {
						continue
					}
					switch resolution.Action {
					case "keep_local":
						setBaselineState(baselineFiles, resolution.Path, remoteFiles.Files)
					case "keep_remote", "externally_resolved":
						setBaselineState(baselineFiles, resolution.Path, localFiles.Files)
					}
					resolved = append(resolved, resolution)
					break
				}
			}
			if len(resolved) > 0 {
				plan = PlanReconciliation(baselineFiles, localFiles.Files, remoteFiles.Files)
				plan.Conflicts = identifyConflicts(
					r.descriptor.RepositoryID, r.descriptor.AssignmentID, remote.Revision,
					baselineFiles, localFiles.Files, remoteFiles.Files, plan.Conflicts,
				)
			}
		}
		r.diagnostics.Conflicts = boundPathSummaries(plan.Conflicts, r.descriptor.Policy.SummaryLimit)
		if len(plan.Conflicts) > 0 {
			if err := r.preserveConflicts(localFiles.Files, remoteFiles.Files, plan.Conflicts, remoteHome, remote.Revision); err != nil {
				return PreparedPublication{}, err
			}
			return PreparedPublication{}, ErrConfigConflict
		}
		if len(resolved) > 0 {
			defer func() {
				if r.pending != nil {
					r.pending.Resolutions = append(r.pending.Resolutions, resolved...)
				}
			}()
		}
	}

	localRuntime, err := os.MkdirTemp(r.stateRoot, "local-runtime-")
	if err != nil {
		return PreparedPublication{}, err
	}
	defer os.RemoveAll(localRuntime)
	localRuntime, err = filepath.EvalSymlinks(localRuntime)
	if err != nil {
		return PreparedPublication{}, err
	}
	localSource, err := NewChezmoiSource(ChezmoiSourceConfig{
		Binary: r.chezmoiBinary, RuntimeRoot: localRuntime, SourceRoot: repositoryRoot,
		HomeRoot: r.homeRoot, IdentityPath: r.identityPath, Recipient: r.descriptor.AgeRecipient,
	})
	if err != nil {
		return PreparedPublication{}, err
	}
	applyPaths := append(append([]string(nil), plan.ApplyRemote...), plan.DeleteLocal...)
	journalPath := filepath.Join(r.stateRoot, "apply-journal.age")
	if len(applyPaths) > 0 {
		if err := beginApplyJournal(
			journalPath, r.homeRoot, r.descriptor.RepositoryID, r.descriptor.AssignmentID,
			remote.Revision, r.descriptor.AgeRecipient, applyPaths, r.descriptor.Policy.MaxBatchBytes,
		); err != nil {
			return PreparedPublication{}, err
		}
	}
	applySucceeded := false
	defer func() {
		if len(applyPaths) > 0 && !applySucceeded {
			_ = recoverApplyJournal(
				journalPath, r.homeRoot, r.descriptor.AgeIdentities,
				r.descriptor.RepositoryID, r.descriptor.AssignmentID,
				r.descriptor.Policy.MaxBatchBytes,
			)
		}
	}()
	if len(plan.ApplyRemote) > 0 {
		if err := localSource.ApplyPaths(ctx, plan.ApplyRemote); err != nil {
			return PreparedPublication{}, err
		}
	}
	for _, path := range plan.DeleteLocal {
		if err := removeManagedPath(r.homeRoot, path); err != nil {
			return PreparedPublication{}, err
		}
	}
	if len(applyPaths) > 0 {
		if err := os.Remove(journalPath); err != nil {
			return PreparedPublication{}, err
		}
	}
	applySucceeded = true
	if format.KeyVersion < int64(r.descriptor.KeyVersion) {
		plan.PublishUpdates = make([]string, 0, len(localFiles.Files))
		for path := range localFiles.Files {
			plan.PublishUpdates = append(plan.PublishUpdates, path)
		}
		sort.Strings(plan.PublishUpdates)
		format.KeyVersion = int64(r.descriptor.KeyVersion)
		format.Recipient = r.descriptor.AgeRecipient
		if err := WriteEncryptedRepositoryFormat(repositoryRoot, format); err != nil {
			return PreparedPublication{}, err
		}
	}
	if err := localSource.AddEncrypted(ctx, plan.PublishUpdates); err != nil {
		return PreparedPublication{}, err
	}
	for _, path := range plan.PublishDeletes {
		if err := localSource.Forget(ctx, path); err != nil {
			return PreparedPublication{}, err
		}
	}
	if err := worktree.AddWithOptions(&git.AddOptions{All: true}); err != nil {
		return PreparedPublication{}, err
	}
	status, err := worktree.Status()
	if err != nil {
		return PreparedPublication{}, err
	}
	if status.IsClean() {
		merged, snapshotErr := TakeSnapshot(r.homeRoot, r.descriptor.Policy)
		if snapshotErr != nil {
			return PreparedPublication{}, snapshotErr
		}
		r.pending = &pendingBaseline{CommitID: remote.Revision, Value: Baseline{
			Format: "paperboat-config-baseline-v1", RepositoryID: r.descriptor.RepositoryID,
			AssignmentID: r.descriptor.AssignmentID, PolicyRevision: r.descriptor.Policy.Revision,
			KeyVersion: int64(r.descriptor.KeyVersion), RemoteRevision: remote.Revision, Files: merged.Files,
		}}
		return PreparedPublication{ExpectedRemoteRevision: remote.Revision, CommitID: remote.Revision, HasChanges: false}, nil
	}
	if err := validateRepositoryBatch(repositoryRoot, status, r.descriptor.Policy); err != nil {
		return PreparedPublication{}, err
	}
	commit, err := worktree.Commit("Synchronize encrypted configuration", &git.CommitOptions{
		Author: &object.Signature{Name: "Paperboat", Email: "config@paperboat.invalid", When: r.clock()},
	})
	if err != nil {
		return PreparedPublication{}, err
	}
	merged, err := TakeSnapshot(r.homeRoot, r.descriptor.Policy)
	if err != nil {
		return PreparedPublication{}, err
	}
	r.pending = &pendingBaseline{CommitID: commit.String(), Value: Baseline{
		Format: "paperboat-config-baseline-v1", RepositoryID: r.descriptor.RepositoryID,
		AssignmentID: r.descriptor.AssignmentID, PolicyRevision: r.descriptor.Policy.Revision,
		KeyVersion: int64(r.descriptor.KeyVersion), RemoteRevision: commit.String(), Files: merged.Files,
	}}
	return PreparedPublication{
		ExpectedRemoteRevision: remote.Revision, CommitID: commit.String(), HasChanges: true,
	}, nil
}

func (r *EncryptedWorkspaceReconciler) Diagnostics() ReconciliationDiagnostics {
	r.mu.Lock()
	defer r.mu.Unlock()
	return ReconciliationDiagnostics{
		ClassifierPending: append([]PathSummary(nil), r.diagnostics.ClassifierPending...),
		Skipped:           append([]PathSummary(nil), r.diagnostics.Skipped...),
		Conflicts:         append([]PathSummary(nil), r.diagnostics.Conflicts...),
	}
}

func (r *EncryptedWorkspaceReconciler) PublicationCommitted(ctx context.Context, prepared PreparedPublication, revision string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending == nil || prepared.CommitID == "" ||
		r.pending.CommitID != prepared.CommitID || revision != prepared.CommitID {
		return ErrBaselineInvalid
	}
	if len(r.pending.Resolutions) > 0 {
		if err := writeResolutionCommit(
			resolutionCommitPath(r.stateRoot), r.descriptor.AgeRecipient,
			resolutionCommit{
				Format: "paperboat-config-resolution-commit-v1", CommitID: revision,
				Baseline: r.pending.Value, Resolutions: r.pending.Resolutions,
			},
		); err != nil {
			return err
		}
	}
	err := WriteBaseline(r.baselinePath, r.pending.Value, r.descriptor.AgeRecipient)
	if err == nil {
		for _, resolution := range r.pending.Resolutions {
			if r.resolutions != nil {
				if acknowledgeErr := r.resolutions.Acknowledge(ctx, resolution.ID, revision); acknowledgeErr != nil {
					return acknowledgeErr
				}
			}
			_ = os.RemoveAll(filepath.Join(r.stateRoot, "conflicts", resolution.ConflictRevision))
		}
		if len(r.pending.Resolutions) > 0 {
			if removeErr := os.Remove(resolutionCommitPath(r.stateRoot)); removeErr != nil {
				return removeErr
			}
		}
		r.pending = nil
	}
	return err
}

func (r *EncryptedWorkspaceReconciler) PublicationPrepared(_ context.Context, prepared PreparedPublication) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending == nil || prepared.CommitID == "" || r.pending.CommitID != prepared.CommitID {
		return ErrBaselineInvalid
	}
	if len(r.pending.Resolutions) == 0 {
		return nil
	}
	return writeResolutionCommit(
		resolutionCommitPath(r.stateRoot), r.descriptor.AgeRecipient,
		resolutionCommit{
			Format: "paperboat-config-resolution-commit-v1", CommitID: prepared.CommitID,
			Baseline: r.pending.Value, Resolutions: r.pending.Resolutions,
		},
	)
}

func (r *EncryptedWorkspaceReconciler) PublicationAborted(_ context.Context, prepared PreparedPublication) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	path := resolutionCommitPath(r.stateRoot)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	commit, err := readResolutionCommit(path, r.descriptor.AgeIdentities)
	if err != nil || commit.CommitID != prepared.CommitID {
		return errors.Join(ErrBaselineInvalid, err)
	}
	return os.Remove(path)
}

func (r *EncryptedWorkspaceReconciler) recoverResolutionCommit(ctx context.Context, remoteRevision string) error {
	path := resolutionCommitPath(r.stateRoot)
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	commit, err := readResolutionCommit(path, r.descriptor.AgeIdentities)
	if err != nil || commit.Baseline.RepositoryID != r.descriptor.RepositoryID ||
		commit.Baseline.AssignmentID != r.descriptor.AssignmentID ||
		commit.Baseline.RemoteRevision != commit.CommitID {
		return errors.Join(ErrBaselineInvalid, err)
	}
	if remoteRevision != commit.CommitID {
		return ErrSyncUncertain
	}
	if err := WriteBaseline(r.baselinePath, commit.Baseline, r.descriptor.AgeRecipient); err != nil {
		return err
	}
	for _, resolution := range commit.Resolutions {
		if r.resolutions == nil {
			return ErrBaselineInvalid
		}
		if err := r.resolutions.Acknowledge(ctx, resolution.ID, commit.CommitID); err != nil {
			return err
		}
		_ = os.RemoveAll(filepath.Join(r.stateRoot, "conflicts", resolution.ConflictRevision))
	}
	return os.Remove(path)
}

func (r *EncryptedWorkspaceReconciler) preserveConflicts(local, remote map[string]FileState, conflicts []PathSummary, remoteHome, remoteRevision string) error {
	recipient, err := age.ParseX25519Recipient(r.descriptor.AgeRecipient)
	if err != nil {
		return ErrEncryptedRepositoryInvalid
	}
	for _, conflict := range conflicts {
		conflictRoot := filepath.Join(r.stateRoot, "conflicts", conflict.Revision)
		if err := os.MkdirAll(conflictRoot, 0o700); err != nil {
			return err
		}
		for _, side := range []struct {
			name  string
			root  string
			state FileState
			ok    bool
		}{
			{"local", r.homeRoot, local[conflict.Path], hasState(local, conflict.Path)},
			{"remote", remoteHome, remote[conflict.Path], hasState(remote, conflict.Path)},
		} {
			target := filepath.Join(conflictRoot, filepath.FromSlash(conflict.Path)+"."+side.name+".age")
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return err
			}
			if info, statErr := os.Lstat(target); statErr == nil {
				if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
					return ErrEncryptedRepositoryInvalid
				}
				continue
			} else if !errors.Is(statErr, os.ErrNotExist) {
				return statErr
			}
			if !side.ok {
				if err := encryptConflictBytes(target, recipient, []byte(`{"deleted":true}`+"\n")); err != nil {
					return err
				}
				continue
			}
			if side.state.Mode&os.ModeSymlink != 0 {
				if err := encryptConflictBytes(target, recipient, []byte(side.state.Target)); err != nil {
					return err
				}
				continue
			}
			source := filepath.Join(side.root, filepath.FromSlash(conflict.Path))
			if err := encryptConflictFile(target, recipient, source, side.state); err != nil {
				return err
			}
		}
		metadata, err := json.Marshal(struct {
			RepositoryID   string      `json:"repository_id"`
			AssignmentID   string      `json:"assignment_id"`
			RemoteRevision string      `json:"remote_revision"`
			Conflict       PathSummary `json:"conflict"`
		}{r.descriptor.RepositoryID, r.descriptor.AssignmentID, remoteRevision, conflict})
		if err != nil {
			return err
		}
		if err := writePrivateAtomic(filepath.Join(conflictRoot, "metadata.json"), append(metadata, '\n')); err != nil {
			return err
		}
	}
	return nil
}

func hasState(states map[string]FileState, path string) bool {
	_, ok := states[path]
	return ok
}

func encryptConflictFile(target string, recipient age.Recipient, source string, expected FileState) error {
	info, err := os.Lstat(source)
	if err != nil || !info.Mode().IsRegular() || info.Size() != expected.Bytes {
		return errors.Join(ErrSourceChanged, err)
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	return encryptConflictReader(target, recipient, input)
}

func encryptConflictBytes(target string, recipient age.Recipient, value []byte) error {
	return encryptConflictReader(target, recipient, strings.NewReader(string(value)))
}

func encryptConflictReader(target string, recipient age.Recipient, input io.Reader) error {
	file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	writer, err := age.Encrypt(file, recipient)
	if err == nil {
		_, err = io.Copy(writer, input)
	}
	if err == nil {
		err = writer.Close()
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if err != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(target)
		return errors.Join(err, syncErr, closeErr)
	}
	return nil
}

func removeManagedPath(root, relative string) error {
	if !safeRelativeStatusPath(relative) {
		return ErrWorkspaceReconcilerInvalid
	}
	target := filepath.Join(root, filepath.FromSlash(relative))
	if !sameOrInsidePath(target, root) {
		return ErrWorkspaceReconcilerInvalid
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(target))
	if err != nil || !sameOrInsidePath(parent, root) {
		return errors.Join(ErrWorkspaceReconcilerInvalid, err)
	}
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || info.IsDir() {
		return errors.Join(ErrWorkspaceReconcilerInvalid, err)
	}
	return os.Remove(target)
}

func validateRepositoryBatch(root string, status git.Status, policy RuntimePolicy) error {
	var total int64
	for relative, fileStatus := range status {
		if relative == ".git" || strings.HasPrefix(relative, ".git/") ||
			fileStatus.Worktree == git.Deleted || fileStatus.Staging == git.Deleted {
			continue
		}
		path := filepath.Join(root, filepath.FromSlash(relative))
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return errors.Join(ErrEncryptedRepositoryInvalid, err)
		}
		if info.Size() > policy.MaxFileBytes+(64<<10) {
			return ErrEncryptedRepositoryInvalid
		}
		total += info.Size()
		if total > policy.MaxBatchBytes+(1<<20) {
			return ErrEncryptedRepositoryInvalid
		}
	}
	return nil
}

func boundPathSummaries(values []PathSummary, limit int) []PathSummary {
	sort.Slice(values, func(i, j int) bool {
		if values[i].Path == values[j].Path {
			return values[i].Reason < values[j].Reason
		}
		return values[i].Path < values[j].Path
	})
	result := values[:0]
	for _, value := range values {
		if len(result) > 0 && result[len(result)-1].Path == value.Path && result[len(result)-1].Reason == value.Reason {
			continue
		}
		result = append(result, value)
		if len(result) == limit {
			break
		}
	}
	return result
}
