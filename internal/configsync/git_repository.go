package configsync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
)

var ErrGitRepositoryInvalid = errors.New("invalid config Git repository")

type RepositoryAccessSource interface {
	RepositoryAccess(context.Context) (RepositoryAccess, error)
}

type WorkspaceReconciler interface {
	Reconcile(context.Context, string, RemoteSnapshot) (PreparedPublication, error)
}

type GitRepositoryConfig struct {
	Root       string
	Access     RepositoryAccessSource
	Reconciler WorkspaceReconciler
}

type GitRepository struct {
	root       string
	access     RepositoryAccessSource
	reconciler WorkspaceReconciler
	mu         sync.Mutex
}

func NewGitRepository(config GitRepositoryConfig) (*GitRepository, error) {
	if !filepath.IsAbs(config.Root) || filepath.Clean(config.Root) != config.Root ||
		config.Access == nil || config.Reconciler == nil {
		return nil, ErrGitRepositoryInvalid
	}
	parent := filepath.Dir(config.Root)
	info, err := os.Lstat(parent)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(ErrGitRepositoryInvalid, err)
	}
	return &GitRepository{root: config.Root, access: config.Access, reconciler: config.Reconciler}, nil
}

func (r *GitRepository) Fetch(ctx context.Context) (RemoteSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	access, repository, err := r.open(ctx)
	if err != nil {
		return RemoteSnapshot{}, err
	}
	auth := &http.BasicAuth{Username: access.Username, Password: access.Password}
	err = repository.FetchContext(ctx, &git.FetchOptions{
		RemoteName: "origin", Auth: auth, Prune: true,
		RefSpecs: []config.RefSpec{config.RefSpec("+refs/heads/" + access.Branch + ":refs/remotes/origin/" + access.Branch)},
		Tags:     git.NoTags, Force: true,
	})
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return RemoteSnapshot{}, sanitizeGitError(err)
	}
	reference, err := repository.Reference(plumbing.NewRemoteReferenceName("origin", access.Branch), true)
	if err != nil || reference.Hash().IsZero() {
		return RemoteSnapshot{}, errors.Join(ErrGitRepositoryInvalid, sanitizeGitError(err))
	}
	return RemoteSnapshot{Revision: reference.Hash().String()}, nil
}

func (r *GitRepository) Reconcile(ctx context.Context, remote RemoteSnapshot) (PreparedPublication, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if remote.Revision == "" {
		return PreparedPublication{}, ErrRemoteRevisionChanged
	}
	return r.reconciler.Reconcile(ctx, r.root, remote)
}

func (r *GitRepository) Publish(ctx context.Context, prepared PreparedPublication, fencingToken int64) (PublishResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if prepared.CommitID == "" || prepared.ExpectedRemoteRevision == "" || fencingToken < 1 {
		return PublishResult{}, ErrGitRepositoryInvalid
	}
	access, repository, err := r.open(ctx)
	if err != nil {
		return PublishResult{}, err
	}
	if access.Capability != "repository_contents_write" {
		return PublishResult{}, ErrWritesDisabled
	}
	localReference, err := repository.Reference(plumbing.NewBranchReferenceName(access.Branch), true)
	if err != nil || localReference.Hash().String() != prepared.CommitID {
		return PublishResult{}, ErrGitRepositoryInvalid
	}
	auth := &http.BasicAuth{Username: access.Username, Password: access.Password}
	err = repository.PushContext(ctx, &git.PushOptions{
		RemoteName: "origin", Auth: auth, Atomic: true,
		RefSpecs: []config.RefSpec{config.RefSpec("refs/heads/" + access.Branch + ":refs/heads/" + access.Branch)},
	})
	if err != nil {
		if errors.Is(err, git.NoErrAlreadyUpToDate) {
			return PublishResult{RemoteRevision: prepared.CommitID, Landed: true}, nil
		}
		return PublishResult{Uncertain: providerOutcomeUncertain(err)}, sanitizeGitError(err)
	}
	return PublishResult{RemoteRevision: prepared.CommitID, Landed: true}, nil
}

func (r *GitRepository) ObserveCommit(ctx context.Context, commitID string) (bool, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !plumbing.IsHash(commitID) {
		return false, "", ErrGitRepositoryInvalid
	}
	access, repository, err := r.open(ctx)
	if err != nil {
		return false, "", err
	}
	auth := &http.BasicAuth{Username: access.Username, Password: access.Password}
	err = repository.FetchContext(ctx, &git.FetchOptions{
		RemoteName: "origin", Auth: auth,
		RefSpecs: []config.RefSpec{config.RefSpec("+refs/heads/" + access.Branch + ":refs/remotes/origin/" + access.Branch)},
		Tags:     git.NoTags, Force: true,
	})
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return false, "", sanitizeGitError(err)
	}
	reference, err := repository.Reference(plumbing.NewRemoteReferenceName("origin", access.Branch), true)
	if err != nil {
		return false, "", sanitizeGitError(err)
	}
	head := reference.Hash()
	landed, err := commitReachable(repository, plumbing.NewHash(commitID), head, 10_000)
	return landed, head.String(), err
}

func (r *GitRepository) PublicationCommitted(ctx context.Context, prepared PreparedPublication, revision string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if observer, ok := r.reconciler.(PublicationObserver); ok {
		return observer.PublicationCommitted(ctx, prepared, revision)
	}
	return nil
}

func (r *GitRepository) PublicationPrepared(ctx context.Context, prepared PreparedPublication) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if journal, ok := r.reconciler.(PublicationJournal); ok {
		return journal.PublicationPrepared(ctx, prepared)
	}
	return nil
}

func (r *GitRepository) PublicationAborted(ctx context.Context, prepared PreparedPublication) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if journal, ok := r.reconciler.(PublicationJournal); ok {
		return journal.PublicationAborted(ctx, prepared)
	}
	return nil
}

func (r *GitRepository) open(ctx context.Context) (RepositoryAccess, *git.Repository, error) {
	access, err := r.access.RepositoryAccess(ctx)
	if err != nil {
		return RepositoryAccess{}, nil, err
	}
	if access.Password == "" || access.Username != "x-access-token" || access.Branch == "" ||
		(access.Capability != "repository_contents_read" && access.Capability != "repository_contents_write") {
		return RepositoryAccess{}, nil, ErrGitRepositoryInvalid
	}
	info, statErr := os.Lstat(r.root)
	switch {
	case errors.Is(statErr, os.ErrNotExist):
		repository, cloneErr := git.PlainCloneContext(ctx, r.root, false, &git.CloneOptions{
			URL: access.CloneURL, Auth: &http.BasicAuth{Username: access.Username, Password: access.Password},
			RemoteName: "origin", ReferenceName: plumbing.NewBranchReferenceName(access.Branch),
			SingleBranch: true, NoCheckout: false, Tags: git.NoTags,
		})
		if cloneErr != nil {
			return RepositoryAccess{}, nil, sanitizeGitError(cloneErr)
		}
		return access, repository, nil
	case statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0:
		return RepositoryAccess{}, nil, errors.Join(ErrGitRepositoryInvalid, statErr)
	}
	repository, err := git.PlainOpen(r.root)
	if err != nil {
		return RepositoryAccess{}, nil, ErrGitRepositoryInvalid
	}
	remote, err := repository.Remote("origin")
	if err != nil || len(remote.Config().URLs) != 1 || remote.Config().URLs[0] != access.CloneURL {
		return RepositoryAccess{}, nil, ErrGitRepositoryInvalid
	}
	return access, repository, nil
}

func commitReachable(repository *git.Repository, want, head plumbing.Hash, limit int) (bool, error) {
	if want == head {
		return true, nil
	}
	seen := make(map[plumbing.Hash]bool)
	queue := []plumbing.Hash{head}
	for len(queue) > 0 && len(seen) < limit {
		hash := queue[0]
		queue = queue[1:]
		if seen[hash] {
			continue
		}
		seen[hash] = true
		commit, err := repository.CommitObject(hash)
		if err != nil {
			return false, sanitizeGitError(err)
		}
		parentErr := commit.Parents().ForEach(func(parent *object.Commit) error {
			if parent.Hash == want {
				queue = nil
				seen[want] = true
				return io.EOF
			}
			queue = append(queue, parent.Hash)
			return nil
		})
		if errors.Is(parentErr, io.EOF) && seen[want] {
			return true, nil
		}
		if parentErr != nil {
			return false, sanitizeGitError(parentErr)
		}
	}
	return seen[want], nil
}

func providerOutcomeUncertain(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "timeout") || strings.Contains(message, "connection") ||
		strings.Contains(message, "eof") || strings.Contains(message, "reset")
}

func sanitizeGitError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: repository operation failed", ErrGitRepositoryInvalid)
}
