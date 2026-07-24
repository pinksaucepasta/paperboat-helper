package configsync

import (
	"context"
	"errors"
	"os"
	"path"
	"sort"
	"strings"
)

var ErrClassificationInvalid = errors.New("invalid config classification metadata")

type ClassificationCandidate struct {
	Path            string                  `json:"path"`
	FileType        string                  `json:"file_type"`
	Size            int64                   `json:"size"`
	ChangeFrequency string                  `json:"change_frequency"`
	LocationClass   string                  `json:"location_class"`
	Siblings        []ClassificationSibling `json:"siblings,omitempty"`
}

type ClassificationSibling struct {
	Name     string `json:"name"`
	FileType string `json:"file_type"`
}

type ClassificationResult struct {
	Path       string  `json:"path"`
	Decision   string  `json:"decision"`
	Confidence float64 `json:"confidence"`
	ReasonCode string  `json:"reason_code"`
	Source     string  `json:"source"`
	Pending    bool    `json:"pending"`
}

type ClassificationResponse struct {
	Results            []ClassificationResult `json:"results"`
	PolicyRevision     string                 `json:"policy_revision"`
	ModelRevision      string                 `json:"model_revision"`
	ClassifierRevision string                 `json:"classifier_revision"`
	Health             string                 `json:"health"`
}

type ClassificationAuthority interface {
	Classify(context.Context, []ClassificationCandidate) (ClassificationResponse, error)
}

func ClassificationCandidates(snapshot Snapshot, paths []string) ([]ClassificationCandidate, []string, error) {
	candidates := make([]ClassificationCandidate, 0, len(paths))
	deletions := make([]string, 0)
	for _, relative := range paths {
		if !safeRelativeStatusPath(relative) {
			return nil, nil, ErrClassificationInvalid
		}
		state, exists := snapshot.Files[relative]
		if !exists {
			deletions = append(deletions, relative)
			continue
		}
		fileType := "file"
		if state.Mode&os.ModeSymlink != 0 {
			fileType = "symlink"
		}
		candidate := ClassificationCandidate{
			Path: relative, FileType: fileType, Size: state.Bytes,
			ChangeFrequency: "changed", LocationClass: classificationLocation(relative),
			Siblings: classificationSiblings(snapshot.Files, relative),
		}
		candidates = append(candidates, candidate)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Path < candidates[j].Path })
	sort.Strings(deletions)
	return candidates, deletions, nil
}

func ValidateClassificationResponse(response ClassificationResponse, candidates []ClassificationCandidate, policy RuntimePolicy) error {
	if len(response.Results) != len(candidates) || response.PolicyRevision != policy.Revision ||
		response.ClassifierRevision != policy.ClassifierRevision ||
		response.ModelRevision != policy.ClassifierModelRevision ||
		(response.Health != "healthy" && response.Health != "unavailable" && response.Health != "disabled") {
		return ErrClassificationInvalid
	}
	for index, result := range response.Results {
		if result.Path != candidates[index].Path ||
			(result.Decision != "portable" && result.Decision != "project_only" &&
				result.Decision != "exclude" && result.Decision != "uncertain") ||
			result.Confidence < 0 || result.Confidence > 1 || result.ReasonCode == "" ||
			result.Source == "" || result.Pending != (result.Decision == "uncertain") {
			return ErrClassificationInvalid
		}
	}
	return nil
}

func classificationLocation(relative string) string {
	switch {
	case strings.HasPrefix(relative, ".config/"):
		return "xdg_config"
	case strings.HasPrefix(relative, ".local/share/"):
		return "xdg_data"
	case strings.HasPrefix(relative, ".local/state/"):
		return "xdg_state"
	case strings.HasPrefix(relative, ".cache/"):
		return "xdg_cache"
	default:
		return "home"
	}
}

func classificationSiblings(files map[string]FileState, relative string) []ClassificationSibling {
	directory := path.Dir(relative)
	values := make([]ClassificationSibling, 0, 8)
	for candidate, state := range files {
		if candidate == relative || path.Dir(candidate) != directory {
			continue
		}
		name := path.Base(candidate)
		if name == "." || name == ".." || strings.Contains(name, "/") {
			continue
		}
		fileType := "file"
		if state.Mode&os.ModeSymlink != 0 {
			fileType = "symlink"
		}
		values = append(values, ClassificationSibling{Name: name, FileType: fileType})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Name < values[j].Name })
	if len(values) > 8 {
		values = values[:8]
	}
	return values
}
