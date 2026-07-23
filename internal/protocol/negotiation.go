package protocol

import (
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/pinksaucepasta/paperboat-helper/internal/config"
)

var ErrInvalidCapabilities = errors.New("invalid available capabilities")

type CapabilityProvider interface{ Capabilities() []string }

func AvailableCapabilities(providers ...CapabilityProvider) (map[string]bool, error) {
	available := make(map[string]bool)
	for _, provider := range providers {
		if provider == nil {
			return nil, ErrInvalidCapabilities
		}
		for _, capability := range provider.Capabilities() {
			if !capabilityPattern.MatchString(capability) || available[capability] {
				return nil, ErrInvalidCapabilities
			}
			available[capability] = true
		}
	}
	if !available["terminal.v1"] || !available["health.v1"] {
		return nil, ErrInvalidCapabilities
	}
	return available, nil
}

const ProtocolVersion = "1.0"

const ProtocolIncompatible Code = "protocol_incompatible"

var requiredCapabilities = map[string]bool{"terminal.v1": true, "health.v1": true}

var allowedCapabilities = map[config.Profile]map[string]bool{
	config.Hosted: {
		"terminal.v1": true, "terminal.input-stream.v1": true, "health.v1": true, "upload.v1": true,
		"preview.public.v1": true, "activity.v1": true, "config.apply.v1": true,
		"hosted.lifecycle.v1": true, "update.signed.v1": true,
	},
	config.BYOD: {
		"terminal.v1": true, "terminal.input-stream.v1": true, "health.v1": true, "upload.v1": true,
		"preview.public.v1": true, "activity.v1": true, "config.apply.v1": true,
		"update.signed.v1": true,
	},
}

type Negotiator struct {
	Profile          config.Profile
	Available        map[string]bool
	ConfigApplyProof bool
}

type Welcome struct {
	Version      string
	Capabilities []string
}

func (n Negotiator) Negotiate(minVersion, maxVersion string, offered []string) (Welcome, error) {
	minMajor, minMinor, minOK := parseVersion(minVersion)
	maxMajor, maxMinor, maxOK := parseVersion(maxVersion)
	if !minOK || !maxOK || minMajor != 1 || maxMajor != 1 || minMinor > 0 || maxMinor < 0 || minMinor > maxMinor {
		return Welcome{}, &Error{Code: ProtocolIncompatible}
	}
	allowed, ok := allowedCapabilities[n.Profile]
	if !ok {
		return Welcome{}, &Error{Code: ProtocolIncompatible}
	}
	offeredSet := make(map[string]bool, len(offered))
	for _, capability := range offered {
		offeredSet[capability] = true
	}
	for capability := range requiredCapabilities {
		if !offeredSet[capability] || !n.Available[capability] {
			return Welcome{}, &Error{Code: CapabilityRequired}
		}
	}
	selected := make([]string, 0, len(offered))
	for capability := range offeredSet {
		if !allowed[capability] || !n.Available[capability] {
			continue
		}
		if capability == "config.apply.v1" && n.Profile == config.BYOD && !n.ConfigApplyProof {
			continue
		}
		selected = append(selected, capability)
	}
	sort.Strings(selected)
	return Welcome{Version: ProtocolVersion, Capabilities: selected}, nil
}

func parseVersion(version string) (int, int, bool) {
	majorText, minorText, ok := strings.Cut(version, ".")
	if !ok || majorText == "" || minorText == "" || strings.Contains(minorText, ".") {
		return 0, 0, false
	}
	major, errMajor := strconv.Atoi(majorText)
	minor, errMinor := strconv.Atoi(minorText)
	return major, minor, errMajor == nil && errMinor == nil && major >= 0 && minor >= 0
}
