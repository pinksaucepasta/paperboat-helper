BINARY := pbh
PACKAGE := ./cmd/pbh
GO_VERSION := 1.25.7
GO := GOTOOLCHAIN=local go
GOFMT := $(shell GOTOOLCHAIN=local go env GOROOT 2>/dev/null)/bin/gofmt
GO_FILES := $(shell find . -path ./.git -prune -o -name '*.go' -print)
VERSION ?= $(shell ./tools/release-version.sh current)
COMMIT ?= $(shell git rev-parse --verify HEAD 2>/dev/null || echo unknown)
LDFLAGS := -X github.com/pinksaucepasta/paperboat-helper/internal/buildinfo.Version=$(VERSION) -X github.com/pinksaucepasta/paperboat-helper/internal/buildinfo.Commit=$(COMMIT)

.PHONY: artifact-manifest build check clean complete contracts crash cross-build fmt fmt-check generate integration platform race security soak test tidy verify-toolchain vet

contracts:
	@./testdata/contracts/validate.sh

verify-toolchain:
	@test "$$(GOTOOLCHAIN=local go env GOVERSION)" = "go$(GO_VERSION)" || { echo "required Go $(GO_VERSION), found $$(GOTOOLCHAIN=local go env GOVERSION)" >&2; exit 1; }

build: verify-toolchain
	@mkdir -p bin
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY) $(PACKAGE)

artifact-manifest: verify-toolchain
	@echo "Use tools/artifact with an explicit development signing key, artifact path, HTTPS URL, and output paths."

cross-build: verify-toolchain
	@mkdir -p dist
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-darwin-amd64 $(PACKAGE)
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-darwin-arm64 $(PACKAGE)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-linux-amd64 $(PACKAGE)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-linux-arm64 $(PACKAGE)

fmt:
	$(GOFMT) -w $(GO_FILES)

fmt-check:
	@test -z "$$($(GOFMT) -l $(GO_FILES))" || { $(GOFMT) -l $(GO_FILES); echo "Go files are not formatted" >&2; exit 1; }

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

integration: verify-toolchain contracts
	$(GO) test -race ./internal/connector/testharness ./internal/process ./internal/preview ./internal/runtime ./internal/server ./internal/upload

platform: verify-toolchain
	$(GO) test -race ./internal/process ./internal/pty ./internal/service ./internal/session ./internal/store ./internal/update

crash: verify-toolchain
	$(GO) test -race ./internal/store -run 'TestProcessCrashBefore(MigrationCommitReopensCleanly|AppendCommitPreservesAcknowledgedSequence)' -count=20
	$(GO) test -race ./internal/update -run 'TestRecovery(MatrixPreservesOrRestoresVerifiedArtifact|RejectsInvalidJournalWithoutDeletingUnexplainedState)' -count=10
	$(GO) test -race ./internal/upload -run 'TestCleanup(ResumesInterruptedTombstoneStates|PreservesAmbiguousHardLinkedDeviceAndDuplicateMetadata)' -count=10

security: verify-toolchain contracts
	$(GO) test -race ./internal/auth ./internal/connector ./internal/identity ./internal/protocol ./internal/server ./internal/update ./internal/upload

soak: verify-toolchain
	$(GO) test -race ./internal/connector/testharness -run TestRepeatedConnectorLifecycleRetainsNoClients -count=5
	$(GO) test -race ./internal/runtime -run 'TestRuntimeCanBeConstructedStartedAndStoppedRepeatedly|TestHelperCompositionNegotiatesAuthenticatedHealthAndClosesDurableState' -count=5
	$(GO) test -race ./internal/session -run TestConfiguredSessionCapacityRemainsBoundedAndShutsDownCleanly -count=5

vet:
	$(GO) vet ./...

check: verify-toolchain contracts fmt-check vet test build

complete: check race integration platform crash security soak cross-build

generate:
	$(GO) generate ./...

tidy:
	$(GO) mod tidy

clean:
	rm -rf bin dist coverage.out
