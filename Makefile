BINARY := paperboat-helper
PACKAGE := ./cmd/paperboat-helper
GO_VERSION := 1.25.7
GO := GOTOOLCHAIN=local go
GOFMT := $(shell GOTOOLCHAIN=local go env GOROOT 2>/dev/null)/bin/gofmt
GO_FILES := $(shell find . -path ./.git -prune -o -name '*.go' -print)
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT ?= $(shell git rev-parse --verify HEAD 2>/dev/null || echo unknown)
LDFLAGS := -X github.com/pinksaucepasta/paperboat-helper/internal/buildinfo.Version=$(VERSION) -X github.com/pinksaucepasta/paperboat-helper/internal/buildinfo.Commit=$(COMMIT)

.PHONY: build check clean fmt fmt-check generate race test tidy verify-toolchain vet

verify-toolchain:
	@test "$$(GOTOOLCHAIN=local go env GOVERSION)" = "go$(GO_VERSION)" || { echo "required Go $(GO_VERSION), found $$(GOTOOLCHAIN=local go env GOVERSION)" >&2; exit 1; }

build: verify-toolchain
	@mkdir -p bin
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY) $(PACKAGE)

fmt:
	$(GOFMT) -w $(GO_FILES)

fmt-check:
	@test -z "$$($(GOFMT) -l $(GO_FILES))" || { $(GOFMT) -l $(GO_FILES); echo "Go files are not formatted" >&2; exit 1; }

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

check: verify-toolchain fmt-check vet test build

generate:
	$(GO) generate ./...

tidy:
	$(GO) mod tidy

clean:
	rm -rf bin dist coverage.out
