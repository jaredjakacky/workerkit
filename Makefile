SHELL := /bin/sh
.ONESHELL:
.SHELLFLAGS := -eu -c

GO ?= go
GOFMT ?= gofmt
PKGS ?= ./...
COVER_PKGS ?= $(shell $(GO) list $(PKGS) | grep -v '/examples')
GOFILES := $(filter-out $(shell git ls-files --deleted -- '*.go'),$(shell git ls-files -- '*.go'))
EXAMPLE_PKGS ?= $(shell $(GO) list ./examples/... | grep -v '/examples/testing$$')
GOVULNCHECK_VERSION ?= v1.4.0
ALLOW_TIDY_CHANGES ?= 0

# Keep build cache inside the repo so local runs are reproducible and do not
# depend on a writable global cache path.
export GOCACHE ?= $(CURDIR)/.cache/go-build

.DEFAULT_GOAL := help

.PHONY: \
	help \
	build-examples \
	test-examples \
	fmt \
	fmt-check \
	vet \
	test \
	test-race \
	coverage \
	tidy \
	tidy-check \
	govulncheck \
	verify \
	clean

help: ## Show available targets.
	@printf "Available targets:\n"
	awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_.-]+:.*##/ {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build-examples: ## Compile the runnable example programs.
	@echo "==> build examples"
	$(GO) build $(EXAMPLE_PKGS)

test-examples: ## Run tests for test-only example packages.
	@echo "==> test examples"
	$(GO) test ./examples/testing

fmt: ## Format tracked Go source files.
	@echo "==> formatting"
	$(GOFMT) -w $(GOFILES)

fmt-check: ## Verify tracked Go source files are formatted.
	@echo "==> checking formatting"
	out="$$( $(GOFMT) -l $(GOFILES) )"
	if [ -n "$$out" ]
	then
		echo "The following files are not formatted:"
		echo "$$out"
		exit 1
	fi

vet: ## Run go vet on all packages.
	@echo "==> vet"
	$(GO) vet $(PKGS)

test: ## Run tests for all packages.
	@echo "==> test"
	$(GO) test $(PKGS)

test-race: ## Run tests with the race detector enabled.
	@echo "==> test race"
	$(GO) test -race $(PKGS)

coverage: ## Run library package tests with coverage output written to coverage.out.
	@echo "==> coverage"
	$(GO) test -coverprofile=coverage.out $(COVER_PKGS)
	$(GO) tool cover -func=coverage.out | tail -1

tidy: ## Run go mod tidy and fail on go.mod/go.sum changes unless allowed.
	@echo "==> tidy"
	$(GO) mod tidy
	if [ "$(ALLOW_TIDY_CHANGES)" != "1" ]
	then
		if ! git diff --quiet -- go.mod go.sum 2>/dev/null
		then
			echo "go mod tidy changed go.mod/go.sum. Commit the changes or rerun with ALLOW_TIDY_CHANGES=1."
			set +e
			git --no-pager diff -- go.mod go.sum
			set -e
			exit 1
		fi
	fi

tidy-check: ## Verify go.mod/go.sum are already tidy.
	@$(MAKE) tidy ALLOW_TIDY_CHANGES=0

govulncheck: ## Run the pinned govulncheck tool against the main module packages.
	@echo "==> govulncheck"
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) $(PKGS)

verify: fmt-check vet test build-examples test-examples tidy-check ## Run the local verification suite.
	@echo "==> verification passed"

clean: ## Remove local build outputs and caches.
	@echo "==> clean"
	rm -rf .cache .bin coverage.out
