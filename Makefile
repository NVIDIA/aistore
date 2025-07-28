#
# Usage: `make help`
#
SHELL := /bin/bash
DEPLOY_DIR = ./deploy/dev/local
SCRIPTS_DIR = ./scripts
BUILD_DIR = ./cmd
BUILD_SRC = $(BUILD_DIR)/aisnode/main.go

ifeq ($(shell go env GOOS),linux)
ifeq ($(strip $(GORACE)),)
	CGO_DISABLE = CGO_ENABLED=0
endif
endif

AISTORE_PATH = $(shell git rev-parse --show-toplevel)

CLI_VERSION := $(shell ais version 2>/dev/null)

# Do not print enter/leave directory when doing 'make -C DIR <target>'
MAKEFLAGS += --no-print-directory

# Uncomment to cross-compile aisnode and cli, respectively:
# CROSS_COMPILE = docker run --rm -v $(AISTORE_PATH):/go/src/n -w /go/src/n golang:1.24
# CROSS_COMPILE_CLI = docker run -e $(CGO_DISABLE) --rm -v $(AISTORE_PATH)/cmd/cli:/go/src/n -w /go/src/n golang:1.24

# Build version, flags, and tags
VERSION = $(shell git rev-parse --short HEAD)
BUILD = $(shell date +%FT%T%z)
BUILD_FLAGS += $(if $(strip $(GORACE)),-race,)
BUILD_DEST = $(GOPATH)/bin
ifdef GOBIN
	BUILD_DEST = $(GOBIN)
endif
ifdef TAGS
	BUILD_TAGS = $(AIS_BACKEND_PROVIDERS) $(TAGS)
else
	BUILD_TAGS = $(AIS_BACKEND_PROVIDERS)
endif

# Race detection
ifneq ($(strip $(GORACE)),)
	WRD = GORACE=$(GORACE)
ifneq ($(findstring log_path,$(GORACE)),log_path)
	@echo
	@echo "Expecting GORACE='log_path=...', run 'make help' for usage examples"
	@exit 1
endif
endif

# Profiling
# Example usage: MEM_PROFILE=/tmp/mem make kill clean deploy <<< $'5\n5\n4\ny\ny\nn\n'
# Note that MEM_PROFILE (memprofile) option requires graceful shutdown (see `kill:`)
ifdef MEM_PROFILE
	export AIS_NODE_FLAGS += -memprofile=$(MEM_PROFILE)
	BUILD_SRC = $(BUILD_DIR)/aisnodeprofile/main.go
endif
ifdef CPU_PROFILE
	export AIS_NODE_FLAGS += -cpuprofile=$(CPU_PROFILE)
	BUILD_SRC = $(BUILD_DIR)/aisnodeprofile/main.go
endif

# Intra-cluster networking: two alternative ways to build AIS `transport` package:
# 1) using Go net/http, or
# 2) with a 3rd party github.com/valyala/fasthttp aka "fasthttp"
#
# The second option is the current default.
# To build with net/http, use `nethttp` build tag, for instance:
# TAGS=nethttp make deploy <<< $'5\n5\n4\ny\ny\nn\n'

ifeq ($(MODE),debug)
	# Debug mode
	GCFLAGS = -gcflags="all=-N -l"
	LDFLAGS = -ldflags "-X 'main.build=$(VERSION)' -X 'main.buildtime=$(BUILD)'"
	BUILD_TAGS += debug
	GOFLAGS =
else
	# Production mode
	GCFLAGS =
	LDFLAGS = -ldflags "-w -s -X 'main.build=$(VERSION)' -X 'main.buildtime=$(BUILD)'"
	GOFLAGS = -trimpath
endif

# mono:  utilize go:linkname for monotonic time source
# !mono: use standard Go runtime
BUILD_TAGS += mono

# Colors
cyan = $(shell { tput setaf 6 || tput AF 6; } 2>/dev/null)
term-reset = $(shell { tput sgr0 || tput me; } 2>/dev/null)
$(call make-lazy,cyan)
$(call make-lazy,term-reset)

.PHONY: all node cli cli-autocompletions authn aisloader xmeta

all: node cli authn aisloader ## Build all main binaries

node: ## Build 'aisnode' binary
	@echo "Building aisnode $(VERSION) [build tags:$(BUILD_TAGS)]"
ifdef WRD
	@echo "Deploying with race detector, writing reports to $(subst log_path=,,$(GORACE)).<pid>"
endif
ifdef CROSS_COMPILE
	@$(CROSS_COMPILE) go build -o ./aisnode $(BUILD_FLAGS) -tags="$(BUILD_TAGS)" $(GCFLAGS) $(GOFLAGS) $(LDFLAGS) $(BUILD_SRC)
	@mv ./aisnode $(BUILD_DEST)/.
else
	@$(WRD) go build -o $(BUILD_DEST)/aisnode $(BUILD_FLAGS) -tags="$(BUILD_TAGS)" $(GCFLAGS) $(GOFLAGS) $(LDFLAGS) $(BUILD_SRC)
endif
	@echo "done."

cli: ## Build CLI binary. NOTE: 1) a separate go.mod, 2) static linkage with cgo disabled
	@echo "Building ais (CLI) [build tags:$(BUILD_TAGS)]"
ifdef CROSS_COMPILE_CLI
	cd $(BUILD_DIR)/cli && \
	$(CROSS_COMPILE_CLI) go build -o ./ais -tags="$(BUILD_TAGS)" $(BUILD_FLAGS) $(GOFLAGS) $(LDFLAGS) *.go && mv ./ais $(BUILD_DEST)/.
else
	@cd $(BUILD_DIR)/cli && $(CGO_DISABLE) go build -o $(BUILD_DEST)/ais -tags="$(BUILD_TAGS)" $(BUILD_FLAGS) $(GOFLAGS) $(LDFLAGS) *.go
endif
	@echo "*** To enable autocompletions in your current shell, run:"
ifeq ($(AISTORE_PATH),$(PWD))
	@echo "*** 'source cmd/cli/autocomplete/bash' or 'source cmd/cli/autocomplete/zsh'"
else
	@echo "*** source $(AISTORE_PATH)/cmd/cli/autocomplete/[bash|zsh]"
endif

cli-autocompletions: ## Add CLI autocompletions
	@echo "Adding CLI autocomplete..."
	@./$(BUILD_DIR)/cli/autocomplete/install.sh

ishard: ## Build ishard CLI binary
	@echo "Building ishard..."
	@cd $(BUILD_DIR)/ishard && go build -o $(BUILD_DEST)/ishard *.go
	@echo "done."

authn: build-authn         ## Build AuthN
aisloader: build-aisloader ## Build aisloader
xmeta: build-xmeta         ## Build xmeta
aisinit: build-aisinit     ## Build aisinit

build-%:
	@echo -n "Building $*... "
ifdef CROSS_COMPILE
	@$(CROSS_COMPILE) go build -o ./$* $(BUILD_FLAGS) $(GOFLAGS) $(LDFLAGS) $(BUILD_DIR)/$*/*.go
	@mv ./$* $(BUILD_DEST)/.
else
	@go build -o $(BUILD_DEST)/$* $(BUILD_FLAGS) $(GOFLAGS) $(LDFLAGS) $(BUILD_DIR)/$*/*.go
endif
	@echo "done."

#
# local deployment (intended for developers)
#
.PHONY: deploy run restart

## Build 'aisnode' and parse command-line to generate configurations
## and deploy the specified numbers of local AIS (proxy and target) daemons
deploy:
	@"$(DEPLOY_DIR)/deploy.sh" $(RUN_ARGS)

## Same as `deploy` except for (not) generating daemon configurations.
## Use `make run` to restart cluster and utilize existing daemon configs.
run:
	@"$(DEPLOY_DIR)/deploy.sh" $(RUN_ARGS) --dont-generate-configs

restart: kill run

#
# cleanup local deployment (cached objects, logs, and executables)
#
.PHONY: kill clean

kill: ## Kill all locally deployed clusters (all targets and all proxies)
ifndef CLI_VERSION
	@echo "Warning: missing CLI (ais) executable for proper graceful shutdown"
else
	@ais cluster shutdown -y 2>/dev/null || true
endif
	@"$(DEPLOY_DIR)/kill.sh"

clean: ## Remove all AIS related files and binaries
	@echo -n "Cleaning... "
	@"$(DEPLOY_DIR)/clean.sh"
	@rm -rf .docs
	@echo "done."

#
# modules
#
.PHONY: mod-all mod-clean mod-tidy

mod-all: mod-clean mod-tidy
	@echo "CLI ..." && cd cmd/cli && $(MAKE) mod-tidy
	@echo "ishard ..." && cd cmd/ishard && $(MAKE) mod-tidy

# cleanup go-mod cache
mod-clean:
	go clean --modcache

# in particular, remove unused
mod-tidy:
	go mod tidy

# for local docker deployment
.PHONY: deploy-docker stop-docker

deploy-docker: ## Deploy AIS cluster inside the dockers
# pass -d=3 because need 3 mountpaths for some tests
	@cd ./deploy/dev/docker && ./deploy_docker.sh -d=3

stop-docker: ## Stop deployed AIS docker cluster
ifeq ($(FLAGS),)
	$(warning missing environment variable: FLAGS="stop docker flags")
endif
	@./deploy/dev/docker/stop_docker.sh $(FLAGS)

#
# tests
#
.PHONY: test-bench test-aisloader test-envcheck test-short test-long test-run test-docker test

# Target for benchmark tests
test-bench: ## Run benchmarking tests
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" test-bench

test-envcheck:
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" test-env

test-short: test-envcheck ## Run short tests
	@RE="$(RE)" BUCKET="$(BUCKET)" TESTS_DIR="$(TESTS_DIR)" AIS_ENDPOINT="$(AIS_ENDPOINT)" $(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" test-short
	@cd $(BUILD_DIR)/cli && go test -v -tags=debug ./...

test-tracing-unit:
	@cd tracing && go test -v -tags=oteltracing ./...

test-assorted: test-envcheck # Run specific tests
	@RE="ETLBucket|ETLFQN" BUCKET="$(BUCKET)" TESTS_DIR="$(TESTS_DIR)" AIS_ENDPOINT="$(AIS_ENDPOINT)" $(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" test-long

test-long: test-envcheck ## Run all integration tests
	@RE="$(RE)" BUCKET="$(BUCKET)" TESTS_DIR="$(TESTS_DIR)" AIS_ENDPOINT="$(AIS_ENDPOINT)" $(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" test-long
	@cd $(BUILD_DIR)/cli && go test -v -tags=debug ./...

test-aisloader:
	@./bench/tools/aisloader/test/ci-test.sh $(FLAGS)

test-run: test-envcheck # runs tests matching a specific regex
ifeq ($(RE),)
	$(error missing environment variable: RE="testnameregex")
endif
	@RE="$(RE)" BUCKET="$(BUCKET)" TESTS_DIR="$(TESTS_DIR)" AIS_ENDPOINT="$(AIS_ENDPOINT)" $(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" test-run

test-docker: ## Run tests inside docker
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" test-docker

ci: spell-check fmt-check lint test-short ## Run CI related checkers and linters (requires BUCKET variable to be set)


# Target for linters
.PHONY: lint-update lint install-python-deps fmt-check fmt-fix spell-check spell-fix cyclo msgp-update

## Removes the previous version of `golangci-lint` and installs the latest (compare with lint-update-ci below)
lint-update:
	@rm -f $(GOPATH)/bin/golangci-lint
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin latest

## Install specific `golangci-lint` version (hardcoded)
## See also: .github/workflows/lint.yml
lint-update-ci:
	@rm -f $(GOPATH)/bin/golangci-lint
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v2.3.0

lint:
	@([[ -x "$(command -v golangci-lint)" ]] && echo "Cannot find golangci-lint, run 'make lint-update' to install" && exit 1) || true
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" lint
	@$(MAKE) -C $(BUILD_DIR)/cli lint
	@$(MAKE) -C $(BUILD_DIR)/ishard lint

install-python-deps:
	@cd python && make common_deps botocore_deps

fmt-check: install-python-deps ## Check code formatting
	@pip3 install --upgrade black[jupyter] -q
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" fmt

fmt-fix: ## Fix code formatting
	@pip3 install --upgrade black[jupyter] -q
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" fmt --fix

spell-check: ## Run spell checker on the project
	@GOOS="" go install github.com/client9/misspell/cmd/misspell@latest
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" spell

spell-fix: ## Fix spell checking issues
	@GOOS="" go install github.com/client9/misspell/cmd/misspell@latest
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" spell --fix

msgp-update: ## MessagePack generator
	@GOOS="" go install github.com/tinylib/msgp@latest

# Misc Targets
.PHONY: cpuprof flamegraph dev-init

# run benchmarks 10 times to generate cpu.prof
cpuprof:
	@go test -v -run=XXX -bench=. -count 10 -cpuprofile=/tmp/cpu.prof

flamegraph: cpuprof
	@go tool pprof -http ":6060" /tmp/cpu.prof

# To help with working with a non-github remote
# It replaces existing github.com AIStore remote with the one specified by REMOTE
dev-init:
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" dev-init

.PHONY: help
help:
	@echo "Usage:"
	@echo "  [VAR=foo VAR2=bar...] make [target...]"
	@echo ""
	@echo "Useful commands:"
	@grep -Eh '^[a-zA-Z._-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(cyan)%-30s$(term-reset) %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@printf "  $(cyan)%s$(term-reset)\n    %s\n\n" \
		"make deploy" "Deploy cluster locally" \
		"make kill clean" "Stop locally deployed cluster and cleanup all cluster-related data and bucket metadata (but not cluster map)" \
		"make kill deploy <<< $$'7\n2\n4\ny\ny\n'"  "Shutdown and then (non-interactively) generate local configs and deploy a cluster consisting of 7 targets (4 mountpaths each) and 2 proxies; build 'aisnode' executable with AWS and GCP backends" \
		"TAGS=\"aws gcp\" make kill deploy <<< $$'7\n2\n'"  "Same as above (see docs/getting_started.md for many more examples)" \
		"GORACE='log_path=/tmp/race' make deploy" "Deploy cluster with race detector, write reports to /tmp/race.<PID>" \
		"MODE=debug make deploy" "Deploy cluster with 'aisnode' (AIS target and proxy) executable built with debug symbols and debug asserts enabled" \
		"BUCKET=tmp make test-short" "Run all short tests" \
		"BUCKET=<existing-cloud-bucket> make test-long" "Run all tests" \
		"BUCKET=tmp make ci" "Run style, lint, and spell checks, as well as all short tests" \
		"MEM_PROFILE=/tmp/mem make deploy" "Deploy cluster with memory profiling enabled, write reports to /tmp/mem.<PID> (and make sure to stop gracefully)" \
		"CPU_PROFILE=/tmp/cpu make deploy" "Build and deploy cluster instrumented for CPU profiling, write reports to /tmp/cpu.<PID>" \
		"TAGS=nethttp make deploy" "Build 'transport' package with net/http (see transport/README.md) and deploy cluster locally" \

.PHONY: restful-api-doc

## Generate RESTful API documentation using swagger
restful-api-doc: ## Generate OpenAPI/Swagger documentation from code annotations
	@echo "Generating swagger annotations from code comments..."
	@GOROOT="" go generate ./...
	@echo "Installing swag if not present..."
	@command -v swag >/dev/null 2>&1 || GOOS="" go install github.com/swaggo/swag/cmd/swag@v1.16.4
	@echo "Generating OpenAPI specification..."
	@mkdir -p .docs
	@swag init --generalInfo tools/gendocs/annotations.go --output .docs
	@echo "Injecting model extensions..."
	@GOROOT="" go run ./tools/gendocs -inject-extensions
	@echo "Cleaning up generated annotations file..."
	@GOROOT="" go run ./tools/gendocs -cleanup
	@echo "$(cyan)Documentation generated successfully!$(term-reset)"
	@echo "$(cyan)Generated files:$(term-reset)"
	@echo "  .docs/swagger.json  - OpenAPI JSON specification"
	@echo "  .docs/swagger.yaml  - OpenAPI YAML specification"
	@echo "  .docs/docs.go       - Go documentation file"
	@echo ""
	@echo "$(cyan)To test the documentation:$(term-reset)"
	@echo "  1. Copy the content of .docs/swagger.json"
	@echo "  2. Paste it into https://editor.swagger.io/"
	@echo "  3. View the rendered API documentation"


.PHONY: api-docs-website

## Generate API documentation for the website with custom template
api-docs-website: restful-api-doc ## Generate complete API documentation for Jekyll website
	@echo "$(cyan)Generating website API documentation...$(term-reset)"
	@echo "Checking OpenAPI Generator CLI availability..."
	@if ! command -v openapi-generator-cli >/dev/null 2>&1; then \
		echo "$(red)Error: openapi-generator-cli not found in PATH$(term-reset)"; \
		echo "$(cyan)Please install it using the bash launcher script:$(term-reset)"; \
		echo "  mkdir -p ~/bin/openapitools"; \
		echo "  curl https://raw.githubusercontent.com/OpenAPITools/openapi-generator/master/bin/utils/openapi-generator-cli.sh > ~/bin/openapitools/openapi-generator-cli"; \
		echo "  chmod u+x ~/bin/openapitools/openapi-generator-cli"; \
		echo "  export PATH=\$$PATH:~/bin/openapitools"; \
		exit 1; \
	fi
	@echo "Generating markdown documentation with custom template..."
	@openapi-generator-cli generate -i .docs/swagger.yaml -g markdown -o ./docs-generated --template-dir ./markdown-template --skip-validate-spec
	@echo "Copying generated documentation to website location..."
	@cp docs-generated/README.md docs/http-api.md
	@cp -r docs-generated/Apis docs/
	@cp -r docs-generated/Models docs/
	@echo "Adding Jekyll front matter..."
	@./scripts/website-preprocess.sh
	@echo "$(cyan)Website API documentation generated successfully!$(term-reset)"
	@echo "$(cyan)Updated files:$(term-reset)"
	@echo "  docs/http-api.md - Website HTTP API documentation"
	@echo "  docs-generated/README.md - Generated documentation"
	@echo ""
	@echo "$(cyan)The documentation is now ready for the Jekyll website!$(term-reset)"
