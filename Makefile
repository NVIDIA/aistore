#
# For usage: `make help`
#
SHELL := /bin/bash
DEPLOY_DIR = ./deploy/dev/local
SCRIPTS_DIR = ./deploy/scripts
BUILD_DIR = ./cmd
BUILD_SRC = $(BUILD_DIR)/aisnode/main.go

# Do not print enter/leave directory when doing 'make -C DIR <target>'
MAKEFLAGS += --no-print-directory

# Uncomment this line to cross-compile:
# CROSS_COMPILE = docker run --rm -v $(shell pwd):/go/src/github.com/NVIDIA/aistore -w /go/src/github.com/NVIDIA/aistore golang:1.18

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
# Example usage: MEM_PROFILE=/tmp/mem make kill clean deploy <<< $'5\n5\n4\n0'
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
# TAGS=nethttp make deploy <<< $'5\n5\n4\n0'

ifeq ($(MODE),debug)
	# Debug mode
	GCFLAGS = -gcflags="all=-N -l"
	LDFLAGS = -ldflags "-X 'main.build=$(VERSION)' -X 'main.buildtime=$(BUILD)'"
	BUILD_TAGS += debug
else
	# Production mode
	GCFLAGS =
	LDFLAGS = -ldflags "-w -s -X 'main.build=$(VERSION)' -X 'main.buildtime=$(BUILD)'"
endif

ifdef AIS_DEBUG
	# Enable `debug` tag also when `AIS_DEBUG` is set.
	# Example AIS_DEBUG usage: $ AIS_DEBUG="fs=4,reb=4" make deploy ...
	# See also: docs/development.md
	BUILD_TAGS += debug
endif

# mono:  utilize go:linkname for monotonic time source
# !mono: use standard Go runtime
BUILD_TAGS += mono

# Colors
cyan = $(shell { tput setaf 6 || tput AF 6; } 2>/dev/null)
term-reset = $(shell { tput sgr0 || tput me; } 2>/dev/null)
$(call make-lazy,cyan)
$(call make-lazy,term-reset)

.PHONY: all node cli cli-autocompletions aisfs authn aisloader xmeta client-bindings

all: node cli aisfs authn aisloader ## Build all main binaries

node: ## Build 'aisnode' binary
	@echo "Building aisnode: build=$(VERSION) providers=$(AIS_BACKEND_PROVIDERS) tags=$(BUILD_TAGS)"
ifdef WRD
	@echo "Deploying with race detector, writing reports to $(subst log_path=,,$(GORACE)).<pid>"
endif
ifdef CROSS_COMPILE
	@$(CROSS_COMPILE) go build -o ./aisnode $(BUILD_FLAGS) -tags="$(BUILD_TAGS)" $(GCFLAGS) $(LDFLAGS) $(BUILD_SRC)
	@mv ./aisnode $(BUILD_DEST)/aisnode
else
	@$(WRD) go build -o $(BUILD_DEST)/aisnode $(BUILD_FLAGS) -tags="$(BUILD_TAGS)" $(GCFLAGS) $(LDFLAGS) $(BUILD_SRC)
endif
	@echo "done."

aisfs-pre:
	@./$(BUILD_DIR)/aisfs/install.sh
aisfs: aisfs-pre build-aisfs ## Build 'aisfs' binary

cli: ## Build CLI ('ais' binary)
	@echo "Building ais (CLI) => $(BUILD_DEST)/ais"
ifdef CROSS_COMPILE
	@$(CROSS_COMPILE) go build -o ./cli $(BUILD_FLAGS) $(LDFLAGS) $(BUILD_DIR)/cli/*.go
	@mv ./cli $(BUILD_DEST)/ais
else
	@go build -o $(BUILD_DEST)/ais $(BUILD_FLAGS) $(LDFLAGS) $(BUILD_DIR)/cli/*.go
endif
	@echo "*** To enable autocompletions in your current shell, run:"
	@echo "*** source $(GOPATH)/src/github.com/NVIDIA/aistore/cmd/cli/autocomplete/bash or"
	@echo "*** source $(GOPATH)/src/github.com/NVIDIA/aistore/cmd/cli/autocomplete/zsh"

cli-autocompletions: ## Add CLI autocompletions
	@echo "Adding CLI autocomplete..."
	@./$(BUILD_DIR)/cli/autocomplete/install.sh

authn: build-authn ## Build 'authn' binary
aisloader: build-aisloader ## Build 'aisloader' binary
xmeta: build-xmeta ## Build 'xmeta' binary

build-%:
	@echo -n "Building $*... "
ifdef CROSS_COMPILE
	@$(CROSS_COMPILE) go build -o ./$* $(BUILD_FLAGS) $(LDFLAGS) $(BUILD_DIR)/$*/*.go
	@mv ./$* $(BUILD_DEST)/.
else
	@go build -o $(BUILD_DEST)/$* $(BUILD_FLAGS) $(LDFLAGS) $(BUILD_DIR)/$*/*.go
endif
	@echo "done."

client-bindings:
	$(SCRIPTS_DIR)/generate-python-api-client.sh

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
.PHONY: kill clean clean-client-bindings

kill: ## Kill all locally deployed targets and proxies
	@which ais >/dev/null || echo "Warning: missing CLI (ais) executable for proper graceful shutdown"
	@ais job stop cluster 2>/dev/null || true
	@"$(DEPLOY_DIR)/kill.sh"

clean: ## Remove all AIS related files and binaries
	@echo -n "Cleaning... "
	@"$(DEPLOY_DIR)/clean.sh"
	@echo "done."

clean-client-bindings: ## Remove all generated client binding files
	$(SCRIPTS_DIR)/clean-python-api-client.sh
#
# go modules
#

.PHONY: mod mod-clean mod-tidy

mod: mod-clean mod-tidy ## Do Go modules chores

# cleanup gomod cache
mod-clean: ## Clean modules
	go clean --modcache

mod-tidy: ## Remove unused modules from 'go.mod'
	go mod tidy

# Target for local docker deployment
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

test-short: test-envcheck ## Run short tests (requires BUCKET variable to be set)
	@RE="$(RE)" BUCKET="$(BUCKET)" TESTS_DIR="$(TESTS_DIR)" AIS_ENDPOINT="$(AIS_ENDPOINT)" $(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" test-short

test-long: test-envcheck ## Run all (long) tests (requires BUCKET variable to be set)
	@RE="$(RE)" BUCKET="$(BUCKET)" TESTS_DIR="$(TESTS_DIR)" AIS_ENDPOINT="$(AIS_ENDPOINT)" $(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" test-long

test-aisloader:
	@./bench/aisloader/test/ci-test.sh $(FLAGS)

test-run: test-envcheck # runs tests matching a specific regex
ifeq ($(RE),)
	$(error missing environment variable: RE="testnameregex")
endif
	@RE="$(RE)" BUCKET="$(BUCKET)" TESTS_DIR="$(TESTS_DIR)" AIS_ENDPOINT="$(AIS_ENDPOINT)" $(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" test-run

test-docker: ## Run tests inside docker
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" test-docker

ci: spell-check fmt-check lint test-short ## Run CI related checkers and linters (requires BUCKET variable to be set)


# Target for linters
.PHONY: lint-update lint fmt-check fmt-fix spell-check spell-fix cyclo msgp-update

## Remove the previous version and upgrade `golangci-lint` to the latest (compare with lint-update-ci below)
lint-update:
	@rm -f $(GOPATH)/bin/golangci-lint
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin latest

## Install specific `golangci-lint` version (hardcoded below)
## See also: .github/workflows/lint.yml
lint-update-ci:
	@rm -f $(GOPATH)/bin/golangci-lint
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.47.3

lint: ## Run linter on whole project
	@([[ -x "$(command -v golangci-lint)" ]] && echo "Cannot find golangci-lint, run 'make lint-update' to install" && exit 1) || true
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" lint

fmt-check: ## Check code formatting
	@ [[ $$(black --help) ]] || pip3 install black[jupyter]
	@ [[ $$(pylint --help) ]] || pip3 install pylint
	@$(SHELL) "$(SCRIPTS_DIR)/bootstrap.sh" fmt

fmt-fix: ## Fix code formatting
	@ [[ $$(black --help) ]] || pip3 install black[jupyter]
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
		"make kill deploy <<< $$'7\n2\n4\ny\ny\nn\nn\n0\n'"  "Shutdown and then (non-interactively) generate local configs and deploy a cluster consisting of 7 targets (4 mountpaths each) and 2 proxies; build 'aisnode' executable with the support for GCP and AWS backends" \
		"make restart <<< $$'7\n2\n4\ny\ny\nn\nn\n0\n'"  "Restart a cluster of 7 targets (4 mountpaths each) and 2 proxies; utilize previously generated (pre-shutdown) local configurations" \
		"RUN_ARGS=-override_backends MODE=debug make kill deploy <<< $$'4\n1\n4\nn\nn\nn\nn\n0\n'"  "Redeploy (4 targets + 1 proxy) cluster; build executable for debug without any backend-supporting libraries; use RUN_ARGS to pass additional command-line option ('-override_backends=true') to each running node"\
		"RUN_ARGS='-override_backends -standby' MODE=debug make kill deploy <<< $$'4\n1\n4\nn\nn\nn\nn\n0\n'"  "Same as above, but additionally run all 4 targets in a standby mode"\
		"make kill clean cli deploy <<< $$'7\n2\n4\ny\ny\nn\nn\n1G\n'"  "Shutdown, cleanup, build CLI, and redeploy from scratch; create 4 loopback devices (size = 1G, one loopback per mountpath)" \
		"GORACE='log_path=/tmp/race' make deploy" "Deploy cluster with race detector, write reports to /tmp/race.<PID>" \
		"MODE=debug make deploy" "Deploy cluster with 'aisnode' (AIS target and proxy) executable built with debug symbols and debug asserts enabled" \
		"BUCKET=tmp make test-short" "Run all short tests" \
		"BUCKET=<existing-cloud-bucket> make test-long" "Run all tests" \
		"BUCKET=tmp make ci" "Run style, lint, and spell checks, as well as all short tests" \
		"MEM_PROFILE=/tmp/mem make deploy" "Deploy cluster with memory profiling enabled, write reports to /tmp/mem.<PID> (and make sure to stop gracefully)" \
		"CPU_PROFILE=/tmp/cpu make deploy" "Build and deploy cluster instrumented for CPU profiling, write reports to /tmp/cpu.<PID>" \
		"TAGS=nethttp make deploy" "Build 'transport' package with net/http (see transport/README.md) and deploy cluster locally" \
		"make client-bindings" "Generate client bindings (ie. the python ais-client)"\
		"make clean-client-bindings" "Clean up all generated client bindings"
