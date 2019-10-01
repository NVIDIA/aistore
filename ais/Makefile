SHELL := /bin/bash

# Build version and flags
VERSION=$(shell git rev-parse --short HEAD)
BUILD=$(shell date +%FT%T%z)
LDFLAGS=-ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'"


# Target for devinit
.PHONY: devinit

# To help with working with a non-github remote
# It replaces existing github.com AIStore remote with the one specified by REMOTE
devinit:
ifeq ($(REMOTE),)
	$(error missing environment variable: REMOTE="http://path/to/remote")
endif
	@if [ -z "$$(which git)" ]; then \
		echo "git not installed"; \
	elif [ -z "$$(git remote -v | grep origin)" ]; then \
		git remote add origin "${REMOTE}"; \
	else \
		git remote set-url origin "${REMOTE}"; \
	fi


# Target for simple build
.PHONY: build

build:
	GODEBUG=madvdontneed=1 GOBIN=${GOPATH}/bin go install -tags="${CLDPROVIDER}" ${LDFLAGS} setup/aisnode.go

#
# cleanup local deployment (cached objects, logs, and executables)
#
.PHONY: kill rmcache clean mod-clean mod-init

kill:
	@pkill -SIGINT aisnode 2>/dev/null; sleep 1; true
	@pkill authn 2>/dev/null; sleep 1; true
	@pkill -SIGKILL aisnode 2>/dev/null; true

# delete only caches, not logs
rmcache:
	@./setup/rmcache.sh

clean:
	rm -rf ~/.ais*
	rm -rf /tmp/ais
	rm -f $(GOPATH)/bin/aisnode
	rm -f $(GOPATH)/bin/ais
	rm -f $(GOPATH)/pkg/linux_amd64/github.com/NVIDIA/aistore/aisnode.a

#
# go modules
#

.PHONY: mod-clean mod-tidy mod-build mod-sepa1 mod-sepa2 mod-init

# cleanup gomod cache
mod-clean:
	go clean --modcache

mod-tidy:
	go mod tidy

mod-build:
	@go build -tags=gcp
	@go build -tags=aws
	@cd ../cli && go build -o /tmp/ais-cli-tmp && rm /tmp/ais-cli-tmp

mod-sepa1:
	@echo tidy: ============================================
mod-sepa2:
	@echo build: ===========================================

# reinitialize gomod
# (cleanup caches and download all dependencies)
mod-init: mod-clean mod-sepa1 mod-tidy mod-sepa2 mod-build

#
# local deployment
#
.PHONY: install_cli deploy

# AIS CLI
install_cli:
	@cd ../cli && ./install.sh

deploy:
	@./setup/deploy.sh

# Target for local docker deployment
.PHONY: deploy_docker stop_docker

deploy_docker:
# pass -d=2 because need 2 mountpaths for some tests
	@cd ../deploy/dev/docker && ./deploy_docker.sh -d=2

stop_docker:
ifeq ($(FLAGS),)
	$(warning missing environment variable: FLAGS="stop docker flags")
endif
	@./../deploy/dev/docker/stop_docker.sh $(FLAGS)

#
# tests
#
.PHONY: test-soak test-envcheck test-short test-long test-run test-docker test

# Target for soak test
test-soak:
ifeq ($(FLAGS),)
	$(warning FLAGS="soak test flags" not passed, using defaults)
endif
	-@./../bench/soaktest/soaktest.sh $(FLAGS)


test-envcheck:
ifeq ($(BUCKET),)
	$(error missing environment variable: BUCKET="bucketname")
endif
	@CHECK="test-env" $(SHELL) ./setup/preflight_check.sh

test-short: test-envcheck
	@CHECK="test-short" BUCKET=$(BUCKET) AISURL=$(AISURL) $(SHELL) ./setup/preflight_check.sh

test-long: test-envcheck
	@CHECK="test-long" BUCKET=$(BUCKET) AISURL=$(AISURL) $(SHELL) ./setup/preflight_check.sh

test-run: test-envcheck #runs tests matching a specific regex
ifeq ($(RE),)
	$(error missing environment variable: RE="testnameregex")
endif
	@CHECK="test-run" RE=$(RE) BUCKET=$(BUCKET) AISURL=$(AISURL) $(SHELL) ./setup/preflight_check.sh

test-docker:
	@CHECK="test-docker" $(SHELL) ./setup/preflight_check.sh

test: test-envcheck spell-check fmt-check lint test-short #main preflight test


# Target for linters
LINT_DIRS := $(shell go list -f '{{.Dir}}' ../... | grep -v -e '3rdparty' -e 'vendor')
MISSPELL_DIRS := $(shell find ../ -maxdepth 1 | grep -v -e '3rdparty' -e 'vendor' -e ".git" -e '__pycache__' -e '^../$$')

.PHONY: lint fmt-check fmt-fix spell-check spell-fix cyclo

lint:
# golangci installs the linters it supports
	@GO111MODULE=off go get -u github.com/golangci/golangci-lint/cmd/golangci-lint
	@CHECK="lint" LINT_DIRS="$(LINT_DIRS)" bash ./setup/preflight_check.sh

fmt-check:
	@GO111MODULE=off go get -u golang.org/x/tools/cmd/goimports
	@CHECK="fmt" LINT_DIRS="$(LINT_DIRS)" $(SHELL) ./setup/preflight_check.sh

fmt-fix:
	@GO111MODULE=off go get -u golang.org/x/tools/cmd/goimports
	@$(GOPATH)/bin/goimports -w $(LINT_DIRS)

spell-check:
	@GO111MODULE=off go get -u github.com/client9/misspell/cmd/misspell
	@CHECK="spell" MISSPELL_DIRS="$(MISSPELL_DIRS)" $(SHELL) ./setup/preflight_check.sh

spell-fix:
	@GO111MODULE=off go get -u github.com/client9/misspell/cmd/misspell
	@$(foreach dir,$(MISSPELL_DIRS) ,$(GOPATH)/bin/misspell -w $(dir);)

cyclo:
	@GO111MODULE=off go get -u github.com/fzipp/gocyclo
	@$(GOPATH)/bin/gocyclo -over 25 $(LINT_DIRS)


# Misc Targets
.PHONY: numget cpuprof flamegraph code-coverage

# example extracting 'numget' stats out of all local logs
numget:
	@./setup/numget.sh

# run benchmarks 10 times to generate cpu.prof
cpuprof:
	@go test -v -run=XXX -bench=. -count 10 -cpuprofile=/tmp/cpu.prof

flamegraph: cpuprof
	@go tool pprof -http ":6060" /tmp/cpu.prof

code-coverage:
	@./setup/code_coverage.sh
