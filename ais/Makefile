SHELL := /bin/bash

# Build version and flags
VERSION = $(shell git rev-parse --short HEAD)
BUILD = $(shell date +%FT%T%z)
LDFLAGS = -ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'"

# Target for simple build
.PHONY: build all
build:
	GODEBUG=madvdontneed=1 GOBIN=${GOPATH}/bin go install -tags="${CLDPROVIDER}" ${LDFLAGS} setup/aisnode.go

all: build

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
	rm -f ${GOPATH}/bin/ais* # cleans 'ais' (CLI), 'aisnode' (TARGET/PROXY), 'aisfs' (FUSE), 'aisloader'
	rm -f ${GOPATH}/pkg/linux_amd64/github.com/NVIDIA/aistore/aisnode.a

#
# go modules
#

.PHONY: mod-clean mod-tidy mod-build mod-init

# cleanup gomod cache
mod-clean:
	go clean --modcache

mod-tidy:
	go mod tidy

mod-build:
	@go build -tags=gcp
	@go build -tags=aws
	@cd ../cli && go build -o /tmp/ais-cli-tmp && rm /tmp/ais-cli-tmp

# reinitialize gomod
# (cleanup caches and download all dependencies)
mod-init: mod-clean mod-tidy mod-build

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
ifeq (${FLAGS},)
	$(warning missing environment variable: FLAGS="stop docker flags")
endif
	@./../deploy/dev/docker/stop_docker.sh ${FLAGS}

#
# tests
#
.PHONY: test-soak test-envcheck test-short test-long test-run test-docker test

# Target for soak test
test-soak:
ifeq (${FLAGS},)
	$(warning FLAGS="soak test flags" not passed, using defaults)
endif
	-@./../bench/soaktest/soaktest.sh ${FLAGS}


test-envcheck:
	@${SHELL} ./setup/bootstrap.sh test-env

test-short: test-envcheck
	@BUCKET=${BUCKET} AISURL=${AISURL} ${SHELL} ./setup/bootstrap.sh test-short

test-long: test-envcheck
	@BUCKET=${BUCKET} AISURL=${AISURL} ${SHELL} ./setup/bootstrap.sh test-long

test-run: test-envcheck # runs tests matching a specific regex
ifeq (${RE},)
	$(error missing environment variable: RE="testnameregex")
endif
	@RE=${RE} BUCKET=${BUCKET} AISURL=${AISURL} ${SHELL} ./setup/bootstrap.sh test-run

test-docker:
	@${SHELL} ./setup/bootstrap.sh test-docker

test: test-envcheck spell-check fmt-check lint test-short # test


# Target for linters
.PHONY: lint-update lint fmt-check fmt-fix spell-check spell-fix cyclo

lint-update:
	@rm -f ${GOPATH}/bin/golangci-lint
	@curl -sfL "https://install.goreleaser.com/github.com/golangci/golangci-lint.sh" | sh -s -- -b ${GOPATH}/bin latest

lint:
	@([[ ! -f ${GOPATH}/bin/golangci-lint ]] && curl -sfL "https://install.goreleaser.com/github.com/golangci/golangci-lint.sh" | sh -s -- -b ${GOPATH}/bin latest) || true
	@${SHELL} ./setup/bootstrap.sh lint

fmt-check:
	@${SHELL} ./setup/bootstrap.sh fmt

fmt-fix:
	@${SHELL} ./setup/bootstrap.sh fmt --fix

spell-check:
	@GO111MODULE=off go get -u github.com/client9/misspell/cmd/misspell
	@${SHELL} ./setup/bootstrap.sh spell

spell-fix:
	@GO111MODULE=off go get -u github.com/client9/misspell/cmd/misspell
	@${SHELL} ./setup/bootstrap.sh spell --fix


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

# Target for devinit
.PHONY: devinit

# To help with working with a non-github remote
# It replaces existing github.com AIStore remote with the one specified by REMOTE
devinit:
	@${SHELL} ./setup/bootstrap.sh devinit
