SHELL := /bin/bash

# Build version and flags
VERSION=$(shell git rev-parse --short HEAD)
BUILD=$(shell date +%FT%T%z)
LDFLAGS=-ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'"

REPOPATH=src/github.com/NVIDIA
DEVPATH=src/gitlab-master.nvidia.com/aistorage/


# Target for gitlab repository
.PHONY: devinit devrefresh

# Only for gitlab repository
# It replaces existing github.com AIStore repository with a symlink to gitlab one
# WARNING: it erases directory $GOPATH/src/github.com/NVIDIA, so make sure
#    that you are done with it before executing 'make devinit'
# Run this target once after you 'go get' AIStore for the first time
devinit:
	@if [ -d "${GOPATH}/${REPOPATH}" ] && [ ! -L "${GOPATH}/${REPOPATH}" ]; then \
		echo "Removing ${GOPATH}/${REPOPATH}" && \
		rm -r "${GOPATH}/${REPOPATH}"; \
	fi
	@if [ -d "${GOPATH}/${REPOPATH}" ]; then \
		echo "Deleting symbolic link" && \
		rm "${GOPATH}/${REPOPATH}"; \
	fi
	@if [ ! -d "${GOPATH}/${REPOPATH}" ]; then \
		echo "Creating symbolic link" && \
		ln -s "${GOPATH}/${DEVPATH}" "${GOPATH}/${REPOPATH}"; \
	fi

# Only for gitlab repository
# Use this target if $GOPATH changed and you want to recreate symlink
#   or just to create symlink if it does not exists
# This target is safe and does not delete any data
devrefresh:
	@if [ -d "${GOPATH}/${REPOPATH}" ] && [ -L "${GOPATH}/${REPOPATH}" ]; then \
		echo "Refreshing symbolic link ${GOPATH}/${REPOPATH}" && \
		rm -r "${GOPATH}/${REPOPATH}"; \
	fi
	@if [ ! -d "${GOPATH}/${REPOPATH}" ]; then \
		echo "Creating symbolic link" && \
		ln -s "${GOPATH}/${DEVPATH}" "${GOPATH}/${REPOPATH}"; \
	else echo "Failed to create symlink: a regular directory exists"; \
	fi


# Target for simple build
.PHONY: build

build:
	GOBIN=${GOPATH}/bin go install -tags="${CLDPROVIDER}" ${LDFLAGS} setup/ais.go


# Target for cleaning deployment
.PHONY: kill rmcache clean

kill:
	@pkill -SIGINT ais 2>/dev/null; sleep 1; true
	@pkill authn 2>/dev/null; sleep 1; true
	@pkill -SIGKILL ais 2>/dev/null; true

# delete only caches, not logs
rmcache:
	@./setup/rmcache.sh

clean:
	rm -rf ~/.ais*
	rm -rf /tmp/ais
	rm -f $(GOPATH)/bin/ais
	rm -f $(GOPATH)/pkg/linux_amd64/github.com/NVIDIA/aistore/ais.a


# Target for local deployment
.PHONY: deploy deploy_next_tier

deploy:
	@./setup/deploy.sh

deploy_next_tier:
	@DEPLOY_AS_NEXT_TIER=1 ./setup/deploy.sh


# Target for tests
.PHONY: test-envcheck test-short test-long test-run test

test-envcheck:
ifeq ($(BUCKET),)
	$(error missing environment variable: BUCKET="bucketname")
endif
	@CHECK="test-env" $(SHELL) ./setup/preflight_check.sh

test-short: test-envcheck
	@CHECK="test-short" BUCKET=$(BUCKET) $(SHELL) ./setup/preflight_check.sh

test-long: test-envcheck
	@CHECK="test-long" BUCKET=$(BUCKET) $(SHELL) ./setup/preflight_check.sh

test-run: test-envcheck #runs tests matching a specific regex
ifeq ($(RE),)
	$(error missing environment variable: RE="testnameregex")
endif
	@CHECK="test-run" RE=$(RE) BUCKET=$(BUCKET) $(SHELL) ./setup/preflight_check.sh

test: test-envcheck spell-check fmt-check lint-install lint test-short #main preflight test


# Target for linters
LINT_DIRS := $(shell find ../ -type d | grep -v -e '3rdparty' -e 'vendor' -e ".git" -e '__pycache__' -e '^../$$')
MISSPELL_DIRS := $(shell find ../ -maxdepth 1 | grep -v -e '3rdparty' -e 'vendor' -e ".git" -e '__pycache__' -e '^../$$')

.PHONY: lint lint-install fmt-check fmt-fix spell-check spell-fix cyclo

lint-install:
	@go get -u github.com/alecthomas/gometalinter
	@go get -u golang.org/x/lint/golint
	@go get -u github.com/kisielk/errcheck
	@go get -u honnef.co/go/tools/cmd/staticcheck

LINT_VET_IGNORE := -composites=false
LINT_IGNORE := "(should have comment|comment on exported)"

lint:
	@CHECK="lint" LINT_VET_IGNORE=$(LINT_VET_IGNORE) LINT_IGNORE=$(LINT_IGNORE) LINT_DIRS="$(LINT_DIRS)" bash ./setup/preflight_check.sh

fmt-check:
	@go get -u golang.org/x/tools/cmd/goimports
	@CHECK="fmt" LINT_DIRS="$(LINT_DIRS)" $(SHELL) ./setup/preflight_check.sh

fmt-fix:
	@go get -u golang.org/x/tools/cmd/goimports
	@$(GOPATH)/bin/goimports -w $(LINT_DIRS)

spell-check:
	@go get -u github.com/client9/misspell/cmd/misspell
	@CHECK="spell" MISSPELL_DIRS="$(MISSPELL_DIRS)" $(SHELL) ./setup/preflight_check.sh

spell-fix:
	@go get -u github.com/client9/misspell/cmd/misspell
	@$(foreach dir,$(MISSPELL_DIRS) ,$(GOPATH)/bin/misspell -w $(dir);)

cyclo:
	@go get github.com/fzipp/gocyclo
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
