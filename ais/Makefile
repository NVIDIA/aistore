SHELL := /bin/bash

# Build version and flags
VERSION := 1.0.0
BUILD := `git rev-parse HEAD`
LDFLAGS=-ldflags "-X=main.version=$(VERSION) -X=main.build=$(BUILD)"

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
	VERSION=`git rev-parse --short HEAD`
	BUILD=`date +%FT%T%z`
	GOBIN=${GOPATH}/bin go install -tags="${CLDPROVIDER}" -ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'" setup/ais.go


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


# Target for linters
LINT_DIRS=$(shell find ../ -type d | grep -v -e '3rdparty' -e 'vendor' -e ".git" -e '__pycache__' -e '^../$$')

.PHONY: lint fmt fmt-fix spell spell-fix cyclo

lint:
	@go get -u github.com/alecthomas/gometalinter
	@go get -u golang.org/x/lint/golint
	@go get -u github.com/kisielk/errcheck
	@go get -u honnef.co/go/tools/cmd/staticcheck
	@echo "Checking vet..."
	@$(foreach dir,$(LINT_DIRS),go tool vet $(dir);)
	@echo "Checking others..."
	@$(GOPATH)/bin/gometalinter --disable-all --enable=golint --enable=errcheck --enable=staticcheck $(LINT_DIRS)

fmt:
	@go get -u golang.org/x/tools/cmd/goimports
	@$(GOPATH)/bin/goimports -d $(LINT_DIRS)  1>&2

fmt-fix:
	@go get -u golang.org/x/tools/cmd/goimports
	@$(GOPATH)/bin/goimports -w $(LINT_DIRS)  1>&2

spell:
	@go get -u github.com/alecthomas/gometalinter
	@go get -u github.com/client9/misspell/cmd/misspell
	@$(GOPATH)/bin/gometalinter --disable-all --enable=misspell $(LINT_DIRS)

spell-fix:
	@go get -u github.com/client9/misspell/cmd/misspell
	@$(foreach dir,$(LINT_DIRS),$(GOPATH)/bin/misspell -w $(dir);)

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
