SHELL := /bin/bash

# Build version and flags
VERSION := 1.0.0
BUILD := `git rev-parse HEAD`
LDFLAGS=-ldflags "-X=dfc.Version=$(VERSION) -X=dfc.Build=$(BUILD)"

REPOPATH=src/github.com/NVIDIA
DEVPATH=src/gitlab-master.nvidia.com/aistorage/

# Only for gitlab repository
# It replaces existing github.com DFC repository with a symlink to gitlab one
# WARNING: it erases directory $GOPATH/src/github.com/NVIDIA, so make sure
#    that you are done with it before executing 'make devinit'
# Run this target once after you 'go get' DFC for the first time
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

build:
	GOBIN=${GOPATH}/bin go build && go install
	GOBIN=${GOPATH}/bin go install setup/dfc.go

kill:
	@pkill -SIGINT dfc 2>/dev/null; sleep 1; true
	@pkill authn 2>/dev/null; sleep 1; true
	@pkill -SIGKILL dfc 2>/dev/null; true

# delete only caches, not logs
rmcache:
	@./setup/rmcache.sh

clean:
	rm -f $(GOPATH)/bin/dfc
	rm -f $(GOPATH)/pkg/linux_amd64/github.com/NVIDIA/dfcpub/dfc.a

deploy:
	@./setup/deploy.sh

deploy_next_tier:
	@DEPLOY_AS_NEXT_TIER=1 ./setup/deploy.sh

# example extracting 'numget' stats out of all local logs
numget:
	@./setup/numget.sh

# run benchmarks 10 times to generate cpu.prof
cpuprof:
	@go test -v -run=XXX -bench=. -count 10 -cpuprofile=/tmp/cpu.prof

flamegraph: cpuprof
	@go-torch dfc.test /tmp/cpu.prof -u http://localhost:6060/

code-coverage:
	@./setup/code_coverage.sh
