SHELL := /bin/bash

# Build version and flags
VERSION := 1.0.0
BUILD := `git rev-parse HEAD`
LDFLAGS=-ldflags "-X=dfc.Version=$(VERSION) -X=dfc.Build=$(BUILD)"

REPOPATH=src/github.com/NVIDIA
DEVPATH=src/gitlab-master.nvidia.com/aistorage/

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

devrefresh:
	@if [ -d "${GOPATH}/${REPOPATH}" ] && [ -L "${GOPATH}/${REPOPATH}" ]; then \
		echo "Refreshing symbolic link ${GOPATH}/${REPOPATH}" && \
		rm -r "${GOPATH}/${REPOPATH}"; \
	fi
	@if [ ! -d "${GOPATH}/${REPOPATH}" ]; then \
		echo "Creating symbolic link" && \
		ln -s "${GOPATH}/${DEVPATH}" "${GOPATH}/${REPOPATH}"; \
	else echo "Failed to create symlink: a regular direct exists"; \
	fi

build:
	GOBIN=${GOPATH}/bin go build && go install
	GOBIN=${GOPATH}/bin go install setup/dfc.go

kill:
	@pkill -SIGINT dfc 2>/dev/null; sleep 1; true
	@pkill authn 2>/dev/null; sleep 1; true

# delete only caches, not logs
rmcache:
	@./setup/rmcache.sh

clean:
	rm -f $(GOPATH)/bin/dfc
	rm -f $(GOPATH)/pkg/linux_amd64/github.com/NVIDIA/dfcpub/dfc.a

deploy:
	@./setup/deploy.sh

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
