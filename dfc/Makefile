SHELL := /bin/bash

# Build version and flags
VERSION := 1.0.0
BUILD := `git rev-parse HEAD`
LDFLAGS=-ldflags "-X=dfc.Version=$(VERSION) -X=dfc.Build=$(BUILD)"

build:
	@go build && go install && GOBIN=$GOPATH/bin go install setup/dfc.go

kill:
	@pkill -SIGINT dfc 2>/dev/null; sleep 1; true

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
