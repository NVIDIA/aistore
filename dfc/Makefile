SHELL := /bin/bash
# The name of the executable (defaults to the current directory)
TARGET := $(shell echo $${PWD\#\#*/})

#$(info TARGET is $(TARGET))
.DEFAULT_GOAL: $(TARGET)

# Build version and flags
VERSION := 1.0.0
BUILD := `git rev-parse HEAD`
LDFLAGS=-ldflags "-X=dfc.Version=$(VERSION) -X=dfc.Build=$(BUILD)"

# go source files, ignore vendor directory
SRC = $(shell find . -type f -name '*.go' -not -path "./vendor/*")

.PHONY: all build clean install uninstall fmt simplify check run

all: check install

$(TARGET): $(SRC)
	@go build $(LDFLAGS) -o $(TARGET)

build: $(TARGET)
	@true

kill:
	@pkill -SIGINT dfc 2>/dev/null; sleep 1; true

# delete only caches, not logs
rmcache:
	@./setup/rmcache.sh

clean: kill rmcache
	@rm -f $(TARGET)

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

install:
	@go install $(LDFLAGS)

uninstall: clean
	@rm -f $$(which ${TARGET})

fmt:
	@gofmt -l -w $(SRC)

simplify:
	@gofmt -s -l -w $(SRC)

check:
	@test -z $(shell gofmt -l dfc.go | tee /dev/stderr) || echo "[WARN] Fix formatting issues with 'make fmt'"
	@for d in $$(go list ./... | grep -v /vendor/); do golint $${d}; done
	@go tool vet ${SRC}

run: install
	@$(TARGET)
