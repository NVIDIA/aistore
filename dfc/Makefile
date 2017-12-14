SHELL := /bin/bash
# The name of the executable (default is current directory name)
TARGET := $(shell echo $${PWD\#\#*/})

#$(info TARGET is $(TARGET))
.DEFAULT_GOAL: $(TARGET)

# These will be provided to the target
VERSION := 1.0.0
BUILD := `git rev-parse HEAD`

# Use linker flags to provide version/build settings to the target
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
	@test -d /tmp/nvidia && cd /tmp/nvidia; find . -name cache -type d | xargs rm -rf; cd - >/dev/null

clean: kill rmcache
	@rm -f $(TARGET)

deploy:
	@./setup/deploy.sh


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
