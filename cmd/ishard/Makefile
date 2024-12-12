AISTORE_PATH = $(shell git rev-parse --show-toplevel)
SCRIPTS_DIR = $(AISTORE_PATH)/scripts

# Do not print enter/leave directory when doing 'make -C DIR <target>'
MAKEFLAGS += --no-print-directory

.PHONY: ishard mod

ishard:
	@$(MAKE) -C $(AISTORE_PATH) ishard

lint:
	@echo "Running lint cmd/ishard..."
	@golangci-lint run --config ../../.golangci.yml `go list -f {{.Dir}} ./...`

#
# go modules
#

mod: mod-clean mod-tidy

# cleanup go-mod cache
mod-clean:
	go clean --modcache

# in particular, remove unused
mod-tidy:
	go mod tidy
