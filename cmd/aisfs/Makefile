AISTORE_PATH=$(shell git rev-parse --show-toplevel)

# Do not print enter/leave directory when doing 'make -C DIR <target>'
MAKEFLAGS += --no-print-directory

aisfs:
	@$(MAKE) -C $(AISTORE_PATH) aisfs

lint:
	@echo "Running lint cmd/aisfs..."
	@golangci-lint run --config ../../.golangci.yml `go list -f {{.Dir}} ./...`

#
# go modules
#
.PHONY: mod mod-clean mod-tidy

mod: mod-clean mod-tidy

# cleanup go-mod cache
mod-clean:
	go clean --modcache

# in particular, remove unused
mod-tidy:
	go mod tidy

