AISTORE_PATH = $(shell git rev-parse --show-toplevel)
SCRIPTS_DIR = $(AISTORE_PATH)/scripts

# Do not print enter/leave directory when doing 'make -C DIR <target>'
MAKEFLAGS += --no-print-directory

.PHONY: cli mod

cli:
	@$(MAKE) -C $(AISTORE_PATH) cli

lint:
	@echo "Running lint cmd/cli..."
	@golangci-lint run $(if $(TAGS),--build-tags "$(TAGS)",) --max-issues-per-linter=0 --config ../../.golangci.yml ./...

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
