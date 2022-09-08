AISTORE_PATH = $(shell git rev-parse --show-toplevel)
SCRIPTS_DIR = $(AISTORE_PATH)/deploy/scripts

# Do not print enter/leave directory when doing 'make -C DIR <target>'
MAKEFLAGS += --no-print-directory

cli:
	@$(MAKE) -C $(AISTORE_PATH) cli

lint:
	@echo "Running lint cmd/cli..."
	@golangci-lint run --config ../../.golangci.yml `go list -f {{.Dir}} ./...`

