AISTORE_PATH=$(shell git rev-parse --show-toplevel)

# Do not print enter/leave directory when doing 'make -C DIR <target>'
MAKEFLAGS += --no-print-directory

cli:
	@$(MAKE) -C $(AISTORE_PATH) cli

