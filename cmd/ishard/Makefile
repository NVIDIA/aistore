AISTORE_PATH = $(shell git rev-parse --show-toplevel)

# Do not print enter/leave directory when doing 'make -C DIR <target>'
MAKEFLAGS += --no-print-directory

# ensure lint always includes dsort tag (needed for ext/dsort/shard import)
comma := ,
empty :=
space := $(empty) $(empty)

# token-aware contains check: TAGS is assumed comma-separated (like golangci-lint expects)
has_dsort := $(filter dsort,$(subst $(comma),$(space),$(TAGS)))

ifeq ($(strip $(TAGS)),)
  ISHARD_TAGS := dsort
  WARN_DSORT  := 1
else ifneq ($(has_dsort),)
  ISHARD_TAGS := $(TAGS)
else
  ISHARD_TAGS := $(TAGS)$(comma)dsort
  WARN_DSORT  := 1
endif

.PHONY: ishard lint mod mod-clean mod-tidy

ishard:
	@$(MAKE) -C $(AISTORE_PATH) ishard

## NOTE:
## - requires dsort build tag (auto-appended for lint if missing)
## - alternatively, split imported ext/dsort/shard/* structures into api.go and api_impl.go
lint:
	@echo "Running lint cmd/ishard..."
ifneq ($(WARN_DSORT),)
	@echo "warning: cmd/ishard lint: appending missing build tag 'dsort' (TAGS='$(TAGS)' => '$(ISHARD_TAGS)')"
endif
	@golangci-lint run --build-tags "$(ISHARD_TAGS)" --max-issues-per-linter=0 --config ../../.golangci.yml ./...

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
