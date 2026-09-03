SHELL := /bin/bash
PYTHON = python3
GO = go
PYTEST = $(PYTHON) -m pytest
PIP ?= pip3
PYAISLOADER_TEST_TYPE ?= short

BOTO_UNIT_TEST_COUNT := $(shell ls -1 tests/unit/botocore_patch/test*py | wc -l)
export AWS_SESSION_TOKEN := testing
# will be fetched from CI pipeline secrets
# export AWS_REGION := us-east-1
# export AWS_SECRET_ACCESS_KEY := testing
# export AWS_ACCESS_KEY_ID := testing
export AWS_SECURITY_TOKEN := testing
BOTO_VERSION_ARGS := ""
ifdef BOTO3_VERSION
	BOTO_VERSION_ARGS := " boto3==$(BOTO3_VERSION) "
endif
ifdef BOTOCORE_VERSION
	BOTO_VERSION_ARGS := " $(BOTO_VERSION_ARGS) botocore==$(BOTOCORE_VERSION) "
endif
BOTO_VERSION_ARGS := $(patsubst "%",%,$(BOTO_VERSION_ARGS))

.PHONY: common_deps
common_deps:
	$(PIP) install -r aistore/common_requirements --quiet

.PHONY: dev_deps
dev_deps:
	$(PIP) install -r aistore/pytorch/dev_requirements --quiet

.PHONY: botocore_deps
botocore_deps:
	$(PIP) install --upgrade -r aistore/botocore_patch/botocore_requirements $(BOTO_VERSION_ARGS) --quiet

# pylint/astroid are only needed for linting, so they're kept out of
# common_requirements - common_deps runs under every supported Python version
# across the test matrix and shouldn't pay for lint-only dependencies.
.PHONY: lint_deps
lint_deps:
	$(PIP) install -r aistore/lint_requirements --quiet

.PHONY: s3_test_deps
s3_test_deps:
	$(PIP) install --upgrade -r tests/s3compat/requirements.txt --quiet

.PHONY: python_unit_tests_scenario_gen
python_unit_tests_scenario_gen:
	@ $(GO) run tests/unit/sdk/hrw/scenario_gen.go

.PHONY: python_tests
python_tests: common_deps dev_deps botocore_deps python_sdk_tests python_etl_tests python_botocore_tests

# Tests for aistore.sdk
.PHONY: python_sdk_tests
python_sdk_tests: common_deps python_sdk_unit_tests python_sdk_integration_tests

.PHONY: python_sdk_integration_tests
python_sdk_integration_tests: common_deps
	$(PYTEST) -v -n auto tests/integration/sdk -m "not etl and not authn and not extended and not nonparallel" --durations=20
	# Run tests that are specifically not able to run alongside certain others
	$(PYTEST) -v tests/integration/sdk -m "nonparallel" --durations=0

.PHONY: python_sdk_unit_tests
python_sdk_unit_tests: common_deps
	$(PYTEST) -v -n auto tests/unit/sdk

.PHONY: mcp_deps
mcp_deps:
	$(PIP) install "mcp[cli]>=1.4.1,<2" --quiet

.PHONY: python_mcp_unit_tests
python_mcp_unit_tests: common_deps mcp_deps
	$(PYTEST) -v tests/unit/mcp

# Run tests with `extended` marker with `AIS_STRESS_TEST` to control increased object count, size, etc.
.PHONY: python_sdk_extended_tests
python_sdk_extended_tests: common_deps
	AIS_STRESS_TEST=true $(PYTEST) -v tests/integration/sdk -m "extended" --durations=0

.PHONY: python_etl_tests
python_etl_tests: common_deps
	$(PYTEST) -v -s tests/integration/sdk/ -m etl

.PHONY: python_authn_tests
python_authn_tests: common_deps
	$(PYTEST) -v -s tests/integration/sdk/ -m authn

.PHONY: python_s3_compat_test
python_s3_compat_test: s3_test_deps
	python3 tests/s3compat/run_tests.py

# Tests for aistore.botocore_patch
.PHONY: python_botocore_tests
python_botocore_tests: common_deps botocore_deps python_botocore_unit_tests python_botocore_integration_tests

.PHONY: python_botocore_unit_tests
python_botocore_unit_tests: common_deps botocore_deps
	$(PYTEST) -v -n $(BOTO_UNIT_TEST_COUNT) --dist loadfile  tests/unit/botocore_patch

.PHONY: python_botocore_integration_tests
python_botocore_integration_tests: common_deps botocore_deps
	$(PYTEST) -v tests/integration/botocore_patch tests/integration/boto3

# Tests for aistore.pytorch
.PHONY: python_pytorch_tests
python_pytorch_tests: common_deps dev_deps python_pytorch_unit_tests python_pytorch_integration_tests

.PHONY: python_pytorch_unit_tests
python_pytorch_unit_tests: common_deps dev_deps
	$(PYTEST) -v tests/unit/pytorch

.PHONY: python_pytorch_integration_tests
python_pytorch_integration_tests: common_deps dev_deps
	$(PYTEST) -v tests/integration/pytorch

# Optional override for pylint's `jobs` setting (see .pylintrc / .pylintrc-tests
# for why auto-detect via jobs=0 can misbehave on some CI runners). Leave unset
# to use the rcfile's own auto-detection.
PYLINT_JOBS ?=
PYLINT_JOBS_FLAG = $(if $(PYLINT_JOBS),--jobs=$(PYLINT_JOBS),)

.PHONY: lint
lint: common_deps botocore_deps mcp_deps lint_deps
	pylint --rcfile=.pylintrc --recursive=y $(PYLINT_JOBS_FLAG) aistore

.PHONY: lint-tests
lint-tests: common_deps botocore_deps lint_deps
	pylint --rcfile=.pylintrc-tests --recursive=y $(PYLINT_JOBS_FLAG) tests

# Coverage targets (unit, integration, combined)
COV_PKG := aistore
COV_CONFIG := .coveragerc
COV_ARGS := --cov=$(COV_PKG) --cov-config=$(COV_CONFIG) --cov-report=term

.PHONY: python_sdk_unit_coverage
python_sdk_unit_coverage: common_deps
	@echo "==> Running Python SDK UNIT tests with coverage"
	@rm -f .coverage.unit
	@COVERAGE_FILE=.coverage.unit $(PYTEST) -v -n auto tests/unit/sdk \
		$(COV_ARGS) \
		--cov-report=xml:coverage.unit.xml \
		--cov-report=html:htmlcov_unit

.PHONY: python_sdk_integration_coverage
python_sdk_integration_coverage: common_deps dev_deps botocore_deps
	@echo "==> Running Python SDK INTEGRATION tests with coverage"
	@rm -f .coverage.int
	@# Parallel-friendly subset
	@COVERAGE_FILE=.coverage.int $(PYTEST) -v -n auto \
		tests/integration/sdk \
		tests/integration/pytorch \
		tests/integration/botocore_patch tests/integration/boto3 \
		-m "not etl and not authn and not extended and not nonparallel" \
		--durations=20 \
		$(COV_ARGS) --cov-append
	@# Serial subset (non-parallel)
	@COVERAGE_FILE=.coverage.int $(PYTEST) -v tests/integration/sdk \
		-m "nonparallel" \
		--durations=0 \
		$(COV_ARGS) --cov-append \
		--cov-report=xml:coverage.int.xml \
		--cov-report=html:htmlcov_int

.PHONY: python_sdk_coverage
python_sdk_coverage: python_sdk_unit_coverage python_sdk_integration_coverage
	@echo "==> Combining unit & integration coverage data"
	@coverage combine .coverage.unit .coverage.int
	@coverage html -d htmlcov
	@coverage xml -o coverage.xml
	@echo "Overall SDK coverage report: python/htmlcov/index.html