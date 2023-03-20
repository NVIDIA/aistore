
SHELL := /bin/bash
PYTHON = python3
PIP = pip3
DOCFILE := ../docs/python_sdk.md
define PREFIX =
---
layout: post
title: PYTHON SDK
permalink: /docs/python-sdk
redirect_from:
 - /python_sdk.md/
 - /docs/python_sdk.md/
---

AIStore Python SDK is a growing set of client-side objects and methods to access and utilize AIS clusters.

> For PyTorch integration and usage examples, please refer to [AIS Python SDK](https://pypi.org/project/aistore) available via Python Package Index (PyPI), or see [https://github.com/NVIDIA/aistore/tree/master/python/aistore](https://github.com/NVIDIA/aistore/tree/master/python/aistore).

endef
export PREFIX
BOTO_UNIT_TEST_COUNT := $(shell ls -1 tests/unit/botocore_patch/test*py | wc -l)
export AWS_SESSION_TOKEN := testing
export AWS_DEFAULT_REGION := us-east-1
export AWS_SECRET_ACCESS_KEY := testing
export AWS_ACCESS_KEY_ID := testing
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

.PHONY: python_tests
python_tests: common_deps dev_deps botocore_deps python_sdk_tests python_etl_tests python_botocore_tests

# Tests for aistore.sdk
.PHONY: python_sdk_tests
python_sdk_tests: common_deps python_sdk_unit_tests python_sdk_integration_tests

.PHONY: python_sdk_integration_tests
python_sdk_integration_tests: common_deps
	pytest -v tests/integration/sdk -m "not etl"

.PHONY: python_sdk_unit_tests
python_sdk_unit_tests: common_deps
	pytest -v tests/unit/sdk

.PHONY: python_etl_tests
python_etl_tests: common_deps
	pytest -v -s tests/integration/sdk/ -m etl

# Tests for aistore.botocore_patch
.PHONY: python_botocore_tests
python_botocore_tests: common_deps botocore_deps python_botocore_unit_tests python_botocore_integration_tests

.PHONY: python_botocore_unit_tests
python_botocore_unit_tests: common_deps botocore_deps
	pytest -v -n $(BOTO_UNIT_TEST_COUNT) --dist loadfile  tests/unit/botocore_patch

.PHONY: python_botocore_integration_tests
python_botocore_integration_tests: common_deps botocore_deps
	pytest -v tests/integration/botocore_patch tests/integration/boto3

.PHONY: lint
lint: common_deps
	pylint --rcfile=.pylintrc --recursive=y aistore

.PHONY: lint-tests
lint-tests: common_deps botocore_deps
	pylint --rcfile=.pylintrc-tests --recursive=y tests

.PHONY: generate-docs
generate-docs:
	@ set -e
	@ [[ $$(pipx --help) ]] || $(PIP) install pipx; $(PYTHON) -m pipx ensurepath --force
	@ [[ $$(pydoc-markdown --help) ]] || pipx install --force pydoc-markdown[novella]
	@ echo "$$PREFIX" > $(DOCFILE)
	@ pydoc-markdown -I ./aistore/sdk -m client -m cluster -m bucket -m object -m multiobj.object_group -m multiobj.object_names -m multiobj.object_range -m multiobj.object_template -m job -m object_reader -m object_iterator -m etl '{ renderer: { type: markdown, descriptive_class_title: "Class: ", render_toc: true, render_toc_title: "", render_module_header: false, classdef_with_decorators: true } }' >> $(DOCFILE)
	@ sed -i -e 's/####/###/g' $(DOCFILE)
