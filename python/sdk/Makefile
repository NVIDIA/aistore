
SHELL := /bin/bash
PYTHON = python3
PIP = pip3
DOCFILE := ../../docs/python_api.md
define PREFIX =
---
layout: post
title: PYTHON API
permalink: /docs/python-api
redirect_from:
 - /python_api.md/
 - /docs/python_api.md/
---

AIStore Python API is a growing set of client-side objects and methods to access and utilize AIS clusters.

> For PyTorch integration and usage examples, please refer to [AIS Python SDK](https://pypi.org/project/aistore) available via Python Package Index (PyPI), or see [https://github.com/NVIDIA/aistore/tree/master/python/sdk](https://github.com/NVIDIA/aistore/tree/master/python/sdk).

endef
export PREFIX
BOTO_UNIT_TEST_COUNT := $(shell ls -1 tests/monkey/unit/test*py | wc -l)
export AWS_SESSION_TOKEN := testing
export AWS_DEFAULT_REGION := us-east-1
export AWS_SECRET_ACCESS_KEY := testing
export AWS_ACCESS_KEY_ID := testing
export AWS_SECURITY_TOKEN := testing

.PHONY: all_deps
all_deps: common_deps etl_deps dev_deps

.PHONY: common_deps
common_deps:
	$(PIP) install -r common_requirements.dev.txt

.PHONY: etl_deps
etl_deps:
	$(PIP) install -r etl_requirements.dev.txt

.PHONY: dev_deps
dev_deps:
	$(PIP) install -r dev_requirements.dev.txt

.PHONY: botocore_deps
botocore_deps:
	$(PIP) install -r botocore_requirements.dev.txt

.PHONY: python_tests
python_tests: common_deps dev_deps
	pytest -v tests/integration --ignore=tests/integration/test_etl_ops.py

.PHONY: python_etl_tests
python_etl_tests: common_deps 
	pytest -v -s tests/integration/test_etl_ops.py

.PHONY: python_botocore_tests
python_botocore_tests: common_deps botocore_deps python_botocore_unit_tests python_botocore_integration_tests

.PHONY: python_unit_tests
python_unit_tests: common_deps
	pytest -v tests/unit

.PHONY: python_botocore_unit_tests
python_botocore_unit_tests: common_deps botocore_deps
	pytest -v -n $(BOTO_UNIT_TEST_COUNT) --dist loadfile  tests/monkey/unit

.PHONY: python_botocore_integration_tests
python_botocore_integration_tests: common_deps botocore_deps
	pytest -v tests/monkey/integration

.PHONY: lint
lint: common_deps
	find . -type f -name "*.py" | egrep -v "^(./.history/|./build/|./dist/|./venv/|./env/|./.venv/|./.env/)" | xargs pylint

.PHONY: generate-docs
generate-docs: 
	@ set -e
	@ [[ $$(pipx --help) ]] || $(PIP) install pipx; $(PYTHON) -m pipx ensurepath --force
	@ [[ $$(pydoc-markdown --help) ]] || pipx install --force pydoc-markdown[novella]
	@ echo "$$PREFIX" > $(DOCFILE)
	@ pydoc-markdown -I ./aistore/client -m api -m cluster -m bucket -m object -m etl '{ renderer: { type: markdown, descriptive_class_title: "Class: ", render_toc: true, render_toc_title: "", render_module_header: false, classdef_with_decorators: true } }' >> $(DOCFILE)
	@ sed -i -e 's/####/###/g' $(DOCFILE)
