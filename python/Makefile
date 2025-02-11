
SHELL := /bin/bash
PYTHON = python3
GO = go
PYTEST = $(PYTHON) -m pytest
PIP ?= pip3
SDK_DOCFILE := ../docs/python_sdk.md
TORCH_DOCFILE := ../docs/pytorch.md
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

.PHONY: s3_test_deps
s3_test_deps:
	$(PIP) install --upgrade -r tests/s3compat/requirements --quiet

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
	$(PYTEST) -v tests/integration/sdk -m "not etl and not authn"

.PHONY: python_sdk_unit_tests
python_sdk_unit_tests: common_deps python_unit_tests_scenario_gen
	$(PYTEST) -v tests/unit/sdk

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

.PHONY: lint
lint: common_deps botocore_deps
	pylint --rcfile=.pylintrc --recursive=y aistore

.PHONY: lint-tests
lint-tests: common_deps botocore_deps
	pylint --rcfile=.pylintrc-tests --recursive=y tests

.PHONY: generate-sdk-docs
generate-sdk-docs:
	@ chmod +x doc_prefixes.sh
	@ set -e
	@ [[ $$(pipx --help) ]] || $(PIP) install pipx; $(PYTHON) -m pipx ensurepath --force
	@ [[ $$(pydoc-markdown --help) ]] || pipx install --force pydoc-markdown[novella]
	@ source doc_prefixes.sh &&  echo "$$SDK_PREFIX" > $(SDK_DOCFILE)
	@ pydoc-markdown -I ./aistore/sdk \
	-m authn.authn_client -m authn.cluster_manager -m authn.role_manager -m authn.token_manager -m authn.user_manager -m authn.access_attr \
	-m bucket -m client -m cluster -m etl -m job -m multiobj.object_group -m multiobj.object_names -m multiobj.object_range -m multiobj.object_template \
	-m obj.object -m obj.object_reader -m obj.obj_file.object_file -m obj.object_props -m obj.object_attributes \
	'{ renderer: { type: markdown, descriptive_class_title: "Class: ", render_toc: true, render_toc_title: "", render_module_header: false, classdef_with_decorators: true } }' >> $(SDK_DOCFILE)	
	@ sed -i -e 's/####/###/g' $(SDK_DOCFILE)

.PHONY: generate-pytorch-docs
generate-pytorch-docs:
	@ chmod +x doc_prefixes.sh
	@ set -e
	@ [[ $$(pipx --help) ]] || $(PIP) install pipx; $(PYTHON) -m pipx ensurepath --force
	@ [[ $$(pydoc-markdown --help) ]] || pipx install --force pydoc-markdown[novella]
	@ source doc_prefixes.sh && echo "$$TORCH_PREFIX" > $(TORCH_DOCFILE)
	@ pydoc-markdown -I ./aistore/pytorch -m base_map_dataset -m base_iter_dataset -m map_dataset -m iter_dataset -m shard_reader -m multishard_dataset '{ renderer: { type: markdown, descriptive_class_title: "Class: ", render_toc: true, render_toc_title: "", render_module_header: false, classdef_with_decorators: true } }' >> $(TORCH_DOCFILE)
	@ sed -i -e 's/####/###/g' $(TORCH_DOCFILE)


.PHONY:
test-pyaisloader:
	@./pyaisloader/ci-test.sh $(PYAISLOADER_TEST_TYPE)
