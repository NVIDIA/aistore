
SHELL := /bin/bash
PYTHON = python3
PIP = pip3
DOCFILE := ../../docs/python_api.md

.PHONY: deps
deps:
	$(PIP) install -r requirements.dev.txt

.PHONY: python_tests
python_tests: deps
	pytest -v tests --ignore=tests/test_etl_ops.py

.PHONY: python_etl_tests
python_etl_tests: deps
	pytest -v tests/test_etl_ops.py


.PHONY: lint
lint: deps
	find . -type f -name "*.py" | egrep -v "^(./.history/|./build/|./dist/|./venv/|./env/|./.venv/|./.env/)" | xargs pylint

.PHONY: generate-docs
generate-docs: 
	@ set -e
	@ [[ $$(pipx --help) ]] || $(PIP) install pipx; $(PYTHON) -m pipx ensurepath --force
	@ [[ $$(pydoc-markdown --help) ]] || pipx install --force pydoc-markdown[novella]
	@ { cat api-doc-prefix.txt; pydoc-markdown -I ./aistore/client -m api -m cluster -m bucket -m object '{ renderer: { type: markdown, descriptive_class_title: "Class: ", render_toc: true, render_toc_title: "", render_module_header: false, classdef_with_decorators: true } }'; } > $(DOCFILE)
	@ sed -i -e 's/####/###/g' $(DOCFILE)
