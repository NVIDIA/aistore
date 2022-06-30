
SHELL := /bin/bash
PYTHON = python3
PIP = pip3
DOCFILE := ../../docs/python_api.md

.PHONY: deps
deps:
	$(PIP) install -r requirements.dev.txt

.PHONY: pipx_deps
pipx_deps:
	$(PIP) install pipx

.PHONY: unit_test
unit_test: deps
	pytest -vv

.PHONY: lint
lint: deps
	find . -type f -name "*.py" | egrep -v "^(./.history/|./build/|./dist/|./venv/|./env/|./.venv/|./.env/)" | xargs pylint

.PHONY: generate-docs
generate-docs: pipx_deps
	pipx install pydoc-markdown
	rm -f $(DOCFILE)
	pydoc-markdown -I ./aistore/client -m api '{ renderer: { type: markdown, descriptive_class_title: false, render_toc: true, render_toc_title: "", render_module_header: false, classdef_with_decorators: true } }' > bare_reference.md 
	cat api-doc-prefix.txt bare_reference.md >> ../../docs/python_api.md 
	rm bare_reference.md
	sed -i -e 's/####/###/g' ../../docs/python_api.md 
