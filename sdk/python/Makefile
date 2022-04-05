
SHELL := /bin/bash
PYTHON = python3
PIP = pip3

.PHONY: deps
deps:
	$(PIP) install -r requirements.dev.txt

.PHONY: unit_test
unit_test: deps
	pytest -vv

.PHONY: lint
lint: deps
	find . -type f -name "*.py" | egrep -v "^(./.history/|./build/|./dist/|./venv/|./env/|./.venv/|./.env/)" | xargs pylint
