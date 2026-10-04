# SAGUIN_BROKER names the broker the suite drives. Until saguin publishes
# a release binary the only one there is is the one you built: `make build`
# in a saguin checkout writes ./bin/saguin.
PYTHON ?= .venv/bin/python

help:
	@echo "install  create .venv and install the library and its test dependencies"
	@echo "test     the suite, against the broker named by SAGUIN_BROKER"
	@echo "wheel    build a wheel into dist/"
	@echo "clean    remove .venv, dist/ and the caches"

install:
	python3 -m venv .venv
	$(PYTHON) -m pip install --quiet --upgrade pip setuptools
	$(PYTHON) -m pip install --quiet -e ".[test]"

test:
	$(PYTHON) -m pytest -q

wheel:
	$(PYTHON) -m pip wheel . --no-deps --wheel-dir dist

clean:
	rm -rf .venv dist build .pytest_cache src/*.egg-info
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

.PHONY: help install test wheel clean
