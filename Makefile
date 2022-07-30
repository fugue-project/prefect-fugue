.PHONY: help clean dev docs package test

help:
	@echo "The following make targets are available:"
	@echo "	 devenv		create venv and install all deps for dev env (assumes python3 cmd exists)"
	@echo "	 dev 		install all deps for dev env (assumes venv is present)"
	@echo "  docs		create pydocs for all relveant modules (assumes venv is present)"
	@echo "	 package	package for pypi"
	@echo "	 test		run all tests with coverage (assumes venv is present)"

devenv:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	pre-commit install

docs:
	rm -rf docs/api
	rm -rf docs/build
	sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api triad/
	sphinx-build -b html docs/ docs/build/

lint:
	pre-commit run --all-files

package:
	rm -rf dist/*
	python3 setup.py sdist
	python3 setup.py bdist_wheel

test:
	python3 -bb -m pytest tests/
