SHELL := /bin/bash

test: check
	coverage run --include "WDL/*" -m unittest -v
	coverage report

# fail fast
qtest:
	python -m unittest -v -f

check:
	pylint --errors-only WDL
	pyre \
		--search-path $(HOME)/.local/lib/python3.6/site-packages/lark \
		--typeshed $(HOME)/.local/lib/pyre_check/typeshed \
		--show-parse-errors check

# uses autopep8 to rewrite source files!
pretty:
	autopep8 --aggressive --aggressive --in-place WDL/*.py
	pylint -d cyclic-import,empty-docstring,missing-docstring,invalid-name --exit-zero WDL

# for use in CI: complain if source code isn't at a fixed point for autopep8
# (assumes we start from a clean checkout)
sopretty: pretty
	@git diff --quiet || (echo "ERROR: Source files were modified by autopep8; please fix up this commit with 'make pretty'"; exit 1)

# run tests in a docker image
docker:
	docker build -t miniwdl .

# push to pypi test
pypi_test: bdist
	twine upload --repository-url https://test.pypi.org/legacy/ dist/*

# push to pypi live
pypi: bdist
	echo -e "\033[0;31;5m -- Not a test: pushing $$(basename `ls -1 dist/*.tar.gz` .tar.gz) to PyPI! -- \033[0m"
	twine upload dist/*

# build dist
bdist:
	rm -rf dist/
	python3 setup.py sdist bdist_wheel

# sphinx
doc:
	$(MAKE) -C docs html

docs: doc

.PHONY: check sopretty pretty test docker doc docs pypi_test pypi bdist
