SHELL := /bin/bash
PYTHON_ENV?=$(HOME)/.local

test: check
	coverage run --include "WDL/*" --omit WDL/CLI.py -m unittest -v
	coverage report -m
	prove -v tests/check.t tests/cromwell.t

# fail fast
qtest:
	python3 -m unittest -v -f
	prove -v tests/check.t

check:
	pylint -j `nproc` --errors-only WDL
	pyre \
		--search-path ${PYTHON_ENV}/lib/python3.6/site-packages/lark \
		--typeshed ${PYTHON_ENV}/lib/pyre_check/typeshed \
		--show-parse-errors check

# uses black to rewrite source files!
pretty:
	black --line-length 100 --py36 WDL/
	pylint -d cyclic-import,empty-docstring,missing-docstring,invalid-name,bad-continuation --exit-zero WDL

# for use in CI: complain if source code isn't at a fixed point for black
sopretty:
	@git diff --quiet || (echo "ERROR: 'make sopretty' must start with a clean working tree"; exit 1)
	$(MAKE) pretty
	@git diff --quiet || (echo "ERROR: source files were modified by black; please fix up this commit with 'make pretty'"; exit 1)

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
