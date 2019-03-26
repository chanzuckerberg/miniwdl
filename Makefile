SHELL := /bin/bash
PYTHON_PKG_BASE?=$(HOME)/.local

test: check check_check
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
		--search-path stubs \
		--typeshed ${PYTHON_PKG_BASE}/lib/pyre_check/typeshed \
		--show-parse-errors check

check_check:
	# regression test against pyre doing nothing (issue #100)
	echo "check_check: str = 42" > WDL/DELETEME_check_check.py
	$(MAKE) check > /dev/null 2>&1 && exit 1 || exit 0
	rm WDL/DELETEME_check_check.py

# uses black to rewrite source files!
pretty:
	black --line-length 100 --target-version py36 WDL/
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

.PHONY: check check_check sopretty pretty test qtest docker doc docs pypi_test pypi bdist
