SHELL := /bin/bash
export TMPDIR = /tmp

test: check_check check unit_tests integration_tests

# fail fast
qtest:
	python3 tests/no_docker_services.py
	pytest -vx --tb=short -n `python3 -c 'import multiprocessing as mp; print(mp.cpu_count())'` --dist=loadscope tests
	python3 -m unittest tests.test_cli_argcomplete
	prove -v tests/{check,runner}.t
	python3 tests/no_docker_services.py

unit_tests:
	pytest -v --tb=short -n `python3 -c 'import multiprocessing as mp; print(mp.cpu_count())'` --dist=loadscope --cov=WDL tests
	python3 -m unittest tests.test_cli_argcomplete
	python3 tests/no_docker_services.py

integration_tests:
	prove -v tests/{check,runner}.t
	python3 tests/no_docker_services.py

skylab_bulk_rna:
	prove -v tests/applied/skylab_bulk_rna.t

DVGLx:
	prove -v tests/applied/DVGLx.t

viral_assemble:
	prove -v tests/applied/viral_assemble.t

viral_refbased:
	prove -v tests/applied/viral_refbased.t

singularity_tests:
	MINIWDL__SCHEDULER__CONTAINER_BACKEND=singularity \
	sh -c 'python3 -m WDL run_self_test && prove -v tests/applied/viral_assemble.t'

ci_housekeeping: sopretty check_check check doc

ci_unit_tests: unit_tests

check:
	pyre \
		--search-path stubs \
		--typeshed `python3 -c 'import sys, site, os; print(next(p for p in (os.path.join(dir,"lib/pyre_check/typeshed") for dir in (sys.prefix,site.getuserbase())) if os.path.isdir(p)))'` \
		--show-parse-errors check
	pylint -j `python3 -c 'import multiprocessing as mp; print(mp.cpu_count())'` --errors-only WDL
	flake8 WDL

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

# build docker image with current source tree, poised to run tests e.g.:
#   docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp miniwdl
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

.PHONY: check check_check sopretty pretty test qtest docker doc docs pypi_test pypi bdist ci_housekeeping unit_tests integration_tests skylab_bulk_rna DVGLx viral_assemble
