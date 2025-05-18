SHELL := /bin/bash
export TMPDIR = /tmp

test: check_check check unit_tests integration_tests

# fail fast
qtest:
	python3 tests/no_docker_services.py
	pytest -vx --tb=short -n auto --dist=loadscope tests
	python3 -m unittest tests.test_cli_argcomplete
	prove -v tests/{check,eval,runner,zip}.t
	python3 tests/no_docker_services.py

unit_tests:
	pytest -v --tb=short -n auto --dist=loadscope --cov=WDL tests
	python3 -m unittest tests.test_cli_argcomplete
	python3 tests/no_docker_services.py

spec_tests:
	pytest -n auto --tb=short tests/spec_tests/spec_tests.py

integration_tests:
	prove -v tests/{check,eval,runner,zip,multi_line_strings}.t
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

ci_housekeeping: check_check check doc

ci_unit_tests: unit_tests spec_tests

check:
	ruff check --ignore E741 WDL
	mypy WDL
	ruff format --check --line-length 100 WDL

check_check:
	# regression test against pyre/mypy doing nothing (issue #100)
	echo "check_check: str = 42" > WDL/DELETEME_check_check.py
	$(MAKE) check > /dev/null 2>&1 && exit 1 || exit 0
	rm WDL/DELETEME_check_check.py

pretty:
	ruff format --line-length 100 WDL

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
	python -m build

# sphinx
doc:
	$(MAKE) -C docs html

docs: doc

.PHONY: check check_check pretty test qtest docker doc docs pypi_test pypi bdist ci_housekeeping unit_tests integration_tests skylab_bulk_rna DVGLx viral_assemble
