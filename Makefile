SHELL := /bin/bash

test: typecheck
	coverage run --include "WDL/*" -m unittest -v
	coverage report

qtest:
	python -m unittest -v -f

typecheck:
	pyre \
		--search-path $(HOME)/.local/lib/python3.6/site-packages/lark \
		--typeshed $(HOME)/.local/lib/pyre_check/typeshed \
		--show-parse-errors check

docker:
	docker build -t miniwdl .

pypi_test: bdist
	twine upload --repository-url https://test.pypi.org/legacy/ dist/*

pypi: bdist
	echo -e "\033[0;31;5m -- Not a test: pushing $$(basename `ls -1 dist/*.tar.gz` .tar.gz) to PyPI! -- \033[0m"
	twine upload dist/*

bdist:
	rm -rf dist/
	python3 setup.py sdist bdist_wheel

doc:
	$(MAKE) -C docs html

docs: doc

.PHONY: typecheck test docker doc docs pypi_test pypi bdist
