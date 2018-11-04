test: typecheck
	coverage run --include "WDL/*" -m unittest -v
	coverage report

typecheck:
	pyre \
		--search-path $(HOME)/.local/lib/python3.6/site-packages/lark \
		--typeshed $(HOME)/.local/lib/pyre_check/typeshed \
		--show-parse-errors check

docker:
	docker build -t miniwdl .

pypi_test: bdist
	twine upload --repository-url https://test.pypi.org/legacy/ dist/*

bdist:
	rm -rf dist/
	python3 setup.py sdist bdist_wheel

doc:
	$(MAKE) -C docs html

docs: doc

.PHONY: typecheck test docker doc docs pypi_test bdist
