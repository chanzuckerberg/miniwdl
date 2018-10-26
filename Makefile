test: check
	coverage run --include "WDL/*" -m unittest -v
	coverage report

check:
	pyre \
		--search-path $(HOME)/.local/lib/python3.6/site-packages/lark \
		--typeshed $(HOME)/.local/lib/pyre_check/typeshed \
		--show-parse-errors check

docker:
	docker build -t miniwdl .

doc:
	$(MAKE) -C docs html

docs: doc

.PHONY: check test docker doc docs
