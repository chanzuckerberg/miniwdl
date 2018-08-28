test: check
	coverage run --include "WDL/*" -m unittest discover
	coverage report

check:
	pyre \
		--search-path $(HOME)/.local/lib/python3.6/site-packages/lark \
		--typeshed $(HOME)/.local/lib/pyre_check/typeshed \
		--show-parse-errors check

.PHONY: check test
