test: check
	coverage run --include "WDL/*" -m unittest discover
	coverage report

check:
	pyre --show-parse-errors check

.PHONY: check test
