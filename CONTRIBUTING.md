# Contributing to miniwdl

Feedback and contributions to miniwdl are welcome, via issues and pull requests on this repository.

* The [Project board](https://github.com/chanzuckerberg/miniwdl/projects/1) shows our current prioritization of [issues](https://github.com/chanzuckerberg/miniwdl/issues)
* [Starter issues](https://github.com/chanzuckerberg/miniwdl/issues?q=is%3Aopen+is%3Aissue+label%3Astarter) are good potential entry points for new contributors

To set up your local development environment,

1. `git clone --recursive` this repository or your fork thereof
2. Install dependencies as illustrated in the [Dockerfile](https://github.com/chanzuckerberg/miniwdl/blob/master/Dockerfile) (OS packages + PyPI packages listed in `requirements.txt` and `requirements.dev.txt`)
3. Invoking user must have [permission to control Docker](https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user)

To invoke the `miniwdl` command-line interface from your working repository, e.g. `python3 -m WDL check ...` or `python3 -m WDL run ...`. Another possibility is to `pip3 install .` to install the `miniwdl` entry point with the current code revision (but leaving it installed!).

The Makefile has a few typical flows:

- `make` or `make test` runs the full test suite with code coverage report (takes several minutes)
- `make qtest` runs most of the tests more quickly (by omitting some slower cases, and not tracking coverage)
- `make pretty` reformats the code with [black](https://github.com/python/black)
- `make check` validates the code with [Pylint](https://www.pylint.org/) and [Pyre](https://pyre-check.org/)

To quickly run only a relevant subset of the tests, you can e.g. `python3 -m unittest -f tests/test_5stdlib.py` or `python3 -m unittest -f tests.test_5stdlib.TestStdLib.test_glob`.

Miniwdl's [online documentation](https://miniwdl.readthedocs.io/en/latest/) includes several codelab tutorials to start with. The [pull request template](https://github.com/chanzuckerberg/miniwdl/blob/master/.github/pull_request_template.md) includes a checklist for preparing your PR. Thank you!

## Security

Please disclose security issues responsibly by contacting security@chanzuckerberg.com.
