# Contributing to miniwdl

Feedback and contributions to miniwdl are welcome, via issues and pull requests on this repository.

* [Online documentation](https://miniwdl.readthedocs.io/en/latest/) includes several "codelab" tutorials to start with
* The [Project board](https://github.com/chanzuckerberg/miniwdl/projects/1) shows our current prioritization of [issues](https://github.com/chanzuckerberg/miniwdl/issues)
* [Starter issues](https://github.com/chanzuckerberg/miniwdl/issues?q=is%3Aopen+is%3Aissue+label%3Astarter) are suitable entry points for new contributors
* [Pull request template](https://github.com/chanzuckerberg/miniwdl/blob/main/.github/pull_request_template.md) includes a preparation checklist

## Development environment

To set up your Linux development environment,

1. `git clone --recursive` this repository or your fork thereof, and `cd` into it
2. Install dependencies as illustrated in the [Dockerfile](https://github.com/chanzuckerberg/miniwdl/blob/main/Dockerfile) (OS packages + `pip3 install --user -r` both `requirements.txt` and `requirements.dev.txt`)
3. Invoking user must have [permission to control Docker](https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user)
4. Try `python3 -m WDL run_self_test` to test the configuration

Generally, `python3 -m WDL ...` invokes the equivalent of the `miniwdl ...` entry point for the local source tree. Another option is to `pip3 install .` to install the `miniwdl` entry point with the current code revision.

The Makefile has a few typical flows:

- `make` or `make test` runs the full test suite with code coverage report (takes several minutes)
- `make qtest` runs most of the tests more quickly (by omitting some slower cases, and not tracking coverage)
- `make pretty` reformats the code with [black](https://github.com/python/black)
- `make check` validates the code with [Pylint](https://www.pylint.org/) and [Pyre](https://pyre-check.org/)

To quickly run only a relevant subset of the tests, you can e.g. `python3 -m unittest -f tests/test_5stdlib.py` or `python3 -m unittest -f tests.test_5stdlib.TestStdLib.test_glob`.

**macOS:** isn't preferred for miniwdl development due to some [test suite incompatibilities](https://github.com/chanzuckerberg/miniwdl/issues/145); but at least simple changes can be prototyped under macOS.

Read the Docs currently builds from the [mlin/miniwdl](https://github.com/mlin/miniwdl) fork of this repository. Locally, `make doc` generates the docs under `docs/_build/html/`.

## Releases, tags, and branches

Release version numbers follow the semantic versioning convention (vA.B.C; A=Major B=Minor C=Patch). Each release is created from a git tag of the version number, then published through GitHub, PyPI, and conda-forge.

The repository has the following principal branches:

* **main** is the trunk, with all diffs pending for the upcoming mainline release
* **release** should be set to the most up-to-date mainline release
* **vA.B.x** track older release lineages, kept in case of need to release patched versions thereof

## Security

Please disclose security issues responsibly by contacting security@chanzuckerberg.com.
