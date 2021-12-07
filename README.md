# miniwdl
**[Workflow Description Language](http://openwdl.org/) local runner & developer toolkit for Python 3.6+**

![Project Status](https://img.shields.io/badge/status-stable-green.svg)
[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://github.com/chanzuckerberg/miniwdl/blob/main/LICENSE)
[![CI](https://github.com/chanzuckerberg/miniwdl/workflows/CI/badge.svg?branch=main)](https://github.com/chanzuckerberg/miniwdl/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/chanzuckerberg/miniwdl/badge.svg?branch=main)](https://coveralls.io/github/chanzuckerberg/miniwdl?branch=main)
[![Docs Status](https://readthedocs.org/projects/miniwdl/badge/?version=latest)](https://miniwdl.readthedocs.io/en/latest/)

## Install miniwdl

Installation requires Python 3.6+, pip3 (or conda) and Docker. Linux preferred; [macOS compatible with extra steps](https://github.com/chanzuckerberg/miniwdl/issues/145). More detail in [full documentation](https://miniwdl.readthedocs.io/en/latest/getting_started.html).

- Intall with pip [![PyPI version](https://img.shields.io/pypi/v/miniwdl.svg)](https://pypi.org/project/miniwdl/) : run `pip3 install miniwdl`
- Install with conda [![Anaconda-Server Badge](https://anaconda.org/conda-forge/miniwdl/badges/version.svg)](https://anaconda.org/conda-forge/miniwdl) : run `conda install -c conda-forge miniwdl`
- Verify your miniwdl installation: `miniwdl run_self_test`
- Install from source code: see the [Dockerfile](https://github.com/chanzuckerberg/miniwdl/blob/main/Dockerfile) for dependencies to run `setup.py`

<img src="https://github.com/openwdl/learn-wdl/blob/master/images/miniwdl-dev.png" width=600>

## Use miniwdl

Run an example bioinformatics WDL pipeline using miniwdl, or learn more abut miniwdl via a short course (screencast examples).  If you are new to the WDL language, see the open source `learn-wdl` course.  

- [Run an example using a viral genome assembly workflow](https://miniwdl.readthedocs.io/en/latest/getting_started.html)
- [Learn miniwdl course w/screencasts](https://github.com/openwdl/learn-wdl/tree/master/6_miniwdl_course) - shown below
- [Learn WDL course w/screencasts](https://github.com/openwdl/learn-wdl)

[<img src="https://github.com/openwdl/learn-wdl/blob/master/images/miniwdl-screencasts.png" width=800>](https://www.youtube.com/playlist?list=PL4Q4HssKcxYv1FQJUD6D1Cu0Q1O-_S1hM)

## [Documentation](https://miniwdl.readthedocs.io/en/latest/WDL.html)

The online documentation includes a user tutorial, reference manual, and Python development codelabs: [![Docs Status](https://readthedocs.org/projects/miniwdl/badge/?version=latest)](https://miniwdl.readthedocs.io/en/latest/WDL.html)

See the [Releases](https://github.com/chanzuckerberg/miniwdl/releases) for change logs. The [Project board](https://github.com/chanzuckerberg/miniwdl/projects/1) shows the current prioritization of [issues](https://github.com/chanzuckerberg/miniwdl/issues).

## Getting Help

* [Open an issue](https://github.com/chanzuckerberg/miniwdl/issues)
* [OpenWDL Slack](https://openwdl.slack.com/) (#miniwdl channel)
* [Bioinformatics Stack Exchange](https://bioinformatics.stackexchange.com/questions/tagged/wdl)

## [Contributing](https://github.com/chanzuckerberg/miniwdl/blob/main/CONTRIBUTING.md)

Feedback and contributions to miniwdl are welcome, via issues and pull requests on this repository. See [CONTRIBUTING.md](https://github.com/chanzuckerberg/miniwdl/blob/main/CONTRIBUTING.md) for guidelines and instructions to set up your development environment.

## Security

Please disclose security issues responsibly by contacting security@chanzuckerberg.com.
