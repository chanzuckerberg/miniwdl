# miniwdl
**[Workflow Description Language](http://openwdl.org/) local runner & developer toolkit for Python 3.8+**

![Project Status](https://img.shields.io/badge/status-stable-green.svg)
[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://github.com/chanzuckerberg/miniwdl/blob/main/LICENSE)
[![CI](https://github.com/chanzuckerberg/miniwdl/workflows/CI/badge.svg?branch=main)](https://github.com/chanzuckerberg/miniwdl/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/chanzuckerberg/miniwdl/badge.svg?branch=main)](https://coveralls.io/github/chanzuckerberg/miniwdl?branch=main)
[![Docs Status](https://readthedocs.org/projects/miniwdl/badge/?version=latest)](https://miniwdl.readthedocs.io/en/latest/)

## Install miniwdl

Installation requires Python 3.8+, pip (or conda) and Docker (or Podman/Singularity/udocker). Linux preferred; [macOS compatible with extra steps](https://github.com/chanzuckerberg/miniwdl/issues/145). More detail in [full documentation](https://miniwdl.readthedocs.io/en/latest/getting_started.html).

- Install with pip [![PyPI version](https://img.shields.io/pypi/v/miniwdl.svg)](https://pypi.org/project/miniwdl/) : run `pip3 install miniwdl`
- Install with conda [![Anaconda-Server Badge](https://anaconda.org/conda-forge/miniwdl/badges/version.svg)](https://anaconda.org/conda-forge/miniwdl) : [setup conda-forge](https://conda-forge.org/docs/user/introduction/#how-can-i-install-packages-from-conda-forge) and run `conda install miniwdl`
- Verify your miniwdl installation: `miniwdl run_self_test`
- Install from source code for development: see [CONTRIBUTING.md](CONTRIBUTING.md)

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

## Scaling up

The miniwdl runner schedules WDL tasks in parallel up to the CPUs & memory available on the local host; so a more-powerful host enables larger workloads. Separately-maintained projects can distribute tasks to cloud & HPC backends with a shared filesystem:

* AWS:
  * [miniwdl-omics-run](https://github.com/miniwdl-ext/miniwdl-omics-run) tool for the [Amazon Omics](https://aws.amazon.com/omics/) workflow service
  * [AWS Batch plugin](https://github.com/miniwdl-ext/miniwdl-aws) (DIY)
* [SLURM](https://github.com/miniwdl-ext/miniwdl-slurm)

## Getting Help

* [Open an issue](https://github.com/chanzuckerberg/miniwdl/issues)
* [OpenWDL Slack](https://openwdl.slack.com/) (#miniwdl channel)
* [Bioinformatics Stack Exchange](https://bioinformatics.stackexchange.com/questions/tagged/wdl)

## [Contributing](https://github.com/chanzuckerberg/miniwdl/blob/main/CONTRIBUTING.md)

Feedback and contributions to miniwdl are welcome, via issues and pull requests on this repository. See [CONTRIBUTING.md](https://github.com/chanzuckerberg/miniwdl/blob/main/CONTRIBUTING.md) for guidelines, instructions to set up your development environment, and a codebase overview.

## Security

Please disclose security issues responsibly by contacting security@chanzuckerberg.com.
