# miniwdl
**[Workflow Description Language](http://openwdl.org/) toolkit for Python 3.6+**

![Project Status](https://img.shields.io/badge/status-alpha-red.svg)
[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://github.com/chanzuckerberg/miniwdl/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Build Status](https://travis-ci.org/chanzuckerberg/miniwdl.svg?branch=master)](https://travis-ci.org/chanzuckerberg/miniwdl) [![Coverage Status](https://coveralls.io/repos/github/chanzuckerberg/miniwdl/badge.svg?branch=master)](https://coveralls.io/github/chanzuckerberg/miniwdl?branch=master)
[![Docs Status](https://readthedocs.org/projects/miniwdl/badge/?version=latest)](https://miniwdl.readthedocs.io/en/latest/)

*miniwdl* provides WDL developer productivity tools, a local runtime implementation, and a foundation for language experimentation. See the [Releases](https://github.com/chanzuckerberg/miniwdl/releases) for change logs. The [Project board](https://github.com/chanzuckerberg/miniwdl/projects/1) reflects the near-term roadmap. This project in alpha development, and interfaces are still liable to change somewhat.

<!-- TOC generator tool: https://magnetikonline.github.io/markdown-toc-generate/ -->
- [Installation](#installation)
- [Command-line tools](#command-line-tools)
  - [miniwdl check](#miniwdl-check)
  - [miniwdl cromwell](#miniwdl-cromwell)
  - [miniwdl run](#miniwdl-run)
- [WDL Python library](#wdl-python-library)
  - [API documentation](#api-documentation)
- [Contributing](#contributing)
- [Security](#security)

## Installation

[![PyPI version](https://img.shields.io/pypi/v/miniwdl.svg)](https://pypi.org/project/miniwdl/) `pip3 install miniwdl`

[![Anaconda-Server Badge](https://anaconda.org/conda-forge/miniwdl/badges/version.svg)](https://anaconda.org/conda-forge/miniwdl)  [configure conda-forge](http://conda-forge.org/docs/user/introduction.html) and `conda install miniwdl` 

locally: see the [Dockerfile](https://github.com/chanzuckerberg/miniwdl/blob/master/Dockerfile) for an example of dependencies prior to running `setup.py`

## Command-line tools

### `miniwdl check`

``miniwdl check /path/to/workflow.wdl`` loads the WDL document and shows a brief outline with any lint warnings. Add ``--path /path/to/tasks/`` with a directory to search for imported documents (one or more times). Example with [HumanCellAtlas/skylab](https://github.com/HumanCellAtlas/skylab):

```
$ git clone https://github.com/HumanCellAtlas/skylab.git
$ miniwdl check --path skylab/library/tasks/ \
    skylab/pipelines/smartseq2_single_sample/SmartSeq2SingleSample.wdl 

SmartSeq2SingleSample.wdl
    workflow SmartSeq2SingleCell
        (Ln 14, Col 8) UnusedDeclaration, nothing references File gtf_file
        call HISAT2.HISAT2PairedEnd
        call Picard.CollectMultipleMetrics
        call Picard.CollectRnaMetrics
        call Picard.CollectDuplicationMetrics
        call HISAT2.HISAT2RSEM as HISAT2Transcriptome
        call RSEM.RSEMExpression
        call GroupQCs.GroupQCOutputs
        call ZarrUtils.SmartSeq2ZarrConversion
    GroupQCs : GroupMetricsOutputs.wdl
        task GroupQCOutputs
            (Ln 10, Col 10) StringCoercion, String mem = :Int:
            (Ln 11, Col 10) StringCoercion, String cpu = :Int:
            (Ln 12, Col 10) StringCoercion, String disk_space = :Int:
    HISAT2 : HISAT2.wdl
        task HISAT2PairedEnd
        task HISAT2RSEM
        task HISAT2InspectIndex (not called)
        task HISAT2SingleEnd (not called)
    Picard : Picard.wdl
        task CollectDuplicationMetrics
        task CollectMultipleMetrics
        task CollectRnaMetrics
    RSEM : RSEM.wdl
        task RSEMExpression
    ZarrUtils : ZarrUtils.wdl
        task SmartSeq2ZarrConversion
            (Ln 36, Col 6) CommandShellCheck, SC2006 Use $(..) instead of legacy `..`.
            (Ln 39, Col 9) CommandShellCheck, SC2006 Use $(..) instead of legacy `..`.
            (Ln 39, Col 15) CommandShellCheck, SC2086 Double quote to prevent globbing and word splitting.
            (Ln 40, Col 10) CommandShellCheck, SC2086 Double quote to prevent globbing and word splitting.
            (Ln 40, Col 21) CommandShellCheck, SC2086 Double quote to prevent globbing and word splitting.
```

In addition to its suite of WDL-specific warnings, `miniwdl check` uses [ShellCheck](https://www.shellcheck.net/), if available, to detect possible issues in each task command script. You may need to install ShellCheck separately, as it's not included with miniwdl.

If you haven't installed the PyPI package to get the `miniwdl` entry point, equivalently `PYTHONPATH=$PYTHONPATH:/path/to/miniwdl python3 -m WDL check ...`.

### `miniwdl cromwell`

This tool provides a nicer command-line interface for running a workflow locally using [Cromwell](https://github.com/broadinstitute/cromwell). Example:

```
$ cat << 'EOF' > hello.wdl
version 1.0
task hello {
    input {
        Array[String]+ who
        Int x = 0
    }
    command <<<
        awk '{print "Hello", $0}' "~{write_lines(who)}"
    >>>
    output {
        Array[String]+ messages = read_lines(stdout())
        Int meaning_of_life = x+1
    }
}
EOF
$ miniwdl cromwell hello.wdl
missing required inputs for hello: who
required inputs:
  Array[String]+ who
optional inputs:
  Int x
outputs:
  Array[String]+ messages
  Int meaning_of_life
$ miniwdl cromwell hello.wdl who=Alyssa "who=Ben Bitdiddle" x=41
{
  "outputs": {
    "hello.messages": [
      "Hello Alyssa",
      "Hello Ben Bitdiddle"
    ],
    "hello.meaning_of_life": 42
  },
  "id": "b75f3449-344f-45ec-86b2-c004a3adc289",
  "dir": "/home/user/20190203_215657_hello"
}
```

By first analyzing the WDL code, this tool translates the freeform command-line arguments into appropriately-typed JSON inputs for Cromwell. It downloads the Cromwell JAR file automatically to a temporary location; a compatible `java` JRE must be available to run it, and the invoking [user must have permission to control Docker](https://docs.docker.com/install/linux/linux-postinstall/). You can use the `-r/--jar` option if you already have a local copy of Cromwell; other Cromwell configuration options are available (see `--help`).
 The outputs and logs are written to a new date/time-named subdirectory of the current working directory (overridable; see `--help`).

The tool supports shell tab-completion for the workflow's available input names. To use this, enable [argcomplete](https://argcomplete.readthedocs.io/en/latest/) global completion by invoking `activate-global-python-argcomplete` and starting a new shell session. Then, start a command line `miniwdl cromwell hello.wdl ` and try double-tab.

### `miniwdl run`

miniwdl's built-in capability to execute workflows on the local host is in early testing. The [Releases](https://github.com/chanzuckerberg/miniwdl/releases) page documents current salient limitations. If you encounter an interoperability problem not mentioned there, we want to hear about it via [Issues](https://github.com/chanzuckerberg/miniwdl/issues)!

Operation is nearly identical to `miniwdl cromwell`. The invoking [user must have permission to control Docker](https://docs.docker.com/install/linux/linux-postinstall/).

```
$ miniwdl run hello.wdl who=Alyssa "who=Ben Bitdiddle" x=41
{
  "outputs": {
    "hello.messages": [
      "Hello Alyssa",
      "Hello Ben Bitdiddle"
    ],
    "hello.meaning_of_life": 42
  },
  "dir": "/home/user/20190718_213847_hello"
}
```

## WDL Python library

The `WDL` package provides programmatic access to the WDL parser and AST. The following example prints all declarations in a workflow, descending into `scatter` and `if` stanzas as needed.

```
$ python3 -c "
import WDL

doc = WDL.load('skylab/pipelines/optimus/Optimus.wdl',
               path=['skylab/library/tasks/'])

def show(body):
  for elt in body:
    if isinstance(elt, WDL.Decl):
      print(str(elt.type) + ' ' + elt.name)
    elif isinstance(elt, WDL.Scatter) or isinstance(elt, WDL.Conditional):
      show(elt.body)
show(doc.workflow.body)
"

String version
Array[File] r1_fastq
Array[File] r2_fastq
Array[File] i1_fastq
String sample_id
File tar_star_reference
File annotations_gtf
File ref_genome_fasta
File whitelist
String fastq_suffix
Array[Int] indices
Array[File] non_optional_i1_fastq
File barcoded_bam
```

### API documentation

Online Python developer documentation for the `WDL` package: [![Docs Status](https://readthedocs.org/projects/miniwdl/badge/?version=latest)](https://miniwdl.readthedocs.io/en/latest/)

(Read the Docs currently builds from the [mlin/miniwdl](https://github.com/mlin/miniwdl) fork of this repository.)

Locally, `make doc` triggers [Sphinx](http://www.sphinx-doc.org/en/stable/) to generate the docs under `docs/_build/html/`. Or, after building the docker image, copy them out with `docker run --rm -v ~/Desktop:/io miniwdl cp -r /miniwdl/docs/_build/html /io/miniwdl_docs`.

## Contributing

Feedback and contributions are welcome on this repository. Please:

1. Add appropriate tests to the automatic suite
2. Use `make pretty` to reformat the code with [black](https://github.com/python/black)
3. Ensure compatibility with this project's MIT license
4. Send pull requests from a dedicated branch without unrelated edits

The [Project board](https://github.com/chanzuckerberg/miniwdl/projects/1) is our up-to-date tracker.

To set up your local development environment,

1. `git clone --recursive` this repository
2. Install dependencies as illustrated in the [Dockerfile](https://github.com/chanzuckerberg/miniwdl/blob/master/Dockerfile) (OS packages + PyPI packages listed in `requirements.txt` and `requirements.dev.txt`)
3. Invoking user must be able to [access local Docker daemon](https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user).

The Makefile has a few typical scripted flows:

- `make` or `make test` runs the full test suite with code coverage report (takes several minutes)
- `make qtest` runs most of the tests more quickly (by omitting some slower cases, and not tracking coverage)
- `make pretty` reformats the code with [black](https://github.com/python/black)
- `make check` validates the code with [Pylint](https://www.pylint.org/) and [Pyre](https://pyre-check.org/)

As the test suite takes several minutes to run, you can e.g. `python3 -m unittest -f tests/test_5stdlib.py` to run relevant subsets.

## Security

Please disclose security issues responsibly by contacting security@chanzuckerberg.com.
