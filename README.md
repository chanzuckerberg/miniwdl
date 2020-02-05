# miniwdl
**[Workflow Description Language](http://openwdl.org/) local runner & developer toolkit for Python 3.6+**

![Project Status](https://img.shields.io/badge/status-beta-yellow.svg)
[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://github.com/chanzuckerberg/miniwdl/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Build Status](https://travis-ci.org/chanzuckerberg/miniwdl.svg?branch=master)](https://travis-ci.org/chanzuckerberg/miniwdl) [![Coverage Status](https://coveralls.io/repos/github/chanzuckerberg/miniwdl/badge.svg?branch=master)](https://coveralls.io/github/chanzuckerberg/miniwdl?branch=master)
[![Docs Status](https://readthedocs.org/projects/miniwdl/badge/?version=latest)](https://miniwdl.readthedocs.io/en/latest/)

<!-- TOC generator tool: https://magnetikonline.github.io/markdown-toc-generate/ -->
- [Getting Started](#getting-started)
- [Contributing](#contributing)
- [Command-line tools](#command-line-tools)
  - [miniwdl check](#miniwdl-check)
  - [miniwdl run](#miniwdl-run)
  - [miniwdl cromwell](#miniwdl-cromwell)
- [WDL Python library](#wdl-python-library)
  - [API documentation](#api-documentation)
- [Security](#security)

## Getting Started

### [Start Here: tutorial with a Human Cell Atlas data pipeline](https://miniwdl.readthedocs.io/en/latest/getting_started.html)

[![PyPI version](https://img.shields.io/pypi/v/miniwdl.svg)](https://pypi.org/project/miniwdl/) `pip3 install miniwdl`

[![Anaconda-Server Badge](https://anaconda.org/conda-forge/miniwdl/badges/version.svg)](https://anaconda.org/conda-forge/miniwdl) `conda install miniwdl` after adding [conda-forge](http://conda-forge.org/docs/user/introduction.html)

Source install: see the [Dockerfile](https://github.com/chanzuckerberg/miniwdl/blob/master/Dockerfile) for dependencies to run `setup.py`

See the [Releases](https://github.com/chanzuckerberg/miniwdl/releases) for change logs. The [Project board](https://github.com/chanzuckerberg/miniwdl/projects/1) shows the current prioritization of [issues](https://github.com/chanzuckerberg/miniwdl/issues).

## [Contributing](https://github.com/chanzuckerberg/miniwdl/blob/master/CONTRIBUTING.md)

Feedback and contributions to miniwdl are welcome, via issues and pull requests on this repository. See [CONTRIBUTING.md](https://github.com/chanzuckerberg/miniwdl/blob/master/CONTRIBUTING.md) for guidelines and instructions to set up your development environment.

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

### `miniwdl run`

miniwdl can run a parallelized workflow on the local host, provided that [Docker is installed](https://docs.docker.com/install/) and the invoking user has [permission to control it](https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user). (miniwdl uses the built-in [Docker Swarm](https://docs.docker.com/engine/swarm/) mode, which it'll enable locally if it isn't already.)

* Start with `miniwdl run_self_test` for a quick viability check.

By analyzing the WDL file, the runner can receive workflow inputs via the command line, as illustrated:

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
$ miniwdl run hello.wdl
missing required inputs for hello: who
required inputs:
  Array[String]+ who
optional inputs:
  Int x
outputs:
  Array[String]+ messages
  Int meaning_of_life
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

Relative or absolute paths are accepted for File inputs. The runner can also provide shell tab-completion for the workflow's available inputs. To use this, enable [argcomplete](https://argcomplete.readthedocs.io/en/latest/) global completion by invoking `activate-global-python-argcomplete` and starting a new shell session. Then, start a command line `miniwdl run hello.wdl ` and try double-tab.

Lastly, inputs can be supplied through a Cromwell-style JSON file; see `miniwdl run --help` for this and other options.

The miniwdl runner is still in beta testing, and the [Releases](https://github.com/chanzuckerberg/miniwdl/releases) page documents certain existing limitations. If you encounter a WDL 1.0 interoperability problem not mentioned there, please file it via [Issues](https://github.com/chanzuckerberg/miniwdl/issues).

### `miniwdl cromwell`

This tool provides `miniwidl run`'s command-line interface for supplying the workflow's inputs, but calls out to [Cromwell](https://github.com/broadinstitute/cromwell) to actually run it instead of the built-in runtime.

```
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


It downloads the Cromwell JAR file automatically to a temporary location; a compatible `java` JRE must be available. You can use the `-r/--jar` option if you already have a local copy of Cromwell; other Cromwell configuration options are available (see `miniwdl cromwell --help`).

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

Online Python developer documentation for the `WDL` package: [![Docs Status](https://readthedocs.org/projects/miniwdl/badge/?version=latest)](https://miniwdl.readthedocs.io/en/latest/WDL.html)

The documentation includes several Python [Codelabs](https://miniwdl.readthedocs.io/en/latest/WDL.html#python-codelabs) to get started.

Read the Docs currently builds from the [mlin/miniwdl](https://github.com/mlin/miniwdl) fork of this repository. Locally, `make doc` triggers [Sphinx](http://www.sphinx-doc.org/en/stable/) to generate the docs under `docs/_build/html/`. Or, after building the docker image, copy them out with `docker run --rm -v ~/Desktop:/io miniwdl cp -r /miniwdl/docs/_build/html /io/miniwdl_docs`.

## Security

Please disclose security issues responsibly by contacting security@chanzuckerberg.com.
