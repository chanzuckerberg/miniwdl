# miniwdl
**[Workflow Description Language](http://openwdl.org/) static analysis toolkit for Python 3.6+**

![Project Status](https://img.shields.io/badge/status-alpha-red.svg)
[![PyPI version](https://img.shields.io/pypi/v/miniwdl.svg)](https://pypi.org/project/miniwdl/)
[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://github.com/chanzuckerberg/miniwdl/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Build Status](https://travis-ci.org/chanzuckerberg/miniwdl.svg?branch=master)](https://travis-ci.org/chanzuckerberg/miniwdl) [![Coverage Status](https://coveralls.io/repos/github/chanzuckerberg/miniwdl/badge.svg?branch=master)](https://coveralls.io/github/chanzuckerberg/miniwdl?branch=master)
[![Docs Status](https://readthedocs.org/projects/miniwdl/badge/?version=latest)](https://miniwdl.readthedocs.io/en/latest/)

*miniwdl* is a library for parsing WDL documents into a type-checked abstract syntax tree (AST), providing a foundation for new runtime systems, developer tooling, and language experimentation. It also includes command-line tools supporting the WDL development cycle, including a "linter" to statically analyze WDL documents for errors and oversights, and a [Cromwell](https://github.com/broadinstitute/cromwell) wrapper to make it more convenient to test a workflow locally.

This project in alpha development; interfaces are liable to change somewhat. See the [Releases](https://github.com/chanzuckerberg/miniwdl/releases) for change logs. The [Project board](https://github.com/chanzuckerberg/miniwdl/projects/1) reflects the near-term roadmap.

<!-- TOC generator tool: https://magnetikonline.github.io/markdown-toc-generate/ -->
- [Installation](#installation)
- [Command-line tools](#command-line-tools)
  - [miniwdl check](#miniwdl-check)
  - [miniwdl cromwell](#miniwdl-cromwell)
- [WDL Python library](#wdl-python-library)
  - [API documentation](#api-documentation)
- [Contributing](#contributing)

## Installation

``pip3 install miniwdl``

This will also install the [Lark parsing library](https://github.com/lark-parser/lark) automatically. 

For development, `git clone --recursive` this repository and ``docker build -t miniwdl .`` to run miniwdl's test suite in a locally-built docker container. The [Dockerfile](https://github.com/chanzuckerberg/miniwdl/blob/master/Dockerfile) illustrates how to configure another environment.


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

By first analyzing the WDL code, this tool translates the freeform command-line arguments into appropriately-typed JSON inputs for Cromwell. It downloads the Cromwell JAR file automatically to a temporary location; a compatible `java` JRE must be available to run it. The outputs and logs are written to a new date/time-named subdirectory of the current working directory (overridable; see `--help`).

## WDL Python library

The `WDL` package provides programmatic access to the WDL parser and AST. The following example prints all declarations in a workflow, descending into `scatter` and `if` stanzas as needed.

```
$ python3 -c "
import WDL

doc = WDL.load('skylab/pipelines/optimus/Optimus.wdl',
               path=['skylab/library/tasks/'])

def show(elements):
  for elt in elements:
    if isinstance(elt, WDL.Decl):
      print(str(elt.type) + ' ' + elt.name)
    elif isinstance(elt, WDL.Scatter) or isinstance(elt, WDL.Conditional):
      show(elt.elements)
show(doc.workflow.elements)
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

1. Send pull requests from a dedicated branch without unrelated edits
2. Add appropriate tests to the automatic suite
3. Use `make pretty` to reformat the code with [black](https://github.com/ambv/black)
4. Ensure compatibility with this project's MIT license

The [Project board](https://github.com/chanzuckerberg/miniwdl/projects/1) is our up-to-date tracker.
