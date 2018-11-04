# miniwdl
**[Workflow Description Language](http://openwdl.org/) static analysis toolkit for Python 3.6+**

*miniwdl* is a library for parsing WDL documents into a type-checked abstract syntax tree (AST), providing a foundation for new runtime systems, developer tooling, and language experimentation. It also includes a command-line tool which validates WDL documents and generates lint/style warnings.

**Pre-Alpha:** interfaces are liable to evolve substantially.

[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://github.com/chanzuckerberg/miniwdl/blob/master/LICENSE)
[![Build Status](https://travis-ci.org/chanzuckerberg/miniwdl.svg?branch=master)](https://travis-ci.org/chanzuckerberg/miniwdl) [![Coverage Status](https://coveralls.io/repos/github/chanzuckerberg/miniwdl/badge.svg?branch=master)](https://coveralls.io/github/chanzuckerberg/miniwdl?branch=master)

## Installation

``pip3 install miniwdl``

This will also install the [Lark parsing library](https://github.com/lark-parser/lark) automatically. 

For development, clone this repository and ``docker build -t miniwdl .`` to run miniwdl's test suite in a locally-built docker container. The [Dockerfile](https://github.com/chanzuckerberg/miniwdl/blob/master/Dockerfile) illustrates how to configure another environment.


## `miniwdl check`

Once installed, ``miniwdl check /path/to/workflow.wdl`` loads the WDL document and shows a brief outline with any lint warnings. Add ``--path /path/to/tasks/`` with a directory to search for imported documents (one or more times). Example with [HumanCellAtlas/skylab](https://github.com/HumanCellAtlas/skylab):

```
$ git clone https://github.com/HumanCellAtlas/skylab.git
$ miniwdl check --path skylab/library/tasks/ \
    skylab/pipelines/smartseq2_single_sample/SmartSeq2SingleSample.wdl 

SmartSeq2SingleSample.wdl
    workflow SmartSeq2SingleCell
        call HISAT2.HISAT2PairedEnd
        call Picard.CollectMultipleMetrics
        call Picard.CollectRnaMetrics
        call Picard.CollectDuplicationMetrics
        call HISAT2.HISAT2RSEM
        call RSEM.RSEMExpression
        call GroupQCs.GroupQCOutputs
        call ZarrUtils.SmartSeq2ZarrConversion
    GroupQCs : GroupMetricsOutputs.wdl
        task GroupQCOutputs
            (Ln 10, Col 3) ImpliedStringCoercion: String mem = <Int>
            (Ln 11, Col 3) ImpliedStringCoercion: String cpu = <Int>
            (Ln 12, Col 3) ImpliedStringCoercion: String disk_space = <Int>
    HISAT2 : HISAT2.wdl
        task HISAT2PairedEnd
        task HISAT2RSEM
        task HISAT2InspectIndex (not called)
        task HISAT2SingleEnd (not called)

    ...
```

If you haven't installed the PyPI package to get the `miniwdl` entry point, equivalently `python3 -m /path/to/miniwdl/WDL check ...`.

## `WDL` package

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

## Documentation

*(Link to hosted documentation will go here)*

`make doc` triggers [Sphinx](http://www.sphinx-doc.org/en/stable/) to generate the documentation under `docs/_build/html/`. Or, after building the docker image, copy them out with `docker run --rm -v ~/Desktop:/io miniwdl cp -r /miniwdl/docs/_build/html /io/miniwdl_docs`.

## Contributing

Contributions and feedback are welcome on this repository. The [Project board](https://github.com/chanzuckerberg/miniwdl/projects/1) is our up-to-date tracker.

