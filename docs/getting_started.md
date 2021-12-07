# Getting Started

<img src="https://raw.githubusercontent.com/chanzuckerberg/miniwdl/main/docs/miniwdl-logo.png" width="200"  />

[miniwdl](https://github.com/chanzuckerberg/miniwdl/) is a local runner and developer toolkit for
the bioinformatics-focused [Workflow Description Language (WDL)](http://openwdl.org/).  In this tutorial, we'll install miniwdl and use its runner to assemble an Ebola virus (EBOV) genome from short sequencing reads.  

TIP: If you are new to working with WDL workflow language, you may want to review the open source 'learn-wdl' course' - [link](https://github.com/openwdl/learn-wdl).    

Also there is an embedded short course 'learn-miniwdl' which includes screencasts reviewing the tutorial on this page in more detail - [link](https://github.com/openwdl/learn-wdl/tree/master/6_miniwdl_course)

## Install miniwdl

Requirements:

1. GNU/Linux or [macOS (specific steps required)](https://github.com/chanzuckerberg/miniwdl/issues/145)
2. Python 3.6 or higher
3. [Docker Engine](https://docs.docker.com/install/) 17 or higher (if unable, see [alternate container runtimes](https://miniwdl.readthedocs.io/en/latest/runner_backends.html))
4. Unix user must have [permission to control Docker](https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user)


Installation options:

* via PyPI: `pip3 install miniwdl`
* via conda: `conda install -c conda-forge miniwdl`
* see the [GitHub repo README](https://github.com/chanzuckerberg/miniwdl/) to install from source

Then open a command prompt and try,

```miniwdl run_self_test```

...to test the installation with a trivial built-in workflow. This should print numerous log messages, and conclude with `miniwdl run_self_test OK` in about 30 seconds.

## Fetch viral-pipelines

For this exercise we'll use the [Broad Institute's viral sequencing pipeline](https://github.com/broadinstitute/viral-pipelines/), which includes a small EBOV dataset for testing. Start by fetching a copy,

```
wget -nv -O - https://github.com/broadinstitute/viral-pipelines/archive/v2.1.0.2.tar.gz | tar zx
cd viral-pipelines-*
```

## Run assemble_refbased workflow

First we can use miniwdl to preview the inputs and outputs of the [reference-based assembly workflow](https://github.com/broadinstitute/viral-pipelines/blob/master/pipes/WDL/workflows/assemble_refbased.wdl):

```
$ miniwdl run pipes/WDL/workflows/assemble_refbased.wdl

missing required inputs for assemble_refbased: reads_unmapped_bams, reference_fasta

required inputs:
  Array[File]+ reads_unmapped_bams
  File reference_fasta

optional inputs:
  String sample_name
  ...

outputs:
  File assembly_fasta
  Int assembly_length
  Int assembly_length_unambiguous
  Int reference_genome_length
  Float assembly_mean_coverage
  ...
```

To invoke the workflow, miniwdl can accept the inputs as command-line arguments in most cases. Here we'll start it on the test reads and EBOV reference genome included in the repository:

```
$ miniwdl run pipes/WDL/workflows/assemble_refbased.wdl   \
    reads_unmapped_bams=test/input/G5012.3.testreads.bam  \
    reference_fasta=test/input/ebov-makona.fasta          \
    sample_name=G5012.3 --verbose
```

The workflow should finish in just a few minutes.

* Adding ``--verbose`` shows more status detail, including a realtime log of each task's standard error stream (often informative for debugging).
* A space may be included after a `=` and before an input value, allowing shell filename autocompletion on the latter.
* Array inputs can be supplied on the command-line by repeating, e.g. `array_input1=/path/to/file1 array_input1=/path/to/file2` translates to `{"array_input1": ["/path/to/file1", "/path/to/file2"]}`
* Strings with spaces can be supplied by quoting the whole pair, `"name=Wid L. Hacker"`
* For other cases or to separate inputs from the invocation, you can supply a [Cromwell-style JSON](https://support.terra.bio/hc/en-us/articles/360037120252) file with `--input inputs.json`.

## Inspect results

By default, `miniwdl run` creates a new subdirectory of the current working directory, used for all of the workflow's operations. The subdirectory's name is timestamp-prefixed, so that multiple runs sort in the order they were invoked. The workflow directory can be overridden on the command line; see `miniwdl run --help` for details.

The standard output from `miniwdl run` provides the subdirectory along with JSON describing the workflow outputs, for example (abbreviated):

```
{
  "outputs": {
    "assemble_refbased.assembly_length": 18865,
    "assemble_refbased.assembly_length_unambiguous": 18865,
    "assemble_refbased.assembly_mean_coverage": 94.95885858958806,
    "assemble_refbased.assembly_fasta": "/tmp/viral-pipelines-2.1.0.2/20200604_132146_assemble_refbased/out/assembly_fasta/G5012.3.fasta",
    "assemble_refbased.reference_genome_length": 18959,
    ...
  },
  "dir": "/tmp/viral-pipelines-2.1.0.2/20200604_132146_assemble_refbased"
}
```

This is also stored in `outputs.json` in the subdirectory. For your convenience, miniwdl furthermore generates a symbolic link `_LAST` pointing to the timestamped subdirectory for most recent run; and an `out` directory tree containing symbolic links to the output files.

```
$ tree _LAST/out/
_LAST/out/
├── align_to_ref_merged_aligned_trimmed_only_bam
│   └── G5012.3.align_to_ref.trimmed.bam -> ../../call-merge_align_to_ref/work/G5012.3.align_to_ref.trimmed.bam
├── align_to_ref_merged_coverage_plot
│   └── G5012.3.coverage_plot.pdf -> ../../call-plot_ref_coverage/work/G5012.3.coverage_plot.pdf
├── align_to_ref_merged_coverage_tsv
│   └── G5012.3.coverage_plot.txt -> ../../call-plot_ref_coverage/work/G5012.3.coverage_plot.txt
├── align_to_ref_multiqc_report
│   └── multiqc.html -> ../../call-multiqc_align_to_ref/work/multiqc-output/multiqc.html
├── align_to_ref_per_input_aligned_flagstat
│   └── 0
│       └── G5012.3.testreads.all.bam.flagstat.txt -> ../../../call-align_to_ref-0/work/G5012.3.testreads.all.bam.flagstat.txt
├── align_to_ref_variants_vcf_gz
│   └── G5012.3.sites.vcf.gz -> ../../call-call_consensus/work/G5012.3.sites.vcf.gz
├── align_to_self_merged_aligned_only_bam
│   └── G5012.3.merge_align_to_self.bam -> ../../call-merge_align_to_self/work/G5012.3.merge_align_to_self.bam
├── align_to_self_merged_coverage_plot
│   └── G5012.3.coverage_plot.pdf -> ../../call-plot_self_coverage/work/G5012.3.coverage_plot.pdf
├── align_to_self_merged_coverage_tsv
│   └── G5012.3.coverage_plot.txt -> ../../call-plot_self_coverage/work/G5012.3.coverage_plot.txt
└── assembly_fasta
    └── G5012.3.fasta -> ../../call-call_consensus/work/G5012.3.fasta
```

The `out` links are often more convenient to consume than the JSON, but they only capture outputs that are files. Individual tasks and sub-workflows run in their own nested subdirectories, each with a similar structure.

## Next steps

The following pages document features and optimization for `miniwdl run`, including numerous available configuration options. Use `miniwdl configure` to create a configuration file with common options interactively.

To aid the workflow development cycle, miniwdl also includes a static code quality checker, `miniwdl check`. Lastly, installing miniwdl makes available a Python `WDL` package, providing programmatic access to miniwdl's WDL parser and runtime.

### Links

* [chanzuckerberg/miniwdl GitHub](https://github.com/chanzuckerberg/miniwdl/) where issues & contributions are welcome
* [openwdl/wdl GitHub](https://github.com/openwdl/wdl) for WDL spec, proposals, and discussion
* [CZI Science Technology](https://chanzuckerberg.com/technology/science/) sponsors this project
