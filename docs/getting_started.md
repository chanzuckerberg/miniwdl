# Getting Started

[miniwdl](https://github.com/chanzuckerberg/miniwdl/) is a local runner and developer toolkit for
the bioinformatics-focused [Workflow Description Language (WDL)](http://openwdl.org/>).  In this tutorial, we'll use miniwdl's runner with a [Human Cell Atlas (HCA) secondary analysis pipeline](https://github.com/HumanCellAtlas/skylab/) and test dataset.

## Install miniwdl

Requirements:

1. Linux or [macOS (limited support)](https://github.com/chanzuckerberg/miniwdl/issues/145)
2. Python 3.6 or higher
3. [Docker Engine](https://docs.docker.com/install/) 17 or higher
4. Unix user must have [permission to control Docker](https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user)


Installation options:

* via PyPI: `pip3 install miniwdl`
* via conda: `conda install miniwdl` after adding [conda-forge](https://conda-forge.org/docs/user/introduction.html)
* see the [GitHub repo README](https://github.com/chanzuckerberg/miniwdl/) to install from source

Then open a command prompt and try,

```miniwdl run_self_test```

to test that miniwdl and Docker are set up properly to run a trivial built-in workflow. This should print numerous log messages, and conclude with `miniwdl run_self_test OK` in about 30 seconds.

Please [file any issues](https://github.com/chanzuckerberg/miniwdl/issues) that arise!

## Fetch skylab and data

[Skylab](https://github.com/HumanCellAtlas/skylab/) is the HCA project's repository of WDL data processing workflows for high-throughput sequencing data. We'll fetch a copy of it and a test dataset to use for this exercise.

Enter some working directory and either,

```
git clone https://github.com/HumanCellAtlas/skylab.git
```

or download & extract the [source ZIP file](https://github.com/HumanCellAtlas/skylab/archive/master.zip).

Next, download ~6 GB of test data to run the [snap-atac pipeline](https://github.com/HumanCellAtlas/skylab/tree/master/pipelines/snap-atac) for single-cell ATAC-seq reads. The files are staged in a public Google Cloud Storage bucket, so first [install gsutil](https://cloud.google.com/storage/docs/gsutil_install) and:

```
gsutil -m cp                                                                                                 \
    gs://hca-dcp-sc-pipelines-test-data/alignmentReferences/snapATAC_BWA/hg38/hg38.tar                       \
    gs://hca-dcp-sc-pipelines-test-data/smallDatasets/snap-atac/readnames_preattached/test_500k.R1.fastq.gz  \
    gs://hca-dcp-sc-pipelines-test-data/smallDatasets/snap-atac/readnames_preattached/test_500k.R2.fastq.gz  \
    .
```

## Run snap-atac workflow

First we can use miniwdl to preview the workflow's inputs and outputs.

```
$ miniwdl run --path skylab/library/tasks skylab/pipelines/snap-atac/snap-atac.wdl

missing required inputs for scATAC: input_fastq1, input_fastq2, genome_name, input_reference

required inputs:
  File input_fastq1
  File input_fastq2
  String genome_name
  File input_reference

optional inputs:
  String output_bam
  String AlignPairedEnd.reference_unpack_name
  Int AlignPairedEnd.min_cov
  String AlignPairedEnd.docker_image
  String SnapPre.genome_size_file
  String SnapPre.docker_image
  String SnapCellByBin.snap_output_name
  String SnapCellByBin.docker_image
  String MakeCompliantBAM.output_bam_filename
  String MakeCompliantBAM.docker_image
  String BreakoutSnap.docker_image

outputs:
  File output_snap_qc
  File output_snap
  File output_aligned_bam
  File breakout_barcodes
  File breakout_fragments
  File breakout_binCoordinates
  File breakout_binCounts
  File breakout_barcodesSection
```

* The ``--path`` argument is needed because the skylab pipelines import from a common library of tasks in a separate directory. Simpler WDL codebases won't need this argument.

To run the workflow, miniwdl can accept the inputs as command-line arguments in most cases:

```
$ miniwdl run --copy-input-files --path skylab/library/tasks  \
    skylab/pipelines/snap-atac/snap-atac.wdl                  \
    input_fastq1=test_500k.R1.fastq.gz                        \
    input_fastq2=test_500k.R2.fastq.gz                        \
    genome_name=hg38                                          \
    input_reference=hg38.tar
```

The workflow should finish in about 10 minutes, with modest parallelization.

* The ``--copy-input-files`` argument is needed because some skylab tasks rename or delete their input files. The miniwdl runner makes all input files read-only by default, causing such commands to fail; they're permitted with ``--copy-input-files`` with increased disk space usage.
* Adding ``--verbose`` shows more status detail, including a realtime log of each task's standard error stream, often informative for debugging.
* Array inputs can be supplied on the command-line by repeating, e.g. `array_input1=/path/to/file1 array_input1=/path/to/file2` translates to `{"array_input1": ["/path/to/file1", "/path/to/file2"]}`
* Strings with spaces can be supplied by quoting the whole pair, `"name=Alyssa P. Hacker"`
* For other cases, you can supply a Cromwell-style JSON file with `--input inputs.json`.

See `miniwdl run --help` for other available options.

## Inspect results

By default, `miniwdl run` creates a new subdirectory of the current working directory, used for all of the workflow's operations. The subdirectory's name is timestamp-prefixed, so that multiple runs sort in the order they were invoked. The workflow directory can be overridden on the command line.

The standard output from `miniwdl run` provides the subdirectory along with the Cromwell-style JSON outputs, for example:

```
{
  "outputs": {
    "scATAC.output_snap_qc": "/tmp/snap-atac/20191012_120816_scATAC/call-SnapPre/work/output.snap.qc",
    "scATAC.breakout_binCounts": "/tmp/snap-atac/20191012_120816_scATAC/call-BreakoutSnap/work/output/binCounts_10000.csv",
    "scATAC.output_aligned_bam": "/tmp/snap-atac/20191012_120816_scATAC/call-MakeCompliantBAM/work/output.bam",
    "scATAC.output_snap": "/tmp/snap-atac/20191012_120816_scATAC/call-SnapCellByBin/work/output.snap",
    "scATAC.breakout_fragments": "/tmp/snap-atac/20191012_120816_scATAC/call-BreakoutSnap/work/output/fragments.csv",
    "scATAC.breakout_binCoordinates": "/tmp/snap-atac/20191012_120816_scATAC/call-BreakoutSnap/work/output/binCoordinates_10000.csv",
    "scATAC.breakout_barcodesSection": "/tmp/snap-atac/20191012_120816_scATAC/call-BreakoutSnap/work/output/barcodesSection.csv",
    "scATAC.breakout_barcodes": "/tmp/snap-atac/20191012_120816_scATAC/call-BreakoutSnap/work/output/barcodes.csv"
  },
  "dir": "/tmp/snap-atac/20191012_120816_scATAC"
}
```

This is also stored in `outputs.json` in the subdirectory. miniwdl furthermore generates an `output_links` directory tree containing symbolic links to the output files, which is sometimes more convenient to consume than the JSON:

```
$ tree 20191012_120816_scATAC/output_links
20191012_120816_scATAC/output_links
├── scATAC.breakout_barcodes
│   └── barcodes.csv -> /tmp/snap-atac/20191012_120816_scATAC/call-BreakoutSnap/work/output/barcodes.csv
├── scATAC.breakout_barcodesSection
│   └── barcodesSection.csv -> /tmp/snap-atac/20191012_120816_scATAC/call-BreakoutSnap/work/output/barcodesSection.csv
├── scATAC.breakout_binCoordinates
│   └── binCoordinates_10000.csv -> /tmp/snap-atac/20191012_120816_scATAC/call-BreakoutSnap/work/output/binCoordinates_10000.csv
├── scATAC.breakout_binCounts
│   └── binCounts_10000.csv -> /tmp/snap-atac/20191012_120816_scATAC/call-BreakoutSnap/work/output/binCounts_10000.csv
├── scATAC.breakout_fragments
│   └── fragments.csv -> /tmp/snap-atac/20191012_120816_scATAC/call-BreakoutSnap/work/output/fragments.csv
├── scATAC.output_aligned_bam
│   └── output.bam -> /tmp/snap-atac/20191012_120816_scATAC/call-MakeCompliantBAM/work/output.bam
├── scATAC.output_snap
│   └── output.snap -> /tmp/snap-atac/20191012_120816_scATAC/call-SnapCellByBin/work/output.snap
└── scATAC.output_snap_qc
    └── output.snap.qc -> /tmp/snap-atac/20191012_120816_scATAC/call-SnapPre/work/output.snap.qc
```

Individual tasks and sub-workflows run in their own nested subdirectories, each with a similar structure.

## Next steps

To aid the workflow development cycle, miniwdl includes a code quality checker which statically analyzes WDL source code to generate warnings and suggestions:

```
$ miniwdl check skylab/library/tasks/ZarrUtils.wdl
ZarrUtils.wdl
    task OptimusZarrConversion
        (Ln 110, Col 6) CommandShellCheck, SC2006 Use $(..) instead of legacy `..`.
        (Ln 113, Col 9) CommandShellCheck, SC2006 Use $(..) instead of legacy `..`.
        (Ln 113, Col 15) CommandShellCheck, SC2086 Double quote to prevent globbing and word splitting.
        (Ln 114, Col 10) CommandShellCheck, SC2086 Double quote to prevent globbing and word splitting.
        (Ln 114, Col 21) CommandShellCheck, SC2086 Double quote to prevent globbing and word splitting.
    task OptimusZarrToLoom
        (Ln 139, Col 5) UnusedDeclaration, nothing references Int cpu
    task SmartSeq2ZarrConversion
        (Ln 37, Col 6) CommandShellCheck, SC2006 Use $(..) instead of legacy `..`.
        (Ln 40, Col 9) CommandShellCheck, SC2006 Use $(..) instead of legacy `..`.
        (Ln 40, Col 15) CommandShellCheck, SC2086 Double quote to prevent globbing and word splitting.
        (Ln 41, Col 10) CommandShellCheck, SC2086 Double quote to prevent globbing and word splitting.
        (Ln 41, Col 21) CommandShellCheck, SC2086 Double quote to prevent globbing and word splitting.
```

Here miniwdl points out an unused declaration in one task, and (if [ShellCheck](https://www.shellcheck.net/) is installed) several suggestions for the embedded shell commands.

Installing miniwdl also makes available a `WDL` package for Python 3.6+, providing programmatic access to miniwdl's WDL parser and other functionality; its documentation follows here.

### Links

* [chanzuckerberg/miniwdl GitHub](https://github.com/chanzuckerberg/miniwdl/) where issues & contributions are welcome
* [openwdl/wdl GitHub](https://github.com/openwdl/wdl) for WDL spec, proposals, and discussion
* [CZI Science Technology](https://chanzuckerberg.com/technology/science/) sponsors this project
