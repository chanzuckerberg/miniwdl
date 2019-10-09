# Getting Started

In this tutorial, we'll use miniwdl's local workflow runner to run a [Human Cell Atlas (HCA) secondary analysis pipeline](https://github.com/HumanCellAtlas/skylab/) on a test dataset.

## Install miniwdl

Requirements:

1. Linux or Mac OS X
2. Python 3.6 or higher
3. [Docker Engine](https://docs.docker.com/install/) 17 or higher
4. User must have [permission to control Docker](https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user)


Installation options:

* via PyPI: `pip3 install miniwdl`
* via conda: `conda install miniwdl` after adding [conda-forge](https://conda-forge.org/docs/user/introduction.html)
* see the [GitHub repo README](https://github.com/chanzuckerberg/miniwdl/) to install from source

Then open a command prompt and try 

```miniwdl run_self_test```

to quickly test that miniwdl and Docker are working together, by running a trivial built-in workflow. This should print numerous log messages, and conclude with `miniwdl run_self_test OK` in about 30 seconds.

Please [file any issues](https://github.com/chanzuckerberg/miniwdl/issues) that arise in this!

## Fetch skylab and data

[Skylab](https://github.com/HumanCellAtlas/skylab/) is the HCA project's repository of WDL data processing workflows for high-throughput sequencing data. We'll get a copy of it as well as a medium-sized dataset used to test it.

Enter some working directory and either `git clone https://github.com/HumanCellAtlas/skylab.git` or download & extract the [source ZIP file](https://github.com/HumanCellAtlas/skylab/archive/master.zip).

Next download ~2 GB of test data to run the [Optimus pipeline](https://github.com/HumanCellAtlas/skylab/tree/master/pipelines/optimus) for 10X single-cell expression data. The files are staged in a public Google Cloud Storage bucket, so first [install gsutil](https://cloud.google.com/storage/docs/gsutil_install) and:

```
gsutil -m cp \
    gs://hca-dcp-mint-test-data/10x/demo/fastqs/pbmc8k_S1_L007_R1_001.fastq.gz     \
    gs://hca-dcp-mint-test-data/10x/demo/fastqs/pbmc8k_S1_L007_R2_001.fastq.gz     \
    gs://hca-dcp-mint-test-data/10x/demo/fastqs/pbmc8k_S1_L007_I1_001.fastq.gz     \
    gs://hca-dcp-mint-test-data/10x/whitelist/737K-august-2016.txt                 \
    gs://hca-dcp-mint-test-data/reference/demo/star.tar                            \
    gs://hca-dcp-mint-test-data/reference/demo/hg19_ds/GSM1629193_hg19_ERCC.gtf.gz \
    gs://hca-dcp-mint-test-data/reference/demo/chr21.fa                            \
    .
```

## Run Optimus workflow

First we can use miniwdl to see the workflow's required and optional inputs.

```
$ miniwdl run --path skylab/library/tasks skylab/pipelines/optimus/Optimus.wdl

missing required inputs for Optimus: whitelist, ref_genome_fasta, annotations_gtf, tar_star_reference, sample_id, r2_fastq, r1_fastq

required inputs:
  File whitelist
  File ref_genome_fasta
  File annotations_gtf
  File tar_star_reference
  String sample_id
  Array[File] r2_fastq
  Array[File] r1_fastq

optional inputs:
  Array[File]? i1_fastq
  (... omitted for brevity ...)

outputs:
  File? loom_output_file
  Array[File] zarr_output_files
  File cell_calls
  File gene_metrics
  File cell_metrics
  File matrix_col_index
  File matrix_row_index
  File matrix
  File bam
  String pipeline_version
```

* The ``--path`` argument is needed because the skylab repository keeps a common library of tasks in a separate directory, which other workflow WDL files import from. We usually won't need such an argument for other WDL codebases with simpler structures.

To run the workflow, miniwdl can accept the inputs as command-line arguments in most cases:

```
$ miniwdl run --copy-input-files --path skylab/library/tasks \
    skylab/pipelines/optimus/Optimus.wdl \
    r1_fastq=pbmc8k_S1_L007_R1_001.fastq.gz \
    r2_fastq=pbmc8k_S1_L007_R2_001.fastq.gz \
    i1_fastq=pbmc8k_S1_L007_I1_001.fastq.gz \
    whitelist=737K-august-2016.txt \
    tar_star_reference=star.tar \
    sample_id=pbmc8k_test \
    annotations_gtf=GSM1629193_hg19_ERCC.gtf.gz \
    ref_genome_fasta=chr21.fa
```

The workflow completes in 10-15 minutes with modest parallelization.

* The ``--copy-input-files`` argument is needed because some of the skylab tasks rename or delete their input files for various reasons. The miniwdl runner makes all input files read-only by default, causing such commands to fail; with ``--copy-input-files`` each task gets read/write file copies, permitting such operations whilst increasing disk space usage.
* Adding ``--verbose`` adds more status detail, including a realtime log of each task's standard error stream, often informative for workflow troubleshooting.

## Inspect results

## Next steps

