#!/bin/bash
# bash-tap tests mirroring the miniwdl "Getting Started" tutorial
# https://miniwdl.readthedocs.io/en/latest/getting_started.html
# Warning: consumes about 32GB of disk space and peaks at >10GB of memory
set -eo pipefail

cd "$(dirname $0)/../.."
SOURCE_DIR="$(pwd)"

pip3 install .  # make sure gs:// downloader is registered

miniwdl run_self_test

if [[ -z $TMPDIR ]]; then
    TMPDIR=/tmp
fi
DN=$(mktemp -d "${TMPDIR}/miniwdl_runner_tests_XXXXXX")
DN=$(realpath "$DN")
cd $DN
echo "$DN"

git clone --depth 1 https://github.com/HumanCellAtlas/skylab.git

BASH_TAP_ROOT="$SOURCE_DIR/tests/bash-tap"
source $SOURCE_DIR/tests/bash-tap/bash-tap-bootstrap
plan tests 1
set +e

miniwdl run --copy-input-files --path skylab/library/tasks --verbose  \
    skylab/pipelines/snap-atac/snap-atac.wdl                           \
    input_fastq1=gs://hca-dcp-sc-pipelines-test-data/smallDatasets/snap-atac/readnames_preattached/test_500k.R1.fastq.gz  \
    input_fastq2=gs://hca-dcp-sc-pipelines-test-data/smallDatasets/snap-atac/readnames_preattached/test_500k.R2.fastq.gz  \
    input_reference=gs://hca-dcp-sc-pipelines-test-data/alignmentReferences/snapATAC_BWA/hg38/hg38.tar \
    genome_name=hg38
is "$?" "0" "snap-atac"
