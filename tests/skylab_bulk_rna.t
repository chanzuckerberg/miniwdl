#!/bin/bash
# run the HCA skylab bulk RNA pipeline test
set -eo pipefail

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

DN=$(mktemp -d --tmpdir miniwdl_runner_tests_XXXXXX)
cd $DN
echo "$DN"

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

git clone --depth 1 https://github.com/HumanCellAtlas/skylab.git

BASH_TAP_ROOT="$SOURCE_DIR/tests/bash-tap"
source $SOURCE_DIR/tests/bash-tap/bash-tap-bootstrap
plan tests 1
set +e

$miniwdl run --path skylab/library/tasks --path skylab/pipelines/bulk_rna_pipeline  \
    skylab/test/bulk_rna_pipeline/pr/test_bulk_rna_pipeline_PR.wdl                  \
    -i skylab/test/bulk_rna_pipeline/pr/test_inputs.json --verbose --no-quant-check
is "$?" "0" "pipeline success"
