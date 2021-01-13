#!/bin/bash
# run the HCA skylab bulk RNA pipeline test
set -eo pipefail

cd "$(dirname $0)/../.."
SOURCE_DIR="$(pwd)"

if [[ -z $TMPDIR ]]; then
    TMPDIR=/tmp
fi
DN=$(mktemp -d "${TMPDIR}/miniwdl_runner_tests_XXXXXX")
DN=$(realpath "$DN")
cd $DN
echo "$DN"

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

BASH_TAP_ROOT="$SOURCE_DIR/tests/bash-tap"
source $SOURCE_DIR/tests/bash-tap/bash-tap-bootstrap
plan tests 1
set +e

$miniwdl run https://raw.githubusercontent.com/ENCODE-DCC/atac-seq-pipeline/v1.5.4/atac.wdl \
    -i https://storage.googleapis.com/encode-pipeline-test-samples/encode-atac-seq-pipeline/ENCSR356KRQ_subsampled_caper.json \
    --runtime-defaults '{"docker":"quay.io/encode-dcc/atac-seq-pipeline:v1.5.4"}' \
    --no-quant-check --verbose --runtime-memory-max 4G --runtime-cpu-max 2
is "$?" "0" "pipeline success"
