#!/bin/bash
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

git clone --depth=1 https://github.com/broadinstitute/viral-pipelines.git
cd viral-pipelines

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

BASH_TAP_ROOT="$SOURCE_DIR/tests/bash-tap"
source $SOURCE_DIR/tests/bash-tap/bash-tap-bootstrap
plan tests 1
set +e

$miniwdl run pipes/WDL/workflows/assemble_denovo.wdl \
    --path pipes/WDL/tasks --dir "$DN" --verbose \
    -i test/input/WDL/test_inputs-assemble_denovo-local.json
is "$?" "0" "pipeline success"
