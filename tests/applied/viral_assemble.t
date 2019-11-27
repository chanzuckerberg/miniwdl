#!/bin/bash
set -eo pipefail

cd "$(dirname $0)/../.."
SOURCE_DIR="$(pwd)"

DN=$(mktemp -d --tmpdir miniwdl_runner_tests_XXXXXX)
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

$miniwdl run pipes/WDL/workflows/assemble_denovo_with_deplete.wdl \
    --path pipes/WDL/tasks --dir "$DN" --verbose \
    -i test/input/WDL/test_inputs-assemble_denovo_with_deplete-local.json
is "$?" "0" "pipeline success"
