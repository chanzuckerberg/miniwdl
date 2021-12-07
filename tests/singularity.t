#!/bin/bash
# bash-tap tests for miniwdl's Singularity task runtime. `singularity` must be available.
set -o pipefail

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

if [[ -z $TMPDIR ]]; then
    TMPDIR=/tmp
fi
DN=$(mktemp -d "${TMPDIR}/miniwdl_runner_tests_XXXXXX")
DN=$(realpath "$DN")
cd $DN
echo "$DN"

plan tests 2

export MINIWDL__SCHEDULER__CONTAINER_BACKEND=singularity

$miniwdl run_self_test --dir "$DN"
is "$?" "0" "run_self_test"

git clone --depth=1 https://github.com/broadinstitute/viral-pipelines.git
cd viral-pipelines

$miniwdl run pipes/WDL/workflows/assemble_denovo.wdl \
    --path pipes/WDL/tasks --dir "$DN" --verbose \
    -i test/input/WDL/test_inputs-assemble_denovo-local.json
is "$?" "0" "assemble_denovo success"
