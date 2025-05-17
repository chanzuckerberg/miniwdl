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

plan tests 7

export MINIWDL__SCHEDULER__CONTAINER_BACKEND=singularity

$miniwdl run_self_test --dir "$DN"
is "$?" "0" "run_self_test"

export MINIWDL__SINGULARITY__IMAGE_CACHE=$(mktemp -d)

$miniwdl run_self_test --dir "$DN"
is "$?" "0" "run_self_test with image cache"
ls $MINIWDL__SINGULARITY__IMAGE_CACHE/*.sif
is "$?" "0" "singularity images cached successfully"

$miniwdl run_self_test --dir "$DN/use_cache"
is "$?" "0" "run_self_test with image cache"
grep 'SIF found in image cache directory' $(find "$DN/use_cache" -name workflow.log)
is "$?" "0" "singularity image used from cache"

$miniwdl run $SOURCE_DIR/tests/task_env_inputs.wdl --dir "$DN"
is "$?" "0" "env input escaping"

git clone --depth=1 https://github.com/broadinstitute/viral-pipelines.git
cd viral-pipelines

$miniwdl run pipes/WDL/workflows/assemble_denovo.wdl \
    --path pipes/WDL/tasks --dir "$DN" --verbose \
    -i test/input/WDL/test_inputs-assemble_denovo-local.json
is "$?" "0" "assemble_denovo success"
