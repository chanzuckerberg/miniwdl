#!/bin/bash
# bash-tap tests for WDL multi-line strings (runs multi_line_strings.wdl)
set -o pipefail

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

plan tests 2

if [[ -z $TMPDIR ]]; then
    TMPDIR=/tmp
fi
DN=$(mktemp -d "${TMPDIR}/miniwdl_tests_XXXXXX")
DN=$(realpath "$DN")
cd $DN

$miniwdl run "$SOURCE_DIR/tests/multi_line_strings.wdl" --verbose
is $? "0"

$miniwdl run $SOURCE_DIR/tests/task_env_inputs.wdl --dir test_env
is "$?" "0" "env input escaping"
