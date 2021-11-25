#!/bin/bash
# bash-tap tests for miniwdl's Podman task runtime. Must run under sudo, with `podman` available.
set -o pipefail

if [[ -z $SUDO_USER ]] || [ "$(id -u)" -ne 0 ]; then
    >&2 echo "These tests must run under sudo"
    exit 2
fi

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

pip3 install --user -r requirements.txt
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

export MINIWDL__SCHEDULER__CONTAINER_BACKEND=podman

$miniwdl run_self_test --dir "$DN"
is "$?" "0" "run_self_test"

git clone --depth=1 https://github.com/broadinstitute/viral-pipelines.git
cd viral-pipelines

$miniwdl run pipes/WDL/workflows/assemble_denovo.wdl \
    --path pipes/WDL/tasks --dir "$DN" --verbose \
    -i test/input/WDL/test_inputs-assemble_denovo-local.json
is "$?" "0" "assemble_denovo success"

#is "$(find -L _LAST/ | xargs -n 1 stat -c %u | sort | uniq)" "$SUDO_UID" \
#    "assemble_denovo artifacts all owned by $SUDO_USER"
