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

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

BASH_TAP_ROOT="$SOURCE_DIR/tests/bash-tap"
source $SOURCE_DIR/tests/bash-tap/bash-tap-bootstrap
plan tests 1
set +e

$miniwdl run https://raw.githubusercontent.com/mlin/DeepVariant-GLnexus-WDL/master/test/range1KGP.wdl --verbose \
    -i https://raw.githubusercontent.com/mlin/DeepVariant-GLnexus-WDL/master/test/range1KGP.test26.ALDH2.json
is "$?" "0" "pipeline success"
