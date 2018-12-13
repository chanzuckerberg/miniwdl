#!/bin/bash
# bash-tap tests for the miniwdl command-line interface
set -o pipefail

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

plan tests 7

DN=$(mktemp -d --tmpdir miniwdl_tests_XXXXXX)
cd $DN

# warm-up
$miniwdl check \
    --path "$SOURCE_DIR/test_corpi/HumanCellAtlas/skylab/library/tasks" \
    "$SOURCE_DIR/test_corpi/HumanCellAtlas/skylab/pipelines/optimus/Optimus.wdl" > optimus.out
is "$?" "0" "check Optimus.wdl"
is "$(grep UnusedDeclaration optimus.out | wc -l)" "1" "Optimus.wdl UnusedDeclaration"
is "$(grep NameCollision optimus.out | wc -l)" "1" "Optimus.wdl NameCollision"
is "$(cat optimus.out | wc -l)" "51" "Optimus.wdl output"

$miniwdl check \
    --path "$SOURCE_DIR/test_corpi/HumanCellAtlas/skylab/library/tasks" \
    "$SOURCE_DIR/test_corpi/HumanCellAtlas/skylab/pipelines/optimus/Optimus.wdl" \
    "$SOURCE_DIR/test_corpi/HumanCellAtlas/skylab/pipelines/smartseq2_single_sample/SmartSeq2SingleSample.wdl" \
    > optimus_smartseq2.out
is "$?" "0" "check Optimus.wdl and SmartSeq2SingleSample.wdl"
is "$(grep UnusedDeclaration optimus_smartseq2.out | wc -l)" "2" "UnusedDeclaration in Optimus.wdl and SmartSeq2SingleSample.wdl"
is "$(cat optimus_smartseq2.out | wc -l)" "80" "Optimus.wdl and SmartSeq2SingleSample.wdl output"

rm -rf $DN
