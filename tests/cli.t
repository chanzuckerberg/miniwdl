#!/bin/bash
# bash-tap tests for the miniwdl command-line interface
set -o pipefail

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

plan tests 18

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

cat << EOF > lex_error.wdl
# comment
workflow 麻雀虽小五脏俱全 {
}
EOF
$miniwdl check lex_error.wdl > lex_error.out 2> lex_error.err
is "$?" "1" "lex_error.wdl exit code"
is "$(cat lex_error.out | wc -c)" "0" "lex_error.wdl stdout"
is "$(grep Traceback lex_error.err | wc -l)" "0" "lex_error.wdl stderr, no traceback"
is "$(grep 'line 2 col 10' lex_error.err | wc -l)" "1" "lex_error.wdl stderr, position"

cat << EOF > parse_error.wdl
# comment 1
# comment 2
workflow x {
EOF
$miniwdl check parse_error.wdl > parse_error.out 2> parse_error.err
is "$?" "1" "parse_error.wdl exit code"
is "$(cat parse_error.out | wc -c)" "0" "parse_error.wdl stdout"
is "$(grep Traceback parse_error.err | wc -l)" "0" "parse_error.wdl stderr, no traceback"
is "$(grep 'line 3, column 12' parse_error.err | wc -l)" "1" "parse_error.wdl stderr, position"

cat << EOF > import_error.wdl
import "bogus.wdl"
EOF
$miniwdl check import_error.wdl > import_error.out 2> import_error.err
is "$?" "1" "import_error.wdl exit code"
is "$(cat import_error.out | wc -c)" "0" "import_error.wdl stdout"
is "$(grep Traceback import_error.err | wc -l)" "0" "import_error.wdl stderr, no traceback"

rm -rf $DN
