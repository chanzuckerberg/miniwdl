#!/bin/bash
# bash-tap tests for the miniwdl command-line interface
set -o pipefail

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

plan tests 36

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

$miniwdl check --debug lex_error.wdl > lex_error_debug.out 2> lex_error_debug.err
is "$(grep Traceback lex_error_debug.err | wc -l)" "1" "lex_error.wdl stderr, traceback"

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

cat << EOF > import_parse_error.wdl
import "parse_error.wdl"
EOF
$miniwdl check import_parse_error.wdl > import_parse_error.out 2> import_parse_error.err
is "$?" "1" "import_parse_error.wdl exit code"
is "$(cat import_parse_error.out | wc -c)" "0" "import_parse_error.wdl stdout"
is "$(grep Traceback import_parse_error.err | wc -l)" "0" "import_parse_error.wdl stderr, no traceback"
is "$(grep 'Failed to import parse_error.wdl' import_parse_error.err | wc -l)" "1" "import_parse_error.wdl stderr, outer error"
is "$(grep 'line 3, column 12' import_parse_error.err | wc -l)" "1" "import_parse_error.wdl stderr, inner position"

cat << EOF > trivial_type_error.wdl
workflow x {
    Int x = "42"
}
EOF
$miniwdl check trivial_type_error.wdl > trivial_type_error.out 2> trivial_type_error.err
is "$?" "1" "trivial_type_error.wdl exit code"
is "$(cat trivial_type_error.out | wc -c)" "0" "trivial_type_error.wdl stdout"
is "$(grep Traceback trivial_type_error.err | wc -l)" "0" "trivial_type_error.wdl stderr, no traceback"
is "$(grep '(trivial_type_error.wdl Ln 2, Col 13) Expected Int instead of String' trivial_type_error.err | wc -l)" "1" "trivial_type_error.wdl error message line 1"
is "$(grep '        Int x = \"42\"' trivial_type_error.err | wc -l)" "1" "trivial_type_error.wdl error message line 2"
is "$(grep '                ^^^^' trivial_type_error.err | wc -l)" "1" "trivial_type_error.wdl error message line 3"

cat << EOF > multi_error.wdl
task t {
    Int? x
    Int y = x
    Array[Int] z = [x]
    command {}
}
EOF
cat << EOF > import_multi_error.wdl
import "multi_error.wdl"
EOF
$miniwdl check import_multi_error.wdl 2> import_multi_error.err
is "$?" "1" "import_multi_error.wdl exit code"
is "$(grep '                ^' import_multi_error.err | wc -l)" "2" "import_multi_error.wdl stderr marker 1"
is "$(grep '                       ^^^' import_multi_error.err | wc -l)" "1" "import_multi_error.wdl stderr marker 2"
$miniwdl check --no-quant-check import_multi_error.wdl > import_multi_error.no_quant_check.out
is "$?" "0" "import_multi_error.wdl --no-quant-check"
is "$(grep QuantityCoercion import_multi_error.no_quant_check.out | wc -l)" "2" "import_multi_error.wdl --no-quant-check QuantityCoercion"
is "$(grep UnusedDeclaration import_multi_error.no_quant_check.out | wc -l)" "2" "import_multi_error.wdl --no-quant-check UnusedDeclaration"

rm -rf $DN
