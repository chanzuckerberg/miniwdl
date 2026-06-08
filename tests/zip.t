#!/bin/bash
# bash-tap tests for `miniwdl zip` command-line interface
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
DN=$(mktemp -d "${TMPDIR}/miniwdl_zip_tests_XXXXXX")
DN=$(realpath "$DN")
cd $DN
echo "$DN"

mkdir -p foo/bar foo/bas
cat << 'EOF' > foo/bar/inner.wdl
version 1.1
task hello {
    input {
        String who
    }
    command {
        echo "Hello, ~{who}!"
    }
    output {
        String message = read_string(stdout())
    }
}
EOF
cat << 'EOF' > foo/bas/outer.wdl
version development
import "../bar/inner.wdl"
workflow w {
    input {
        String who
    }
    call inner.hello {
        input:
        who
    }
}
EOF

plan tests 19

$miniwdl zip -o outer.wdl.zip foo/bas/outer.wdl --input ' {"w.who": "Alice"}' --debug
is "$?" "0" "build zip"

mkdir __extract
env -C __extract unzip ../outer.wdl.zip
is "$?" "0" "extract zip"

$miniwdl check __extract/outer.wdl
is "$?" "0" "check extracted workflow"

$miniwdl run __extract | tee out
is "$?" "0" "run using default inputs"
is "$(jq -r '.outputs["w.hello.message"]' out)" "Hello, Alice!" "run extracted output"
$miniwdl run outer.wdl.zip who=Bob | tee out
is "$?" "0" "run zip with input override"
is "$(jq -r '.outputs["w.hello.message"]' out)" "Hello, Bob!" "run zip output 2"
$miniwdl run outer.wdl.zip -i '{"w.who": "Carol"}' | tee out
is "$?" "0" "run zip with input override file"
is "$(jq -r '.outputs["w.hello.message"]' out)" "Hello, Carol!" "run zip output 3"

# WDL 1.2 source-relative inputs packaged with --add
mkdir -p source_rel/src/data/glob root/runs root/tmp root/runs_tmpdir
cat << 'EOF' > source_rel/src/main.wdl
version 1.2
workflow w {
    File f = "data/input.txt"
    Array[File] gs = ["data/glob/a.txt", "data/glob/b.txt"]
    output {
        String s = read_string(f)
        String g = read_string(gs[1])
    }
}
EOF
echo "zip input" > source_rel/src/data/input.txt
echo "glob a" > source_rel/src/data/glob/a.txt
echo "glob b" > source_rel/src/data/glob/b.txt
$miniwdl zip -o root/source_rel.zip source_rel/src/main.wdl --add 'source_rel/src/data/**/*.txt' --debug
is "$?" "0" "build source-relative zip with --add glob"
mkdir __extract_source_rel
env -C __extract_source_rel unzip ../root/source_rel.zip
is "$(cat __extract_source_rel/data/input.txt)" "zip input" "source-relative zip preserves additional path"
cat << EOF > root.cfg
[file_io]
root = $DN/root
EOF
MINIWDL_CFG=$DN/root.cfg $miniwdl run root/source_rel.zip --dir root/runs | tee out_source_rel
is "$?" "0" "run source-relative zip with extraction fallback under file_io.root"
is "$(jq -r '.outputs["w.s"] + "|" + .outputs["w.g"]' out_source_rel)" "zip input|glob b" "run source-relative zip output"
TMPDIR=$DN/root/tmp MINIWDL_CFG=$DN/root.cfg $miniwdl run root/source_rel.zip --dir root/runs_tmpdir | tee out_source_rel_tmpdir
is "$?" "0" "run source-relative zip with TMPDIR under file_io.root"
is "$(jq -r '.outputs["w.s"]' out_source_rel_tmpdir)" "zip input" "run source-relative zip TMPDIR output"
touch badroot
cat << EOF > badroot.cfg
[file_io]
root = $DN/badroot
EOF
MINIWDL_CFG=$DN/badroot.cfg $miniwdl run root/source_rel.zip --dir badroot/runs > badroot.out 2> badroot.err
isnt "$?" "0" "source zip extraction fails when file_io.root cannot hold tempdir"
like "$(cat badroot.err)" "TMPDIR" "source zip extraction failure recommends TMPDIR"

# issue #610 regression test
mkdir -p issue610/wdl/tasks
wget -nv -O issue610/hello1.wdl https://raw.githubusercontent.com/openwdl/learn-wdl/9d05365/1_script_examples/1_hello_worlds/1_hello/hello.wdl
cp issue610/hello1.wdl issue610/wdl/tasks/hello2.wdl
cat << 'EOF' > issue610/wdl/test_miniwdl_zip.wdl
version 1.0

import "../hello1.wdl"
import "../wdl/tasks/hello2.wdl" as hello_b
import "https://raw.githubusercontent.com/openwdl/learn-wdl/9d05365/1_script_examples/1_hello_worlds/1_hello/hello.wdl"

workflow HelloWorld {
  call hello_b.WriteGreeting
}
EOF
$miniwdl zip -o issue610.zip issue610/wdl/test_miniwdl_zip.wdl --debug
is "$?" "0" "issue 610 (zip)"
mkdir __extract610
env -C __extract610 unzip ../issue610.zip
$miniwdl check __extract610/test_miniwdl_zip.wdl
is "$?" "0" "issue 610 (check)"
