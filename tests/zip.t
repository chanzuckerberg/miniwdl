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

mkdir -p wdl/wf
cat << 'EOF' > wdl/inner.wdl
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
cat << 'EOF' > wdl/wf/outer.wdl
version development
import "../inner.wdl"
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

plan tests 9

$miniwdl zip -o outer.wdl.zip wdl/wf/outer.wdl --input ' {"w.who": "Alice"}' --debug
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
