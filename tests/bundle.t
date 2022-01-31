#!/bin/bash
# bash-tap tests for `miniwdl bundle` command-line interface, and use of bundle in other commands
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
DN=$(mktemp -d "${TMPDIR}/miniwdl_bundle_tests_XXXXXX")
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

plan tests 11

$miniwdl bundle wdl/wf/outer.wdl --input ' {"w.who": "Alice"}' > my_bundle
is "$?" "0" "build bundle"
$miniwdl bundle wdl/wf/outer.wdl --input ' {"w.who": "Alice"}' --compress > compressed_bundle
is "$?" "0" "compress bundle"
$miniwdl check my_bundle
is "$?" "0" "check bundle"
$miniwdl bundle my_bundle > my_bundle2
diff my_bundle my_bundle2
is "$?" "0" "rebuild bundle"
diff <($miniwdl bundle --compress my_bundle) compressed_bundle
is "$?" "0" "recompress bundle"

$miniwdl run my_bundle | tee out
is "$?" "0" "run bundle"
is "$(jq -r '.outputs["w.hello.message"]' out)" "Hello, Alice!" "run bundle output"
$miniwdl run my_bundle who=Bob | tee out
is "$?" "0" "run bundle with input override"
is "$(jq -r '.outputs["w.hello.message"]' out)" "Hello, Bob!" "run bundle output 2"

MINIWDL__SOURCE__BUNDLE="$(cat compressed_bundle)" $miniwdl run foobar | tee out
is "$?" "0" "run env bundle"
is "$(jq -r '.outputs["w.hello.message"]' out)" "Hello, Alice!" "run bundle output 3"
