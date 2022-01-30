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

plan tests 7

$miniwdl bundle wdl/wf/outer.wdl --input ' {"w.who": "Alice"}' --compress > my_bundle
is "$?" "0"
$miniwdl check my_bundle
is "$?" "0"
$miniwdl bundle --compress <(miniwdl bundle my_bundle) > my_bundle2
cmp my_bundle my_bundle2
is "$?" "0"

$miniwdl run my_bundle | tee out
is "$?" "0"
is "$(jq -r '.outputs["w.hello.message"]' out)" "Hello, Alice!"
$miniwdl run my_bundle who=Bob | tee out
is "$?" "0"
is "$(jq -r '.outputs["w.hello.message"]' out)" "Hello, Bob!"
