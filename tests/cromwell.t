#!/bin/bash
# bash-tap tests for the `miniwdl cromwell` command-line interface
set -o pipefail

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

plan tests 20

DN=$(mktemp -d --tmpdir miniwdl_cromwell_tests_XXXXXX)
cd $DN
echo "$DN"

cat << 'EOF' > echo_task.wdl
version 1.0
task echo {
    input {
        String s
        Int i
        File f
        Array[String]+ a_s
        Array[File] a_f
        File? o_f
        Array[String]? o_a_s
    }

    command {}

    output {
        Int out_i = i
        Array[String]+ out_s = flatten([[s],a_s])
        Array[File]+ out_f = flatten([[f],a_f,select_all([o_f])])
    }
}
EOF
touch quick brown fox

$miniwdl cromwell echo_task.wdl 2> stderr
is "$?" "2" "help status"
is "$(cat stderr | wc -l)" "19" "help lines"

$miniwdl cromwell --json echo_task.wdl s=foo i=42 f=quick a_s=bar a_s=baz a_f=brown > task_inputs.json
is "$?" "0" "task json status"
is "$(jq .i task_inputs.json)" "42" "task json i"
is "$(jq '.a_s | length' task_inputs.json)" "2" "task json a_s length"
is "$(jq .a_s[0] task_inputs.json)" '"bar"' "task json a_s bar"
is "$(jq .a_s[1] task_inputs.json)" '"baz"' "task json a_s baz"

$miniwdl cromwell --dir taskrun/ echo_task.wdl s=foo i=42 f=quick a_s=bar a_f=brown | tee stdout
is "$?" "0" "task run"
is "$(ls -1 taskrun/outputs/ | wc -l)" "2" "task output count"
is "$(ls taskrun/outputs/quick)" "taskrun/outputs/quick" "task output quick"
is "$(ls taskrun/outputs/brown)" "taskrun/outputs/brown" "task output brown"

cat << 'EOF' > echo.wdl
version 1.0
import "echo_task.wdl" as lib
workflow echo {
    input {
        Int i = 42
        Array[String] a_s = ["bat"]
    }
    call lib.echo as t { input:
        i = i,
        o_a_s = a_s
    }
}
EOF
$miniwdl cromwell echo.wdl 2> stderr
is "$?" "2" "help status"
is "$(cat stderr | wc -l)" "19" "help lines"

$miniwdl cromwell echo.wdl t.s=foo t.f=quick t.a_s=bar t.a_f=brown --empty a_s --json > workflow_inputs.json
is "$?" "0" "workflow json status"
is "$(jq '.["echo.t.a_s"] | length' workflow_inputs.json)" "1" "workflow json t.a_s length"
is "$(jq '.["echo.as"] | length' workflow_inputs.json)" "0" "--empty"

$miniwdl cromwell --dir workflowrun echo.wdl t.s=foo t.f=quick t.a_s=bar t.a_f=brown --empty a_s | tee stdout
is "$?" "0" "workflow run"
is "$(ls -1 workflowrun/outputs/ | wc -l)" "2" "workflow output count"
is "$(ls workflowrun/outputs/quick)" "workflowrun/outputs/quick" "workflow output quick"
is "$(ls workflowrun/outputs/brown)" "workflowrun/outputs/brown" "workflow output brown"

# TODO: look at output JSONs when we figure out how to collect them

rm -rf $DN
