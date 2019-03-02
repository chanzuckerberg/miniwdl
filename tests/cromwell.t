#!/bin/bash
# bash-tap tests for the `miniwdl cromwell` command-line interface
set -o pipefail

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

plan tests 54

DN=$(mktemp -d --tmpdir miniwdl_cromwell_tests_XXXXXX)
cd $DN
echo "$DN"

cat << 'EOF' > do_nothing.wdl
version 1.0
task do_nothing {
    command {}
}
EOF
$miniwdl cromwell --dir do_nothing_task do_nothing.wdl | tee stdout
is "$?" "0" "run do_nothing task"
is "$(jq .outputs stdout)" "{}" "do_nothing task stdout"
is "$(jq .outputs do_nothing_task/outputs.json)" "{}" "do_nothing task outputs"

cat << 'EOF' > do_nothing_wf.wdl
version 1.0
workflow do_nothing {
}
EOF
$miniwdl cromwell --dir do_nothing_wf do_nothing_wf.wdl | tee stdout
is "$?" "0" "run do_nothing workflow"
is "$(jq .outputs stdout)" "{}" "do_nothing workflow stdout"
is "$(jq .outputs do_nothing_wf/outputs.json)" "{}" "do_nothing workflow outputs"

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

    command {
        echo fox > fox
    }

    output {
        Int out_i = i
        Array[String]+ out_s = flatten([[s],a_s])
        Array[File]+ out_f = flatten([[f],a_f,select_all([o_f]),["fox"]])
    }
}
EOF
touch quick brown fox

$miniwdl cromwell echo_task.wdl 2> >(tee stderr >&2)
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
is "$(jq '.outputs["echo.out_i"]' stdout)" "42" "task stdout out_i"
is "$(jq '.outputs["echo.out_i"]' taskrun/outputs.json)" "42" "task outputs.json out_i"
is "$(jq '.outputs["echo.out_s"] | length' taskrun/outputs.json)" "2" "task outputs.json out_s length"
is "$(jq '.outputs["echo.out_s"][0]' taskrun/outputs.json)" '"foo"' "task outputs.json out_s foo"
is "$(jq '.outputs["echo.out_s"][1]' taskrun/outputs.json)" '"bar"' "task outputs.json out_s bar"
is "$(jq '.outputs["echo.out_f"] | length' taskrun/outputs.json)" '3' "task outputs.json out_f length"
f1=$(jq -r '.outputs["echo.out_f"][0]' taskrun/outputs.json)
is "$(basename $f1)" "quick" "task product quick"
is "$(ls $f1)" "$f1" "task product quick file"
is "$(ls taskrun/outputs/echo.out_f/0)" "quick" "task product quick link"
f1=$(jq -r '.outputs["echo.out_f"][1]' taskrun/outputs.json)
is "$(basename $f1)" "brown" "task product brown"
is "$(ls $f1)" "$f1" "task product brown file"
is "$(ls taskrun/outputs/echo.out_f/1)" "brown" "task product brown link"
f1=$(jq -r '.outputs["echo.out_f"][2]' taskrun/outputs.json)
is "$(basename $f1)" "fox" "task product fox"
is "$(ls $f1)" "$f1" "task product fox file"
is "$(ls taskrun/outputs/echo.out_f/2)" "fox" "task product fox link"

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
$miniwdl cromwell echo.wdl 2> >(tee stderr >&2)
is "$?" "2" "help status"
is "$(cat stderr | wc -l)" "19" "help lines"

$miniwdl cromwell echo.wdl t.s=foo t.f=quick t.a_s=bar t.a_f=brown --empty a_s --json > workflow_inputs.json
is "$?" "0" "workflow json status"
is "$(jq '.["echo.t.a_s"] | length' workflow_inputs.json)" "1" "workflow json t.a_s length"
is "$(jq '.["echo.as"] | length' workflow_inputs.json)" "0" "--empty"

$miniwdl cromwell --dir workflowrun echo.wdl t.s=foo t.f=quick t.a_s=bar t.a_f=brown --empty a_s | tee stdout
is "$?" "0" "workflow run"
is "$(jq '.outputs["echo.t.out_i"]' stdout)" "42" "workflow stdout out_i"
is "$(jq '.outputs["echo.t.out_i"]' workflowrun/outputs.json)" "42" "workflow outputs.json out_i"
is "$(jq '.outputs["echo.t.out_f"] | length' workflowrun/outputs.json)" '3' "workflow outputs.json out_f length"
f1=$(jq -r '.outputs["echo.t.out_f"][0]' workflowrun/outputs.json)
is "$(basename $f1)" "quick" "workflow product quick"
is "$(ls $f1)" "$f1" "workflow product quick file"
is "$(ls workflowrun/outputs/echo.t.out_f/0)" "quick" "workflow product quick link"
f1=$(jq -r '.outputs["echo.t.out_f"][1]' workflowrun/outputs.json)
is "$(basename $f1)" "brown" "workflow product brown"
is "$(ls $f1)" "$f1" "workflow product brown file"
is "$(ls workflowrun/outputs/echo.t.out_f/1)" "brown" "workflow product brown link"
f1=$(jq -r '.outputs["echo.t.out_f"][2]' workflowrun/outputs.json)
is "$(basename $f1)" "fox" "workflow product fox"
is "$(ls $f1)" "$f1" "workflow product fox file"
is "$(ls workflowrun/outputs/echo.t.out_f/2)" "fox" "workflow product fox link"

cat << 'EOF' > scatter_echo.wdl
version 1.0
import "echo_task.wdl" as lib
workflow echo {
    input {
        Int n
    }

    scatter (i in range(n)) {
        call lib.echo as t { input:
            i = i,
            o_a_s = ["bat"]
        }
    }
}
EOF
$miniwdl cromwell --dir scatterrun scatter_echo.wdl n=2 t.s=foo t.f=quick t.a_s=bar t.a_f=brown | tee stdout
is "$?" "0" "scatter run"
is "$(ls scatterrun/outputs/echo.t.out_f/0/0)" "quick" "scatter product 0 quick link"
is "$(ls scatterrun/outputs/echo.t.out_f/0/1)" "brown" "scatter product 0 brown link"
is "$(ls scatterrun/outputs/echo.t.out_f/0/2)" "fox" "scatter product 0 fox link"
is "$(ls scatterrun/outputs/echo.t.out_f/1/0)" "quick" "scatter product 1 quick link"
is "$(ls scatterrun/outputs/echo.t.out_f/1/1)" "brown" "scatter product 1 brown link"
is "$(ls scatterrun/outputs/echo.t.out_f/1/2)" "fox" "scatter product 1 fox link"

rm -rf $DN
