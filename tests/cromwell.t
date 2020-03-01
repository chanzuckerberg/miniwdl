#!/bin/bash
# bash-tap tests for the `miniwdl cromwell` command-line interface
set -o pipefail

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

plan tests 59

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
rundir="$(jq -r .dir stdout)"
is "$(dirname "$rundir")" "${DN}/do_nothing_task" "do_nothing task created subdirectory"
is "$(jq . "$rundir/outputs.json")" "{}" "do_nothing task outputs"

cat << 'EOF' > do_nothing_wf.wdl
version 1.0
workflow do_nothing {
}
EOF
$miniwdl cromwell --dir do_nothing_wf do_nothing_wf.wdl | tee stdout
is "$?" "0" "run do_nothing workflow"
is "$(jq .outputs stdout)" "{}" "do_nothing workflow stdout"
rundir="$(jq -r .dir stdout)"
is "$(dirname "$rundir")" "${DN}/do_nothing_wf" "do_nothing workflow created subdirectory"
is "$(jq . "$rundir/outputs.json")" "{}" "do_nothing workflow outputs"

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
is "$(cat stderr | wc -l)" "20" "help lines"

$miniwdl cromwell --json echo_task.wdl s=foo i=42 f=quick a_s=bar a_s=baz a_f=brown > task_inputs.json
is "$?" "0" "task json status"
is "$(jq .i task_inputs.json)" "42" "task json i"
is "$(jq '.a_s | length' task_inputs.json)" "2" "task json a_s length"
is "$(jq .a_s[0] task_inputs.json)" '"bar"' "task json a_s bar"
is "$(jq .a_s[1] task_inputs.json)" '"baz"' "task json a_s baz"

$miniwdl cromwell --dir taskrun/. echo_task.wdl s=foo i=42 f=quick a_s=bar a_f=brown | tee stdout
is "$?" "0" "task run"
is "$(jq '.outputs["echo.out_i"]' stdout)" "42" "task stdout out_i"
is "$(jq '.["echo.out_i"]' taskrun/outputs.json)" "42" "task outputs.json out_i"
is "$(jq '.["echo.out_s"] | length' taskrun/outputs.json)" "2" "task outputs.json out_s length"
is "$(jq '.["echo.out_s"][0]' taskrun/outputs.json)" '"foo"' "task outputs.json out_s foo"
is "$(jq '.["echo.out_s"][1]' taskrun/outputs.json)" '"bar"' "task outputs.json out_s bar"
is "$(jq '.["echo.out_f"] | length' taskrun/outputs.json)" '3' "task outputs.json out_f length"
f1=$(jq -r '.["echo.out_f"][0]' taskrun/outputs.json)
is "$(basename $f1)" "quick" "task product quick"
is "$(ls $f1)" "$f1" "task product quick file"
f1=$(jq -r '.["echo.out_f"][1]' taskrun/outputs.json)
is "$(basename $f1)" "brown" "task product brown"
is "$(ls $f1)" "$f1" "task product brown file"
f1=$(jq -r '.["echo.out_f"][2]' taskrun/outputs.json)
is "$(basename $f1)" "fox" "task product fox"
is "$(ls $f1)" "$f1" "task product fox file"
is "$(ls taskrun/output_links/out_f/2)" "fox" "task product fox link"

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
is "$(cat stderr | wc -l)" "20" "help lines"

$miniwdl cromwell echo.wdl t.s=foo t.f=quick t.a_s=bar t.a_f=brown --empty a_s --json > workflow_inputs.json
is "$?" "0" "workflow json status"
is "$(jq '.["echo.t.a_s"] | length' workflow_inputs.json)" "1" "workflow json t.a_s length"
is "$(jq '.["echo.a_s"] | length' workflow_inputs.json)" "0" "workflow json --empty"

echo '{"echo.i":88,"echo.t.f":"quick","echo.t.a_f":["brown"],"echo.a_s":["bogus"]}' > test_input.json
$miniwdl cromwell echo.wdl t.s=foo t.a_s=bar a_s=ok --input test_input.json --empty a_s --json > workflow_inputs2.json
is "$?" "0" "workflow --input json status"
is "$(jq '.["echo.i"]' workflow_inputs2.json)" "88" "workflow --input json i"
is "$(basename `jq -r '.["echo.t.f"]' workflow_inputs2.json`)" 'quick' "workflow --input json t.f"
is "$(jq '.["echo.a_s"] | length' workflow_inputs2.json)" "1" "workflow --input --empty"
is "$(jq -r '.["echo.a_s"][0]' workflow_inputs2.json)" "ok" "workflow --input --empty & append"

# issue #230: relative file paths in input JSON file
mkdir relative
echo Alice > relative/file
echo '{"who": "relative/file"}' > relative/input.json
cat << 'EOF' > greet.wdl
version 1.0
task greet {
    input {
        File who
    }
    command <<<
        echo "Hello, $(cat ~{who})!"
    >>>
    output {
        String message = read_string(stdout())
    }
}
EOF
$miniwdl cromwell greet.wdl --input relative/input.json | tee stdout
is "$?" "0" "relative file path in input JSON file"
is "$(jq -r '.outputs["greet.message"]' stdout)" 'Hello, Alice!' "output from relative path"

$miniwdl cromwell --dir workflowrun/. echo.wdl t.s=foo t.f=quick t.a_s=bar t.a_f=brown --empty a_s | tee stdout
is "$?" "0" "workflow run"
is "$(jq '.outputs["echo.t.out_i"]' stdout)" "42" "workflow stdout out_i"
is "$(jq '.["echo.t.out_i"]' workflowrun/outputs.json)" "42" "workflow outputs.json out_i"
is "$(jq '.["echo.t.out_f"] | length' workflowrun/outputs.json)" '3' "workflow outputs.json out_f length"
f1=$(jq -r '.["echo.t.out_f"][0]' workflowrun/outputs.json)
is "$(basename $f1)" "quick" "workflow product quick"
is "$(ls $f1)" "$f1" "workflow product quick file"
f1=$(jq -r '.["echo.t.out_f"][1]' workflowrun/outputs.json)
is "$(basename $f1)" "brown" "workflow product brown"
is "$(ls $f1)" "$f1" "workflow product brown file"
f1=$(jq -r '.["echo.t.out_f"][2]' workflowrun/outputs.json)
is "$(basename $f1)" "fox" "workflow product fox"
is "$(ls $f1)" "$f1" "workflow product fox file"
is "$(ls workflowrun/output_links/t.out_f/2)" "fox" "workflow product fox link"

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
$miniwdl cromwell --dir scatterrun/. scatter_echo.wdl n=2 t.s=foo t.f=quick t.a_s=bar t.a_f=brown | tee stdout
is "$?" "0" "scatter run"
is "$(ls scatterrun/output_links/t.out_f/0/2)" "fox" "scatter product 0 fox link"
is "$(ls scatterrun/output_links/t.out_f/1/2)" "fox" "scatter product 1 fox link"

mkdir my_imports
cat << 'EOF' > my_imports/my_hello.wdl
version 1.0
task hello {
    command {}
    output {
        String message = "Hello, world!"
    }
}
EOF
cat << 'EOF' > importer1.wdl
version 1.0
import "my_imports/my_hello.wdl"
workflow wf {
    call my_hello.hello
    output {
        String message = hello.message
    }
}
EOF
$miniwdl cromwell importer1.wdl | tee stdout
is "$?" "0" "relative importer"
is "$(jq -r '.outputs["wf.message"]' stdout)" "Hello, world!" "relative importer output"

cat << 'EOF' > cat_etc_issue.wdl
version 1.0

task cat_etc_issue {
    command {
        cat /etc/issue
    }
    output {
        String issue = read_string(stdout())
    }
}
EOF
$miniwdl cromwell cat_etc_issue.wdl --options <(echo '{"default_runtime_attributes":{"docker":"ubuntu:18.10"}}') | tee stdout
is "$?" "0" "workflow options JSON file"
grep -qF 18.10 stdout
is "$?" "0" "workflow options JSON file effective"

rm -rf $DN
