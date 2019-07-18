#!/bin/bash
# bash-tap tests for the `miniwdl run` command-line interface
set -o pipefail

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

plan tests 45

DN=$(mktemp -d --tmpdir miniwdl_runner_tests_XXXXXX)
cd $DN
echo "$DN"

cat << 'EOF' > do_nothing.wdl
version 1.0
task do_nothing {
    command {}
}
EOF
$miniwdl run --dir do_nothing_task do_nothing.wdl | tee stdout
is "$?" "0" "run do_nothing task"
is "$(jq .outputs stdout)" "{}" "do_nothing task stdout"
is "$(jq .outputs do_nothing_task/outputs.json)" "{}" "do_nothing task outputs"

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

$miniwdl run --dir taskrun/ echo_task.wdl s=foo i=42 f=quick a_s=bar a_f=brown | tee stdout
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
is "$(ls taskrun/output_links/echo.out_f/0)" "quick" "task product quick link"
f1=$(jq -r '.outputs["echo.out_f"][1]' taskrun/outputs.json)
is "$(basename $f1)" "brown" "task product brown"
is "$(ls $f1)" "$f1" "task product brown file"
is "$(ls taskrun/output_links/echo.out_f/1)" "brown" "task product brown link"
f1=$(jq -r '.outputs["echo.out_f"][2]' taskrun/outputs.json)
is "$(basename $f1)" "fox" "task product fox"
is "$(ls $f1)" "$f1" "task product fox file"
is "$(ls taskrun/output_links/echo.out_f/2)" "fox" "task product fox link"

cat << 'EOF' > sleep.wdl
version 1.0
task sleep {
    input {
        Int seconds
    }

    command {
        sleep ~{seconds}
    }
}
EOF

t0=$(date +%s)
$miniwdl run sleep.wdl seconds=30 & pid=$!
sleep 3
kill $pid
wait $pid || true
t1=$(date +%s)
is "$(( t1 - t0 < 15 ))" "1" "task SIGTERM"

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

$miniwdl run --dir workflowrun echo.wdl t.s=foo t.f=quick t.a_s=bar t.a_f=brown --empty a_s | tee stdout
is "$?" "0" "workflow run"
is "$(jq '.outputs["echo.t.out_i"]' stdout)" "42" "workflow stdout out_i"
is "$(jq '.outputs["echo.t.out_i"]' workflowrun/outputs.json)" "42" "workflow outputs.json out_i"
is "$(jq '.outputs["echo.t.out_f"] | length' workflowrun/outputs.json)" '3' "workflow outputs.json out_f length"
f1=$(jq -r '.outputs["echo.t.out_f"][0]' workflowrun/outputs.json)
is "$(basename $f1)" "quick" "workflow product quick"
is "$(ls $f1)" "$f1" "workflow product quick file"
is "$(ls workflowrun/output_links/echo.t.out_f/0)" "quick" "workflow product quick link"
f1=$(jq -r '.outputs["echo.t.out_f"][1]' workflowrun/outputs.json)
is "$(basename $f1)" "brown" "workflow product brown"
is "$(ls $f1)" "$f1" "workflow product brown file"
is "$(ls workflowrun/output_links/echo.t.out_f/1)" "brown" "workflow product brown link"
f1=$(jq -r '.outputs["echo.t.out_f"][2]' workflowrun/outputs.json)
is "$(basename $f1)" "fox" "workflow product fox"
is "$(ls $f1)" "$f1" "workflow product fox file"
is "$(ls workflowrun/output_links/echo.t.out_f/2)" "fox" "workflow product fox link"

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
$miniwdl run --dir scatterrun scatter_echo.wdl n=2 t.s=foo t.f=quick t.a_s=bar t.a_f=brown | tee stdout
is "$?" "0" "scatter run"
is "$(ls scatterrun/output_links/echo.t.out_f/0/0)" "quick" "scatter product 0 quick link"
is "$(ls scatterrun/output_links/echo.t.out_f/0/1)" "brown" "scatter product 0 brown link"
is "$(ls scatterrun/output_links/echo.t.out_f/0/2)" "fox" "scatter product 0 fox link"
is "$(ls scatterrun/output_links/echo.t.out_f/1/0)" "quick" "scatter product 1 quick link"
is "$(ls scatterrun/output_links/echo.t.out_f/1/1)" "brown" "scatter product 1 brown link"
is "$(ls scatterrun/output_links/echo.t.out_f/1/2)" "fox" "scatter product 1 fox link"

$miniwdl run --dir failer2000 --verbose <(echo "
version 1.0
workflow failer2000 { call failer }
task failer { command { echo >&2 this is the end, beautiful friend; exit 1 } }
") 2> failer2000.log.txt
is "$?" "2" "failer2000"
grep -q beautiful failer2000/call-failer/stderr.txt
is "$?" "0" "failer2000 stderr"
grep -q beautiful failer2000.log.txt
is "$?" "0" "failer2000 stderr logged"

cat << 'EOF' > multitask.wdl
version 1.0
workflow multi {
    call first
}

task first {
    command {
        echo -n one
    }
    output {
        String msg = read_string(stdout())
    }
}

task second {
    command {
        echo -n two
    }
    output {
        String msg = read_string(stdout())
    }
}
EOF

$miniwdl run multitask.wdl --task second | tee stdout
is "$?" "0" "multitask"
is "$(jq -r '.outputs["second.msg"]' stdout)" "two" "multitask stdout"
