#!/bin/bash
# bash-tap tests for the `miniwdl run` command-line interface
set -o pipefail

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

plan tests 20

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
is "$(ls taskrun/outputs/echo.out_f/0)" "quick" "task product quick link"
f1=$(jq -r '.outputs["echo.out_f"][1]' taskrun/outputs.json)
is "$(basename $f1)" "brown" "task product brown"
is "$(ls $f1)" "$f1" "task product brown file"
is "$(ls taskrun/outputs/echo.out_f/1)" "brown" "task product brown link"
f1=$(jq -r '.outputs["echo.out_f"][2]' taskrun/outputs.json)
is "$(basename $f1)" "fox" "task product fox"
is "$(ls $f1)" "$f1" "task product fox file"
is "$(ls taskrun/outputs/echo.out_f/2)" "fox" "task product fox link"

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
$miniwdl run sleep.wdl seconds=10 & pid=$!
sleep 3
kill $pid
wait $pid || true
t1=$(date +%s)
is "$(( t1 - t0 < 5 ))" "1" "task SIGTERM"
