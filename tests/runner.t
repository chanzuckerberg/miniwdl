#!/bin/bash
# bash-tap tests for the `miniwdl run` command-line interface
set -o pipefail

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

plan tests 52

$miniwdl run_self_test
is "$?" "0" "run_self_test"

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
rundir="$(jq -r .dir stdout)"
is "$(dirname "$rundir")" "${DN}/do_nothing_task" "do_nothing task created subdirectory"
is "$(jq . "$rundir/outputs.json")" "{}" "do_nothing task outputs"

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

$miniwdl run --dir taskrun/. echo_task.wdl s=foo i=42 f=quick a_s=bar a_f=brown | tee stdout
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

$miniwdl run --dir workflowrun/. echo.wdl t.s=foo t.f=quick t.a_s=bar t.a_f=brown --empty a_s | tee stdout
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
is "$(cat workflowrun/rerun)" "pushd $DN && miniwdl run --dir workflowrun/. echo.wdl t.s=foo t.f=quick t.a_s=bar t.a_f=brown --empty a_s; popd"

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
MINIWDL__FILE_IO__OUTPUT_HARDLINKS=true $miniwdl run --dir scatterrun/. scatter_echo.wdl n=2 t.s=foo t.f=quick t.a_s=bar t.a_f=brown | tee stdout
is "$?" "0" "scatter run"
is "$(ls scatterrun/output_links/t.out_f/0/2)" "fox" "scatter product 0 fox link"
is "$(ls scatterrun/output_links/t.out_f/1/2)" "fox" "scatter product 1 fox link"
is "$(find scatterrun/output_links -type l | wc -l)" "0" "scatter product hardlinks"
is "$(find scatterrun/ | xargs -n 1 stat -c %U | sort | uniq)" "$(whoami)" "scatter files all owned by $(whoami)"
cmp -s scatter_echo.wdl scatterrun/wdl/scatter_echo.wdl
is "$?" "0" "copy_source scatter_echo.wdl"
cmp -s echo_task.wdl scatterrun/wdl/echo_task.wdl
is "$?" "0" "copy_source echo_task.wdl"

cat << 'EOF' > failer2000.wdl
version 1.0

workflow failer2000 {
    call failer {
        input:
            message = "this is the end, beautiful friend"
    }
}

task failer {
    input {
        String message
        Int retries = 2
    }
    File messagefile = write_lines([message])
    command {
        cat "~{messagefile}" | tee iwuzhere > /dev/stderr
        exit 42
    }
    runtime {
        maxRetries: 2
    }
}
EOF
$miniwdl run --dir failer2000/. --verbose --error-json failer2000.wdl > failer2000.stdout 2> failer2000.log.txt
is "$?" "42" "failer2000"
is "$(jq '.cause.exit_status' failer2000.stdout)" "42" "workflow error stdout"
is "$(jq '.cause.exit_status' failer2000/error.json)" "42" "workflow error.json"
is "$(jq '.cause.exit_status' failer2000/call-failer/error.json)" "42" "task error.json"
grep -q beautiful failer2000/call-failer/stderr.txt
is "$?" "0" "failer2000 stderr"
grep -q beautiful failer2000.log.txt
is "$?" "0" "failer2000 stderr logged"
grep -q beautiful failer2000/call-failer/failed_tries/1/stderr.txt
is "$?" "0" "failer2000 try1 stderr"
grep -q beautiful failer2000/call-failer/failed_tries/1/work/iwuzhere
is "$?" "0" "failer2000 try1 iwuzhere"


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

cat << 'EOF' > mv_input_file.wdl
version 1.0
task mv_input_file {
    input {
        File file
    }
    command {
        mv "~{file}" xxx
    }
    output {
        File xxx = "xxx"
    }
}
EOF

$miniwdl run --copy-input-files mv_input_file.wdl file=quick
is "$?" "0" "copy input files"

cat << 'EOF' > uri_inputs.json
{"my_workflow.files": ["https://google.com/robots.txt", "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/master/tests/alyssa_ben.txt"]}
EOF
cat << 'EOF' > localize_me.wdl
version 1.0
workflow my_workflow {
    input {
        Array[File] files
    }
}
EOF
MINIWDL__DOWNLOAD_CACHE__PUT=true MINIWDL__DOWNLOAD_CACHE__DIR="${DN}/test_localize/cache" MINIWDL__DOWNLOAD_CACHE__ENABLE_PATTERNS='["*"]' MINIWDL__DOWNLOAD_CACHE__DISABLE_PATTERNS='["*/alyssa_ben.txt"]' \
    $miniwdl localize localize_me.wdl uri_inputs.json --uri gs://gcp-public-data-landsat/LC08/01/044/034/LC08_L1GT_044034_20130330_20170310_01_T2/LC08_L1GT_044034_20130330_20170310_01_T2_MTL.txt > localize.stdout
is "$?" "0" "localize exit code"
is "$(find "${DN}/test_localize/cache/files" -type f | wc -l)" "2" "localize cache"
