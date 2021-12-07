#!/bin/bash
# bash-tap tests for the `miniwdl run` command-line interface
set -o pipefail

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

plan tests 81

$miniwdl run_self_test
is "$?" "0" "run_self_test"

if [[ -z $TMPDIR ]]; then
    TMPDIR=/tmp
fi
DN=$(mktemp -d "${TMPDIR}/miniwdl_runner_tests_XXXXXX")
DN=$(realpath "$DN")
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
        Array[String]? o_a_s = ["zzz"]
    }

    command {
        echo fox > fox
    }

    output {
        Int out_i = i
        Array[String]+ out_s = flatten([[s],a_s,select_all([o_a_s])])
        Array[File]+ out_f = flatten([[f],a_f,select_all([o_f]),["fox"]])
    }
}
EOF
touch quick brown fox

$miniwdl run --dir taskrun/. echo_task.wdl s=foo i=42 f= quick a_s=bar a_f=brown --none o_a_s | tee stdout
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
is "$(ls taskrun/out/out_f/2)" "fox" "task product fox link"

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
is "$(ls workflowrun/out/t.out_f/2)" "fox" "workflow product fox link"
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
is "$(ls scatterrun/out/t.out_f/0/2)" "fox" "scatter product 0 fox link"
is "$(ls scatterrun/out/t.out_f/1/2)" "fox" "scatter product 1 fox link"
is "$(find scatterrun/out -type l | wc -l | tr -d ' ')" "0" "scatter product hardlinks"
# if the following stat fails on macOS, ensure the GNU coreutils version of stat is used
is "$(find scatterrun/ | xargs -n 1 stat -c %u | sort | uniq)" "$(id -u)" "scatter files all owned by $(whoami)"
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
$miniwdl run --dir failer2000/. --verbose --error-json failer2000.wdl -o failer2000.stdout 2> failer2000.log.txt
is "$?" "42" "failer2000"
is "$(jq '.cause.exit_status' failer2000.stdout)" "42" "workflow error stdout"
is "$(jq '.cause.exit_status' failer2000/error.json)" "42" "workflow error.json"
is "$(jq '.cause.exit_status' failer2000/call-failer/error.json)" "42" "task error.json"
is `basename "$(jq -r '.cause.stderr_file' failer2000/error.json)"` "stderr3.txt" "error.json stderr.txt"
grep -q beautiful failer2000/call-failer/stderr.txt
is "$?" "0" "failer2000 try1 stderr"
grep -q beautiful failer2000/call-failer/work/iwuzhere
is "$?" "0" "failer2000 try1 iwuzhere"
grep -q beautiful failer2000.log.txt
is "$?" "0" "failer2000 stderr logged"
grep -q beautiful failer2000/call-failer/stderr3.txt
is "$?" "0" "failer2000 try3 stderr"
grep -q beautiful failer2000/call-failer/work3/iwuzhere
is "$?" "0" "failer2000 try3 iwuzhere"


cat << 'EOF' > multitask.wdl
version development
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
        cp /etc/issue issue
    }
    output {
        String msg = read_string(stdout())
        File issue = "issue"
    }
}
EOF

$miniwdl run multitask.wdl runtime.docker=ubuntu:20.10 --task second
is "$?" "0" "multitask"
is "$(jq -r '.["second.msg"]' _LAST/outputs.json)" "two" "multitask stdout & _LAST"
grep -q 20.10 _LAST/out/issue/issue
is "$?" "0" "override runtime.docker"

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
is "$(basename `jq -r '.["mv_input_file.xxx"]' _LAST/outputs.json`)" "xxx" "updated _LAST"

cat << 'EOF' > dir_io.wdl
version development
workflow w {
    input {
        Directory d
    }
    call t {
        input:
        d = d
    }
    output {
        Int dsz = round(size(t.files))
        File issue = t.issue
    }
}
task t {
    input {
        Directory d
    }
    command <<<
        cp /etc/issue issue
        mkdir outdir
        find ~{d} -type f | xargs -i{} cp {} outdir/
    >>>
    output {
        Array[File] files = glob("outdir/*")
        File issue = "issue"
    }
}
EOF

mkdir -p indir/subdir
echo alice > indir/alice.txt
echo bob > indir/subdir/bob.txt
$miniwdl run dir_io.wdl d=indir t.runtime.docker=ubuntu:20.10
is "$?" "0" "directory input"
is `jq -r '.["w.dsz"]' _LAST/outputs.json` "10" "use of directory input"
grep -q 20.10 _LAST/out/issue/issue
is "$?" "0" "override t.runtime.docker"

cat << 'EOF' > uri_inputs.json
{
    "my_workflow.files": ["https://google.com/robots.txt", "https://raw.githubusercontent.com/chanzuckerberg/miniwdl/main/tests/alyssa_ben.txt"],
    "my_workflow.directories": ["s3://1000genomes/phase3/integrated_sv_map/supporting/breakpoints/"]
}
EOF
cat << 'EOF' > localize_me.wdl
version development
workflow my_workflow {
    input {
        Array[File] files
        Array[Directory] directories
    }
}
EOF
MINIWDL__DOWNLOAD_CACHE__PUT=true MINIWDL__DOWNLOAD_CACHE__DIR="${DN}/test_localize/cache" MINIWDL__DOWNLOAD_CACHE__ENABLE_PATTERNS='["*"]' MINIWDL__DOWNLOAD_CACHE__DISABLE_PATTERNS='["*/alyssa_ben.txt"]' \
    $miniwdl localize localize_me.wdl uri_inputs.json --file gs://gcp-public-data-landsat/LC08/01/044/034/LC08_L1GT_044034_20130330_20170310_01_T2/LC08_L1GT_044034_20130330_20170310_01_T2_MTL.txt --verbose > localize.stdout
is "$?" "0" "localize exit code"
is "$(find "${DN}/test_localize/cache/files" -type f | wc -l | tr -d ' ')" "2" "localize cache files"
is "$(find "${DN}/test_localize/cache/dirs" -type f | wc -l | tr -d ' ')" "3" "localize cache dirs"  # two files in downloaded directory + flock file


# test task call caching --
cat << 'EOF' > call_cache.wdl
version development
workflow w {
    input {
        Int denom1
        Int denom2
        File file_in
    }
    call t as t1 {
        input:
            file_in = file_in, denominator = denom1
    }
    call t as t2 {
        input:
            file_in = t1.file_out, denominator = denom2
    }
}
task t {
    input {
        File file_in
        Int denominator
    }
    command {
        cat ~{file_in} | wc -l > line_count.txt
    }
    output {
        File file_out = "line_count.txt"
        Int quotient = read_int("line_count.txt") / denominator
    }
}
EOF
export MINIWDL__CALL_CACHE__PUT=true
export MINIWDL__CALL_CACHE__GET=true
export MINIWDL__CALL_CACHE__DIR="${DN}/test_call_cache/cache"
# t1 runs, t2 fails:
$miniwdl run call_cache.wdl file_in=call_cache.wdl denom1=1 denom2=0
is "$?" "2" "intended divide by zero"
test -d _LAST/call-t1/work
is "$?" "0" "call-t1 ran"
test -d _LAST/call-t2/work
is "$?" "0" "call-t2 ran"
# repeat with adjusted t2, see t1 reused
$miniwdl run call_cache.wdl file_in=call_cache.wdl denom1=1 denom2=1 --verbose
is "$?" "0" "call-t2 succeeded"
test -d _LAST/call-t1/work
is "$?" "1" "call-t1 was cached"
test -d _LAST/call-t2/work
is "$?" "0" "call-t2 ran"
cached_file=$(jq -r '.["w.t1.file_out"]' _LAST/outputs.json)
cached_file=$(realpath "$cached_file")
test -f "$cached_file"
is "$?" "0" "$cached_file"
# repeat again, see both reused
$miniwdl run call_cache.wdl file_in=call_cache.wdl denom1=1 denom2=1
test -d _LAST/call-t1/work
is "$?" "1" "call-t1 was cached"
test -d _LAST/call-t2/work
is "$?" "1" "call-t2 was cached"
# touch intermediate file & see cache invalidated
touch "$cached_file"
$miniwdl run call_cache.wdl file_in=call_cache.wdl denom1=1 denom2=1 --verbose
test -d _LAST/call-t1/work
is "$?" "0" "call-t1 ran"
test -d _LAST/call-t2/work
is "$?" "0" "call-t2 ran"
# check cache works with URI
$miniwdl run call_cache.wdl file_in=https://raw.githubusercontent.com/chanzuckerberg/miniwdl/main/tests/alyssa_ben.txt denom1=1 denom2=1 --verbose
is "$?" 0
$miniwdl run call_cache.wdl file_in=https://raw.githubusercontent.com/chanzuckerberg/miniwdl/main/tests/alyssa_ben.txt denom1=1 denom2=1 --verbose
is "$?" 0
test -d _LAST/call-t1/work
is "$?" "1" "call-t1 ran"

# test "fail-slow"
cat << 'EOF' > fail_slow.wdl
version development
workflow w {
    call t as succeeder {
        input:
        wait = 10,
        fail = false
    }
    call t as failer {
        input:
        wait = 5,
        fail = true
    }
}
task t {
    input {
        Int wait
        Boolean fail
    }
    command {
        sleep ~{wait}
        if [[ '~{fail}' == 'true' ]]; then
            exit 1
        fi
    }
    output {
        Int result = 42
    }
}
EOF
MINIWDL__SCHEDULER__FAIL_FAST=false $miniwdl run fail_slow.wdl
is "$?" "1" "fail-slow"
test -f _LAST/call-succeeder/outputs.json
is "$?" "0" "fail-slow -- in-progress task allowed to succeed"

# test --no-outside-imports
cat << 'EOF' > outside.wdl
version 1.1
task hello {
    command {
        echo "Hello from outside!"
    }
}
EOF
mkdir inside
cat << 'EOF' > inside/inside.wdl
version 1.1
import "../outside.wdl"
workflow w {
    call outside.hello
}
EOF
$miniwdl run inside/inside.wdl
is "$?" "0" "outside import allowed"
$miniwdl run inside/inside.wdl --no-outside-imports
is "$?" "2" "outside import denied"

# test --env
cat << 'EOF' > env.wdl
version development
task t {
    input {}
    command <<<
        echo "${WWW}/${XXX}/${YYY}/${ZZZ}"
    >>>
    output {
        String out = read_string(stdout())
    }
    runtime {
        docker: "ubuntu:20.04"
    }
}
EOF
XXX=quick YYY=not $miniwdl run env.wdl --env WWW --env XXX --env YYY= --env "ZZZ=brown fox" -o env_out.json
is "$?" "0" "--env succeeds"
is "$(jq -r '.outputs["t.out"]' env_out.json)" "/quick//brown fox" "--env correct"

# test flock of top-level workflow.log whilst workflow is running
cat << 'EOF' > test_log_lock.wdl
version development
workflow w {
    call sleeper
}
task sleeper {
    input {}
    command <<<
        sleep 3
    >>>
    output {
    }
    runtime {
        docker: "ubuntu:20.04"
    }
}
EOF
$miniwdl run test_log_lock.wdl --dir test_log_lock/. &
sleep 2
flock -nx -E 142 test_log_lock/workflow.log echo
is "$?" "142" "workflow.log is flocked during run"
