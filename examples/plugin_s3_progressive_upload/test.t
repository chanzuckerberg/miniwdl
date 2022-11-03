#!/bin/bash
#
# bash-tap tests for miniwdl_s3_progressive_upload plugin
#
# Prerequisites:
# 1) Recursively cloned miniwdl repo (with submodules)
# 2) AWS CLI configured
# 3) S3 bucket for testing
#
# To run:
#   MINIWDL__S3_PROGRESSIVE_UPLOAD__FOLDER=s3://YourBucket/TestFolder prove -v examples/plugin_s3_progressive_upload/test.t
set -o pipefail

if [[ -z $MINIWDL__S3_PROGRESSIVE_UPLOAD__FOLDER ]]; then
    >&2 echo -e "\n\nSet MINIWDL__S3_PROGRESSIVE_UPLOAD__FOLDER and try again\n\n"
    exit 1
fi

cd "$(dirname $0)/../.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"
pip3 install examples/plugin_s3_progressive_upload

if [[ -z $TMPDIR ]]; then
    TMPDIR=/tmp
fi
DN=$(mktemp -d "${TMPDIR}/miniwdl_check_tests_XXXXXX")
DN=$(realpath "$DN")
cd $DN

plan tests 6

cat << 'EOF' > inner.wdl
version development

workflow inner {
    input {
        Array[String] names
        Int num
    }
    scatter (name in names) {
        call hello {
            input:
            name = name,
            num = num
        }
    }
    call make_dir {
        input:
        messages = hello.message
    }
    output {
        Array[File] messages = hello.message
        Directory outdir = make_dir.outdir
    }
}

task hello {
    input {
        String name
        Int num
    }
    command <<<
        echo 'Hello, ~{name} ~{num}!' > message.txt
        >&2 cat message.txt
    >>>
    output {
        File message = "message.txt"
    }
}

task make_dir {
    input {
        Array[File] messages
    }
    command <<<
        mkdir MyDirectory
        xargs -i cp {} MyDirectory/ < '~{write_lines(messages)}'
    >>>
    output {
        Directory outdir = "MyDirectory"
    }
}
EOF

cat << 'EOF' > outer.wdl
version development
import 'inner.wdl'

workflow outer {
    input {
        Array[String] names
    }
    scatter (num in range(2)) {
        call inner.inner {
            input:
            names = names,
            num = num
        }
        scatter (message in inner.messages) {
            String msgstr = read_string(message)
        }
    }
    output {
        Array[File] messages = flatten(inner.messages)
        Array[String] messages2 = flatten(msgstr)
        # FIXME: the plugin currently doesn't handle files that are generated at the workflow (not
        #        task) level such as the following. It just outputs a warning. We can handle this
        #        in the future.
        File messages3 = write_lines(messages2)
        # FIXME: also doesn't yet handle Directory
        Array[Directory] outdirs = inner.outdir
    }
}
EOF

$miniwdl run outer.wdl names=Alice names=Bob names=Carol --verbose | tee miniwdl.stdout.txt
is $? "0" "run"

DEST="$(jq -r .dir miniwdl.stdout.txt)/outputs.s3folder.txt"
DEST="$(cat $DEST)"

aws s3 ls --recursive $DEST
aws s3 cp "$DEST/outputs.s3.json" - | tee outputs.s3.json
is $? "0" "outputs.s3.json"
cmp "$(jq -r .dir miniwdl.stdout.txt)/outputs.s3.json" outputs.s3.json
is $? "0" "outputs.s3.json local"

grep -q "$DEST/inner-1/hello-1/message/message.txt" outputs.s3.json
is $? "0" "outputs.s3.json content"

aws s3 cp "$DEST/inner-1/hello-1/message/message.txt" - | grep -q "Hello, Bob 1!"
is $? "0" "Hello, Bob 1!"

aws s3 cp "$DEST/inner-1/hello-1/stderr.txt" - | grep -q "Hello, Bob 1!"
is $? "0" "Hello, Bob 1! (stderr)"
