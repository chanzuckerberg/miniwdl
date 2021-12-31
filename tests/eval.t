#!/bin/bash
# bash-tap tests for the `miniwdl eval` command-line interface
set -o pipefail

cd "$(dirname $0)/.."
SOURCE_DIR="$(pwd)"

BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

plan tests 7

$miniwdl eval 'sub("Eva Ator"," "," Lu ")' > /tmp/miniwdl_eval.test.txt
is "$?" "0"
is "$(cat /tmp/miniwdl_eval.test.txt)" '"Eva Lu Ator"'

$miniwdl eval -t 'Int n = 42' 'Array[Int] a = [n, n/2]' 'a[1]' > /tmp/miniwdl_eval.test.txt
is "$?" "0"
is "$(head -n 1 /tmp/miniwdl_eval.test.txt)" "Int"
is "$(tail -n 1 /tmp/miniwdl_eval.test.txt)" "21"

$miniwdl eval 'Map[String,Array[String]] m = {"names":["Eva","Lu","Ator"]}' 'm["names"]' | jq -r '.[1]' > /tmp/miniwdl_eval.test.txt
is "$?" "0"
is "$(cat /tmp/miniwdl_eval.test.txt)" "Lu"
