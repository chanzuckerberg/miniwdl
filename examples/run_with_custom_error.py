#!/usr/bin/env python3

"""
shell for `miniwdl run` implementing a NON-STANDARD mechanism for tasks to communicate a specific
error message upon failure. Any task may generate, at the end of its standard error stream, one
single line of JSON including the key "wdl_error_message". Following a nonzero exit status from the
task command, if such a line is present then the corresponding value is taken as an error message
for display, and any other JSON contents are retained as additional structured error information.
"""

import sys
import subprocess
import json

# Pass through arguments to miniwdl run, with --error-json to make it print error information to
# standard output (opt-in feature because the standard error logs tend to be more helpful to
# interactive users)
cmd = ["miniwdl", "run"] + sys.argv[1:] + ["--error-json"]
rslt = subprocess.run(cmd, stdout=subprocess.PIPE, check=False)

error_json = None
try:
    # Parse miniwdl's output JSON & check if failure cause was task nonzero exit status.
    # The same JSON can also be found in the workflow run directory, either outputs.json or
    # error.json if the workflow succeeded or failed respectively.
    error_json = json.loads(rslt.stdout)
    assert error_json["cause"]["error"] == "CommandFailed"
    stderr_file = error_json["cause"]["stderr_file"]
    assert stderr_file

    # read last line of failed task's standard error
    tail = subprocess.run(["tail", "-n", "1", stderr_file], stdout=subprocess.PIPE, check=True)
    msg = json.loads(tail.stdout)

    # if wdl_error_message is present, decorate error_json
    error_json["cause"]["message"] = str(msg["wdl_error_message"])
    del msg["wdl_error_message"]
    error_json["cause"]["info"] = msg
except:
    error_json = None

# print decorated error info, if we got that far; otherwise reprint miniwdl's original output
if error_json:
    print(json.dumps(error_json, indent=2))
else:
    sys.stdout.buffer.write(rslt.stdout)

sys.exit(rslt.returncode)

"""
quick test:

cat << 'EOF' > /tmp/run_with_custom_error_test.wdl
version 1.0
task test {
    input {
        Boolean fail
    }
    command <<<
        >&2 echo 'Hello!!!! }{}"'
        if [ "~{fail}" == "true" ]; then
            >&2 echo '{"wdl_error_message": "this is the end, my only friend, the end", "meaning": 420}'
            exit 42
        fi
    >>>
}
EOF
examples/run_with_custom_error.py /tmp/run_with_custom_error_test.wdl fail=false --verbose
examples/run_with_custom_error.py /tmp/run_with_custom_error_test.wdl fail=true --verbose; echo "exit status should be 42: $?"
"""
