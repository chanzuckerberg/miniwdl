"""
miniwdl plugin instrumenting each task container to log its own CPU & memory resource usage
periodically. The logs are written to the task's standard error stream, so they'll appear on the
console only with --verbose logging (but are always recorded in each task's stderr.txt).

To enable, install this plugin (`pip3 install .` & confirm listed by `miniwdl --version`) and
set configuration [log_task_usage] period (or the environment variable
MINIWDL__LOG_TASK_USAGE__PERIOD) to the desired logging period in seconds.

YMMV because host OS version & configuration may affect availability of the cgroup counters read
from pseudo-files under /sys/fs/cgroup
"""

import WDL


def main(cfg, logger, run_id, run_dir, task, **recv):
    # do nothing with inputs
    recv = yield recv

    # inject logger into command script
    if cfg.has_option("log_task_usage", "period"):
        period = cfg["log_task_usage"].get_int("period")
        recv["command"] = _logger_sh + f"_miniwdl_log_task_usage {period} &\n\n" + recv["command"]
    recv = yield recv

    # do nothing with outputs
    yield recv


_logger_sh = r"""
_miniwdl_log_task_usage() {
    set +ex
    local PERIOD_SECS=${1:-10}  # logging period (default 10s)

    # detect whether host provides cgroup v2 or v1, and helper functions to read CPU & memory usage
    # counters from the appropriate pseudo-files
    local cgroup_version=""
    if [ -f /sys/fs/cgroup/cpu.stat ]; then
        cgroup_version=2
    elif [ -f /sys/fs/cgroup/cpuacct/cpuacct.stat ]; then
        cgroup_version=1
    else
        >&2 echo "miniwdl_log_task_usage unable to report: cgroup CPU usage counters not found"
        exit 1
    fi

    cpu_secs() {
        local ans
        if [ $cgroup_version -eq 2 ]; then
            ans=$(awk '/^usage_usec/ {print $2}' /sys/fs/cgroup/cpu.stat)
            echo $(( ans / 1000000 ))
        else
            ans=$(cut -f2 -d ' ' /sys/fs/cgroup/cpuacct/cpuacct.stat | head -n 1)
            echo $(( ans / 100 )) # 100 "jiffies" per second
        fi
    }

    mem_bytes() {
        if [ $cgroup_version -eq 2 ]; then
            awk '$1 == "anon" { print $2 }' /sys/fs/cgroup/memory.stat
        else
            awk -F ' ' '$1 == "total_rss" { print $2 }'  /sys/fs/cgroup/memory/memory.stat
        fi
    }

    local T_0=$(date +%s)
    local t_last=$T_0
    local cpu_secs_0=$(cpu_secs)
    local cpu_secs_last=$cpu_secs_0

    while true; do
        sleep "$PERIOD_SECS"
        local t=$(date +%s)
        local wall_secs=$(( t - T_0 ))

        local cpu_secs_current=$(cpu_secs)
        local cpu_total_secs=$(( cpu_secs_current - cpu_secs_0 ))
        local cpu_period_secs=$(( cpu_secs_current - cpu_secs_last ))

        local mem_bytes_current=$(mem_bytes)

        >&2 echo "container usage :: cpu_pct: $(( 100 * cpu_period_secs / PERIOD_SECS )), mem_MiB: $(( mem_bytes_current/1048576 )), cpu_total_s: ${cpu_total_secs}, elapsed_s: ${wall_secs}"

        cpu_secs_last=$cpu_secs_current
        t_last=$t
    done
}
"""
