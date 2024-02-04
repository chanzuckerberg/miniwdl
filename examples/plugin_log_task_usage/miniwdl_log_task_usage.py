"""
miniwdl plugin instrumenting each task container to log its own CPU & memory resource usage
periodically. The logs are written to the task's standard error stream, so they'll appear on the
console only with --verbose logging (but are always recorded in each task's stderr.txt).

To enable, install this plugin (`pip3 install .` & confirm listed by `miniwdl --version`) and
set configuration [log_task_usage] period (or the environment variable
MINIWDL__LOG_TASK_USAGE__PERIOD) to the desired logging period in seconds.

YMMV because host OS version & configuration may affect availability of the cgroup v2 counters read
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
    PERIOD_SECS=${1:-10}  # logging period (default 10s)
    if [ ! -f /sys/fs/cgroup/cpu.stat ] || [ ! -f /sys/fs/cgroup/io.stat ]; then
        >&2 echo "miniwdl_log_task_usage unable to report: cgroup v2 counters /sys/fs/cgroup/{cpu,io}.stat not found"
        exit 1
    fi

    # CPU usage in microseconds
    cpu_usecs() {
        awk '/^usage_usec/ {print $2}' /sys/fs/cgroup/cpu.stat
    }

    # total block device I/O
    io_bytes() {
        awk '{
            for (i = 1; i <= NF; i++) {
                if ($i ~ /^rbytes=/) {
                    rbytes += substr($i, index($i, "=") + 1);
                }
                if ($i ~ /^wbytes=/) {
                    wbytes += substr($i, index($i, "=") + 1);
                }
            }
        }
        END {
            print rbytes, wbytes;
        }' /sys/fs/cgroup/io.stat
    }

    T_0=$(date +%s)
    cpu_usecs_0=$(cpu_usecs)
    read rbytes0 wbytes0 < <(io_bytes)
    cpu_usecs_last=$cpu_usecs_0
    rbytes_last=$rbytes0
    wbytes_last=$wbytes0
    t_last=$T_0
    mem_max_bytes=0

    while true; do
        sleep "$PERIOD_SECS"
        t=$(date +%s)
        wall_secs=$(( t - T_0 ))

        cpu_usecs_current=$(cpu_usecs)
        cpu_total_usecs=$(( cpu_usecs_current - cpu_usecs_0 ))
        cpu_period_usecs=$(( cpu_usecs_current - cpu_usecs_last ))

        read rbytes_current wbytes_current < <(io_bytes)
        rbytes_total=$(( rbytes_current - rbytes0 ))
        wbytes_total=$(( wbytes_current - wbytes0 ))
        rbytes_period=$(( rbytes_current - rbytes_last ))
        wbytes_period=$(( wbytes_current - wbytes_last ))

        mem_bytes=$(awk '$1 == "anon" { print $2 }' /sys/fs/cgroup/memory.stat)
        mem_max_bytes=$(( mem_bytes > mem_max_bytes ? mem_bytes : mem_max_bytes ))

        >&2 echo "container usage (last ${PERIOD_SECS}s) :: cpu_pct: $(( 100 * cpu_period_usecs / 1000000 / PERIOD_SECS )), mem_GiB: $(( mem_bytes/1073741824 )), io_read_MiB: $(( rbytes_period/1048576 )), io_write_MiB: $(( wbytes_period/1048576 ))"
        >&2 echo "container usage (total ${wall_secs}s) :: cpu_s: $(( cpu_total_usecs / 1000000 )), mem_max_GiB: $(( mem_max_bytes/1073741824 )), io_read_GiB: $(( rbytes_total/1073741824 )), io_write_GiB: $(( wbytes_total/1073741824 ))"

        cpu_usecs_last=$cpu_usecs_current
        rbytes_last=$rbytes_current
        wbytes_last=$wbytes_current
        t_last=$t
    done
}
"""
