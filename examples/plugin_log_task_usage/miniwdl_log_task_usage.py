"""
miniwdl plugin instrumenting each task container to log its own CPU & memory resource usage
periodically. The logs are written to the task's standard error stream, so they'll appear on the
console only with --verbose logging (but are always recorded in each task's stderr.txt).

To enable, install this plugin (`pip3 install .` & confirm listed by `miniwdl --version`) and
set configuration [log_task_usage] period (or the environment variable
MINIWDL__LOG_TASK_USAGE__PERIOD) to the desired logging period in seconds.

YMMV because host OS version & configuration may affect availability of the counters read from
pseudo-files under /sys/fs/cgroup
"""

import WDL


def main(cfg, logger, run_id, run_dir, task, **recv):
    # do nothing with inputs
    recv = yield recv

    # inject logger into command script
    if cfg.has_option("log_task_usage", "period"):
        period = cfg["log_task_usage"].get_int("period")
        recv["command"] = _logger_sh + f"log_cpu_mem_in_docker {period} &\n\n" + recv["command"]
    recv = yield recv

    # do nothing with outputs
    yield recv


_logger_sh = r"""
log_cpu_mem_in_docker() {
    set +ex
    PERIOD_SECS=${1:-10}  # logging period (default 10s)
    JIFFIES_PER_SEC=100   # see http://man7.org/linux/man-pages/man7/time.7.html
    T_0=$(date +%s)

    cpu_user_jiffies() {
        cut -f2 -d ' ' /sys/fs/cgroup/cpuacct/cpuacct.stat | head -n 1
    }
    user_jiffies_0=$(cpu_user_jiffies)
    user_jffies_last=$user_jiffies_0
    t_last=$T_0

    while true; do
        sleep "$PERIOD_SECS"
        t=$(date +%s)
        wall_secs=$(( t - T_0 ))

        user_jiffies=$(cpu_user_jiffies)
        user_pct=$(( 100*(user_jiffies - user_jffies_last)/JIFFIES_PER_SEC/(t - t_last) ))
        user_secs=$(( (user_jiffies - user_jiffies_0)/ JIFFIES_PER_SEC ))

        user_jffies_last=$user_jiffies
        t_last=$t

        rss_bytes=$(awk -F ' ' '$1 == "total_rss" { print $2 }'  /sys/fs/cgroup/memory/memory.stat)

        >&2 echo "container usage :: cpu_pct: ${user_pct}, mem_MiB: $(( rss_bytes/1048576 )), cpu_total_s: ${user_secs}, elapsed_s: ${wall_secs}"
    done
}
"""
