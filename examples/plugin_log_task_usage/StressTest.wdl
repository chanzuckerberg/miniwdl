version 1.1
# MINIWDL__LOG_TASK_USAGE__PERIOD=2 miniwdl run examples/plugin_log_task_usage/StressTest.wdl --dir /tmp --verbose
# MINIWDL__LOG_TASK_USAGE__PERIOD=2 miniwdl-aws-submit plugin_log_task_usage/StressTest.wdl --verbose --follow

task StressTest {
    input {
        Int cpu = 4
        Int memory_G = 2
        Int cpu_memory_duration_s = 10
        Int disk_load_G = 4

        String docker = "polinux/stress" # Docker image with stress tool
    }

    command <<<
        set -euxo pipefail

        >&2 ls -l /sys/fs/cgroup

        stress --cpu 4 --vm 1 --vm-bytes ~{memory_G}G --vm-hang 0 --timeout ~{cpu_memory_duration_s}s || true
        dd if=/dev/zero of=testfile bs=1G count=~{disk_load_G}
        sync
        cat testfile > /dev/null &
        sleep 5
    >>>

    runtime {
        docker: docker
        memory: "${memory_G*2}G"
        cpu: cpu
    }

    output {}
}
