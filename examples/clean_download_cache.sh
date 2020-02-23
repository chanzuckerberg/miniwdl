#!/bin/bash
# Maintenance script for the miniwdl file download cache directory. Evicts (deletes) least-recently
# used files until total space usage is below a threshold given in GB; while using flock to avoid
# interfering with any concurrent miniwdl process. Waits if all fails are in use concurrently.

set -euo pipefail

DIR=$1
MAX_GB=$2

if [ -d "$DIR" ]; then
    mkdir -p "${DIR}/ops" "${DIR}/files"  # avoid need for subsequent existence checks
else
    >&2 echo "${DIR} does not exist"
    exit 1
fi

# First, delete any directories under 'ops' more than 2 days old. These are the detritus of
# downloader tasks (logs etc.) and shouldn't consume much space, unless permitted to accumulate
# indefinitely.
find "${DIR}/ops" -mindepth 1 -type d -ctime +2 -exec rm -rf {} +
>&2 echo "download ops <= $(du -sBG "${DIR}/ops")"

# repeat until success
while true ; do
    # measure current space usage
    used=$(du -sBG "${DIR}/files" | cut -f1 | head -c -2)
    >&2 echo -e "cached files <= ${used}G, limit = ${MAX_GB}G\t${DIR}"
    if [ "$used" -le "$MAX_GB" ]; then
        # success
        exit 0
    fi

    # iterate through files in order of increasing atime. (miniwdl explicitly bumps atime when
    # it uses a cached file.)
    eviction=0
    for fn in $(find "$DIR/files" -type f -printf "%A@\t%p\n" | sort -nk1 | cut -f2); do
        # delete this file if we can get an exclusive flock on it. (miniwdl takes shared flocks
        # on any cached files used by a running workflow.)
        flock_status=0
        flock -xnE 142 "$fn" rm "$fn" || flock_status=$?
        if (( flock_status == 0 )); then
            >&2 echo "evicted: $fn"
            eviction=1
            break
        elif (( flock_status != 142 )); then
            >&2 echo "failed to rm: $fn"
            exit "$flock_status"
        else
            >&2 echo "in use: $fn"
        fi
    done

    # if we weren't able to evict anything, pause awhile before continuing
    if (( eviction == 0 )); then
        >&2 echo "all files in use; waiting 30s..."
        sleep 30
    fi
done
