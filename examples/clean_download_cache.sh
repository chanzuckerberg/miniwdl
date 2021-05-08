#!/bin/bash
# Maintenance script for the miniwdl File/Directory download cache directory. Evicts (deletes)
# least-recently used items until total space usage is below a threshold given in GB. This script
# should be run under exclusive flock on the file ${DIR}/_miniwdl_flock (which should be created if
# necessary). It also uses flocks on cache items to avoid interfering with ongoing runs using them.

set -euo pipefail

DIR=$1
MAX_GB=$2

if [ -d "$DIR" ]; then
    mkdir -p "${DIR}/ops" "${DIR}/files" "${DIR}/dirs"  # avoid need for existence checks below
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
    used=$(du -scBG "${DIR}/files" "${DIR}/dirs" | tail -n1 | cut -f1 | head -c -2)
    >&2 echo -e "cached files <= ${used}G, limit = ${MAX_GB}G\t${DIR}"
    if [ "$used" -le "$MAX_GB" ]; then
        # success
        exit 0
    fi

    # iterate through cache items in order of increasing atime. (miniwdl explicitly bumps atime
    # when it uses an item)
    eviction=0
    for fn in $( (find "${DIR}/dirs" -mindepth 4 -maxdepth 4 -type d -printf "%A@\t%p\n";
                  find "${DIR}/files" -type f -printf "%A@\t%p\n") | sort -nk1 | cut -f2 ); do
        # If we can get an exclusive flock, rename the file/directory and then delete it.
        # - miniwdl takes shared flocks on any items in use by a running workflow
        # - the rename step ensures cached directories disappear "atomically"
        # - lockfiles are used for dirs, since not all filesystems support directory flocks
        flock_status=0
        flock_fn="$fn"
        if [ -d "$flock_fn" ]; then
            flock_fn="${flock_fn}._miniwdl_flock"
        fi
        deleting_fn="${DIR}/ops/_deleting"
        rm -rf "$deleting_fn"
        (flock -xnE 142 "$flock_fn" mv "$fn" "$deleting_fn" && rm -rf "$deleting_fn") || flock_status=$?
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

    if (( eviction == 0 )); then
        >&2 echo "WARNING: unable to shrink miniwdl download cache ${DIR} to <${MAX_GB}GB, " \
                 "as ${used}GB of cached files are all in use"
        exit
    fi
done
