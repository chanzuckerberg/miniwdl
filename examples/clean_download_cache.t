#!/bin/bash
# bash-tap integration test for miniwdl file download cache & accessory clean_download_cache.sh
# run with: prove -v clean_download_cache.t
set -o pipefail

cd "$(dirname "$0")/.."
SOURCE_DIR="$(pwd)"

export BASH_TAP_ROOT="tests/bash-tap"
source tests/bash-tap/bash-tap-bootstrap

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

TEST_ITERATIONS=${TEST_ITERATIONS:-25}
plan tests $(( 2 * TEST_ITERATIONS + 1 ))

DN=$(mktemp -d --tmpdir miniwdl_clean_download_cache_test_XXXXXX)
cd "$DN"
echo "$DN"

# Test workflow to process files
cat << 'EOF' > test.wdl
version 1.0

workflow test {
    input {
        Array[File] files
    }

    scatter (file in files) {
        String filename = basename(file)
        call sha256sum {
            input:
                file = file
        }
    }

    output {
        Array[Pair[String,String]] sha256 = zip(filename,sha256sum.value)
        Float total_bytes = size(files)
    }
}

task sha256sum {
    input {
        File file
    }

    command {
        sha256sum "~{file}" | cut -f1 -d ' '
    }

    output {
        String value = read_string(stdout())
    }
}
EOF
$miniwdl check test.wdl
is "$?" "0" "test.wdl"

# prepare list of 4 URIs with total size slightly >1GB
for chr in $(seq 19 22); do
    echo "https://1000genomes.s3.amazonaws.com/release/20130502/ALL.chr${chr}.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz" >> uris.txt
done

# configure miniwdl cache via environment
export MINIWDL__DOWNLOAD_CACHE__PUT=1
export MINIWDL__DOWNLOAD_CACHE__GET=1
export MINIWDL__DOWNLOAD_CACHE__DIR="${DN}/cache"
export MINIWDL__DOWNLOAD_CACHE__ENABLE_PATTERNS='["*"]'
export MINIWDL__DOWNLOAD_CACHE__DISABLE_PATTERNS='[]'
for i in $(seq "$TEST_ITERATIONS"); do
    # pick two random URIs
    shuf uris.txt | head -n 2 | tee uris.random2.txt
    uri1=$(head -n 1 uris.random2.txt)
    uri2=$(tail -n 1 uris.random2.txt)
    # run test workflow on them
    $miniwdl run test.wdl "files=$uri1" "files=$uri2"
    is "$?" "0" "run $i"

    # run cache cleaner with 1GB target
    ls -lhR --time=atime "${DN}/cache/files/https/1000genomes.s3.amazonaws.com/_release_20130502/" || true
    "${SOURCE_DIR}/examples/clean_download_cache.sh" "${DN}/cache" 1
    is "$?" "0" "clean $i"
done
