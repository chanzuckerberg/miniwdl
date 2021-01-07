#!/bin/bash
set -eo pipefail

cd "$(dirname $0)/../.."
SOURCE_DIR="$(pwd)"

if [[ -z $TMPDIR ]]; then
    TMPDIR=/tmp
fi
DN=$(mktemp -d "${TMPDIR}/miniwdl_runner_tests_XXXXXX")
DN=$(realpath "$DN")
cd $DN
echo "$DN"

git clone --depth=1 https://github.com/broadinstitute/viral-pipelines.git

export PYTHONPATH="$SOURCE_DIR:$PYTHONPATH"
miniwdl="python3 -m WDL"

BASH_TAP_ROOT="$SOURCE_DIR/tests/bash-tap"
source $SOURCE_DIR/tests/bash-tap/bash-tap-bootstrap
plan tests 3
set +e

SRR_ID=${SRR_ID:-SRR11454608}  # desired SRA run (short read pairs)
wget -O NC_045512.2.fa 'https://www.ncbi.nlm.nih.gov/search/api/sequence/NC_045512.2/?report=fasta'
is "$?" "0" "fetch reference genome"

$miniwdl run viral-pipelines/pipes/WDL/tasks/tasks_ncbi_tools.wdl "SRA_ID=${SRR_ID}" \
    --dir "${SRR_ID}/." --task Fetch_SRA_to_BAM --verbose
is "$?" "0" "fetch SRA run"

$miniwdl run viral-pipelines/pipes/WDL/workflows/assemble_denovo_with_isnv_calling.wdl \
    "reads_unmapped_bam=${SRR_ID}/out/reads_ubam/${SRR_ID}.bam" \
    filter_to_taxon.lastal_db_fasta=NC_045512.2.fa \
    assemble.trim_clip_db=viral-pipelines/test/input/clipDb.fasta \
    scaffold.reference_genome_fasta=NC_045512.2.fa \
    --verbose
is "$?" "0" "pipeline success"
