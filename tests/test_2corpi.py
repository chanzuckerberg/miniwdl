import unittest, inspect, subprocess, tempfile, os, glob
from .context import WDL

# download and extract a zip file with a corpus of WDL documents; load each one
def test_corpus_zip(test_klass, prefix, zip_url, dir=['**'], path=[], blacklist=[]):
    tdn = tempfile.mkdtemp(prefix='miniwdl_test_'+prefix+"_")
    subprocess.check_call(['wget', '-q', '-O', 'corpus.zip', zip_url], cwd=tdn)
    subprocess.check_call(['unzip', '-q', 'corpus.zip'], cwd=tdn)
    files = glob.glob(os.path.join(*([tdn] + dir + ['*.wdl'])), recursive=True)
    gpath = []
    for p in path:
        gpath = gpath + glob.glob(os.path.join(*([tdn] + p)), recursive=True)
    for fn in files:
        name = os.path.split(fn)[1]
        name = name[:-4]
        if name not in blacklist:
            name = "test_" + prefix + "_" + name.replace('.', '_')
            def t(self, fn=fn):
                cmd = ['check']
                for dn in gpath:
                    cmd.append('--path')
                    cmd.append(dn)
                cmd.append(fn)
                print()
                WDL.CLI.main(cmd)
            setattr(test_klass, name, t)

class TestHCAskylab(unittest.TestCase):
    pass
test_corpus_zip(TestHCAskylab, "HCAskylab_task",
                'https://github.com/HumanCellAtlas/skylab/archive/face906.zip', ['skylab-*', 'library', 'tasks'])
test_corpus_zip(TestHCAskylab, "HCAskylab_workflow",
                'https://github.com/HumanCellAtlas/skylab/archive/face906.zip', ['skylab-*', 'pipelines', '**'], [['skylab-*', 'library', 'tasks']])

class TestGATK(unittest.TestCase):
    pass
test_corpus_zip(TestGATK, "GATK_five_dollar",
                'https://github.com/gatk-workflows/five-dollar-genome-analysis-pipeline/archive/89f11be.zip',
                blacklist=['fc_germline_single_sample_workflow', 'split_large_readgroup', 'unmapped_bam_to_aligned_bam'])
test_corpus_zip(TestGATK, "gatk4_germline_snps_indels",
                'https://github.com/gatk-workflows/gatk4-germline-snps-indels/archive/b9bbbdc.zip',
                # TODO: support pre-1.0 style of workflow outputs (identifiers and wildcards)
                # https://github.com/gatk-workflows/gatk4-germline-snps-indels/blob/b9bbbdcfca7ece0d011ac1225ce6818b33720f48/joint-discovery-gatk4-local.wdl#L345
                # also needed for the CNN variant filter repo.
                blacklist=['joint-discovery-gatk4-local', 'joint-discovery-gatk4'])
# TODO: support out-of-order use of artifact_modes in https://github.com/gatk-workflows/gatk4-somatic-snvs-indels/blob/0a82bedcedd2a2176ccced7cc2ed700e37a025f5/mutect2.wdl#L90
#test_corpus_zip(TestGATK, "gatk4_somatic_snvs_indels",
#                'https://github.com/gatk-workflows/gatk4-somatic-snvs-indels/archive/0a82bed.zip')
test_corpus_zip(TestGATK, "broad_prod_wgs",
                'https://github.com/gatk-workflows/broad-prod-wgs-germline-snps-indels/archive/5585cdf.zip', blacklist=['JointGenotypingWf'])

class TestGTEx(unittest.TestCase):
    pass
test_corpus_zip(TestGTEx, "GTEx",
                'https://github.com/broadinstitute/gtex-pipeline/archive/a228198.zip',
                # need URI import
                blacklist=["rnaseq_pipeline_bam","rnaseq_pipeline_fastq"])

class TestTOPMed(unittest.TestCase):
    pass
test_corpus_zip(TestTOPMed, "TOPMed",
                'https://github.com/DataBiosphere/topmed-workflows/archive/31ba8a7.zip',
                # need urI import
                blacklist=['CRAM_md5sum_checker_wrapper', 'checker-workflow-wrapping-alignment-workflow',
                           'topmed_freeze3_calling', 'topmed_freeze3_calling_checker', 'u_of_michigan_aligner_checker'])

class TestViralNGS(unittest.TestCase):
    pass
test_corpus_zip(TestViralNGS, "ViralNGS",
                'https://github.com/broadinstitute/viral-ngs/archive/v1.21.2.zip',
                ['viral-ngs-*', 'pipes', 'WDL', 'workflows'],
                path=[['viral-ngs-*', 'pipes', 'WDL', 'workflows', 'tasks']])
