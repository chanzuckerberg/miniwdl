import unittest, inspect, subprocess, tempfile, os, glob
from .context import WDL

def test_corpus(test_klass, prefix, dir, path=[], blacklist=[]):
    files = glob.glob(os.path.join(*(dir + ['*.wdl'])), recursive=True)
    assert len(files) > 0, "{} test corpus missing from {}; please `git submodule update --init --recursive`".format(prefix, os.path.join(*dir))
    gpath = []
    for p in path:
        gpath = gpath + glob.glob(os.path.join(*p), recursive=True)
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

# download and extract a zip file with a corpus of WDL documents; load each one
def test_corpus_zip(test_klass, prefix, zip_url, dir=['**'], path=[], blacklist=[]):
    tdn = tempfile.mkdtemp(prefix='miniwdl_test_'+prefix+"_")
    subprocess.check_call(['wget', '-q', '-O', 'corpus.zip', zip_url], cwd=tdn)
    subprocess.check_call(['unzip', '-q', 'corpus.zip'], cwd=tdn)
    return test_corpus(test_klass, prefix, [tdn] + dir, [[tdn] + p for p in path], blacklist)

class TestHCAskylab(unittest.TestCase):
    pass
test_corpus(TestHCAskylab, "HCAskylab_task", ["test_corpi/HumanCellAtlas/skylab/library/tasks/**"])
test_corpus(TestHCAskylab, "HCAskylab_workflow", ["test_corpi/HumanCellAtlas/skylab/pipelines/**"],
            path=[["test_corpi/HumanCellAtlas/skylab/library/tasks"]])

class TestGATK(unittest.TestCase):
    pass
test_corpus(TestGATK, "GATK_five_dollar", ["test_corpi/gatk-workflows/five-dollar-genome-analysis-pipeline/**"],
            path=[["test_corpi/gatk-workflows/five-dollar-genome-analysis-pipeline"]],
            blacklist=['fc_germline_single_sample_workflow'])
test_corpus(TestGATK, "gatk4_germline_snps_indels", ["test_corpi/gatk-workflows/gatk4-germline-snps-indels/**"],
            # TODO: support pre-1.0 style of workflow outputs (identifiers and wildcards)
            # https://github.com/gatk-workflows/gatk4-germline-snps-indels/blob/b9bbbdcfca7ece0d011ac1225ce6818b33720f48/joint-discovery-gatk4-local.wdl#L345
            # also needed for the CNN variant filter repo.
            blacklist=['joint-discovery-gatk4-local', 'joint-discovery-gatk4'])
test_corpus(TestGATK, "broad_prod_wgs", ["test_corpi/gatk-workflows/broad-prod-wgs-germline-snps-indels/**"],
            blacklist=['JointGenotypingWf'])
# TODO: support out-of-order use of artifact_modes in https://github.com/gatk-workflows/gatk4-somatic-snvs-indels/blob/0a82bedcedd2a2176ccced7cc2ed700e37a025f5/mutect2.wdl#L90
#test_corpus_zip(TestGATK, "gatk4_somatic_snvs_indels",
#                'https://github.com/gatk-workflows/gatk4-somatic-snvs-indels/archive/0a82bed.zip')

class TestGTEx(unittest.TestCase):
    pass
test_corpus(TestGTEx, "GTEx", ["test_corpi/broadinstitute/gtex-pipeline/**"],
            # need URI import
            blacklist=["rnaseq_pipeline_bam","rnaseq_pipeline_fastq"])

class TestTOPMed(unittest.TestCase):
    pass
test_corpus(TestTOPMed, "TOPMed", ["test_corpi/DataBiosphere/topmed-workflows/**"],
            # need URI import
            blacklist=['CRAM_md5sum_checker_wrapper', 'checker-workflow-wrapping-alignment-workflow',
                       'topmed_freeze3_calling', 'topmed_freeze3_calling_checker', 'u_of_michigan_aligner_checker'])

class TestViralNGS(unittest.TestCase):
    pass
test_corpus(TestViralNGS, "ViralNGS", ["test_corpi/broadinstitute/viral-ngs/pipes/WDL/workflows"],
            path=[["test_corpi/broadinstitute/viral-ngs/pipes/WDL/workflows/tasks"]])

class TestENCODE(unittest.TestCase):
    pass
test_corpus(TestENCODE, "ENCODE_ChIPseq", ["test_corpi/ENCODE-DCC/chip-seq-pipeline2/**"])
test_corpus(TestENCODE, "ENCODE_ATACseq", ["test_corpi/ENCODE-DCC/atac-seq-pipeline/**"])
test_corpus(TestENCODE, "ENCODE_RNAseq", ["test_corpi/ENCODE-DCC/rna-seq-pipeline/**"])
test_corpus(TestENCODE, "ENCODE_WGBS", ["test_corpi/ENCODE-DCC/wgbs-pipeline/**"])

class TestDxWDL(unittest.TestCase):
    pass
test_corpus(TestDxWDL, "dxWDL", ["test_corpi/dnanexus/dxWDL/test/**"],
            blacklist=[
                # library_math and docs that import it use Object
                "cast","complex","decl_mid_wf","dict","library_math","math","math2","optionals","toplevel_calls","trivial","trivial2",
                # use dnanexus extensions
                "call_native", "call_native_app",
                # pre-1.0 style outputs
                "movie", "foo_toplevel", "foo_if_flag", "foo",
                # double quantifier
                "conditionals_base"
            ])
