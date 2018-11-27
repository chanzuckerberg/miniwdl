import unittest, inspect, subprocess, tempfile, os, glob
from .context import WDL

def test_corpus(dir, path=[], blacklist=[], expected_lint={}):
    def decorator(test_klass):

        test_klass._lint_count = {}
        test_klass._expected_lint = expected_lint
        test_klass.tearDownClass = classmethod(check_lint)

        prefix = test_klass.__name__
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
                    # run the 'miniwd check' command-line tool
                    cmd = ['check']
                    for dn in gpath:
                        cmd.append('--path')
                        cmd.append(dn)
                    cmd.append(fn)
                    print()
                    WDL.CLI.main(cmd)

                    # also load & lint the document to verify the lint count
                    doc = WDL.load(fn, path=gpath)
                    WDL.Lint.lint(doc)
                    for _, linter, _ in WDL.Lint.collect(doc):
                        test_klass._lint_count[linter] = 1 + test_klass._lint_count.get(linter, 0)
                setattr(test_klass, name, t)

        return test_klass
    return decorator

def check_lint(cls):
    if cls._lint_count != cls._expected_lint:
        raise Exception("Lint results changed for {}; expected: {} got: {}".format(cls.__name__, str(cls._expected_lint), str(cls._lint_count)))

@test_corpus(
    ["test_corpi/HumanCellAtlas/skylab/library/tasks/**"],
    expected_lint={'StringCoercion': 3}
)
class HCAskylab_task(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/HumanCellAtlas/skylab/pipelines/**"],
    path=[["test_corpi/HumanCellAtlas/skylab/library/tasks"]],
    expected_lint={'UnusedDeclaration': 15, 'CallImportNameCollision': 1, 'StringCoercion': 3}
)
class HCAskylab_workflow(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/gatk-workflows/five-dollar-genome-analysis-pipeline/**"],
    # path is needed expressly here as a wdl imports from "./tasks_pipelines/import.wdl"
    # when it itself is in ./tasks_pipelines
    path=[["test_corpi/gatk-workflows/five-dollar-genome-analysis-pipeline"]],
    blacklist=['fc_germline_single_sample_workflow'],
    expected_lint={'StringCoercion': 11, 'UnusedDeclaration': 4, 'CallImportNameCollision': 2, 'ArrayCoercion': 4, 'UnusedCall': 1}
)
class GATK_five_dollar(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/gatk-workflows/gatk4-germline-snps-indels/**"],
    # TODO: support pre-1.0 style of workflow outputs (identifiers and wildcards)
    # https://github.com/gatk-workflows/gatk4-germline-snps-indels/blob/b9bbbdcfca7ece0d011ac1225ce6818b33720f48/joint-discovery-gatk4-local.wdl#L345
    # also needed for the CNN variant filter repo.
    blacklist=['joint-discovery-gatk4-local', 'joint-discovery-gatk4'],
    expected_lint={'UnusedDeclaration': 1, 'StringCoercion': 2},
)
class gatk4_germline_snps_indels(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/gatk-workflows/gatk4-somatic-snvs-indels/**"],
    expected_lint={'UnusedDeclaration': 30, 'ForwardReference': 6, 'StringCoercion': 20},
)
class gatk4_somatic_snvs_indels(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/gatk-workflows/broad-prod-wgs-germline-snps-indels/**"],
    blacklist=['JointGenotypingWf'],
    expected_lint={'StringCoercion': 48, 'UnusedDeclaration': 10, 'ArrayCoercion': 4, 'UnusedCall': 2}
)
class broad_prod_wgs(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/broadinstitute/gtex-pipeline/**"],
    # need URI import
    blacklist=["rnaseq_pipeline_bam","rnaseq_pipeline_fastq"],
    expected_lint={'IncompleteCall': 30, 'UnusedDeclaration': 3}
)
class GTEx(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/DataBiosphere/topmed-workflows/**"],
    # need URI import
    blacklist=['CRAM_md5sum_checker_wrapper', 'checker-workflow-wrapping-alignment-workflow',
                'topmed_freeze3_calling', 'topmed_freeze3_calling_checker', 'u_of_michigan_aligner_checker'],
    expected_lint={'StringCoercion': 26, 'UnusedDeclaration': 22}
)
class TOPMed(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/broadinstitute/viral-ngs/pipes/WDL/workflows"],
    path=[["test_corpi/broadinstitute/viral-ngs/pipes/WDL/workflows/tasks"]],
    expected_lint={'UnusedDeclaration': 8, 'IncompleteCall': 44, 'UnusedImport': 1}
)
class ViralNGS(unittest.TestCase):
    pass

@test_corpus(
     ["test_corpi/ENCODE-DCC/chip-seq-pipeline2/**"],
     expected_lint={'StringCoercion': 192, 'ArrayCoercion': 64}
)
class ENCODE_ChIPseq(unittest.TestCase):
    pass

@test_corpus(
     ["test_corpi/ENCODE-DCC/atac-seq-pipeline/**"],
     expected_lint={'StringCoercion': 156, 'ArrayCoercion': 41, 'UnusedCall': 1}
)
class ENCODE_ATACseq(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/ENCODE-DCC/rna-seq-pipeline/**"],
    expected_lint={'StringCoercion': 2, 'UnusedDeclaration': 3, 'IncompleteCall': 3}
)
class ENCODE_RNAseq(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/ENCODE-DCC/wgbs-pipeline/**"],
    expected_lint={'StringCoercion': 9, 'UnusedDeclaration': 1}
)
class ENCODE_WGBS(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/dnanexus/dxWDL/test/**"],
    blacklist=[
        # library_math and docs that import it use Object
        "cast","complex","decl_mid_wf","dict","library_math","math","math2","optionals","toplevel_calls","trivial","trivial2",
        # use dnanexus extensions
        "call_native", "call_native_app",
        # pre-1.0 style outputs
        "movie", "foo_toplevel", "foo_if_flag", "foo",
        # double quantifier
        "conditionals_base"
    ],
    expected_lint={'UnusedDeclaration': 14, 'UnusedCall': 15}
)
class dxWDL(unittest.TestCase):
    pass
