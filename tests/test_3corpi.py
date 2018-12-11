import unittest, inspect, subprocess, tempfile, os, glob
from .context import WDL

def test_corpus(dir, path=[], blacklist=[], expected_lint={}, check_quant=True):
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
                    # run the 'miniwdl check' command-line tool
                    cmd = ['check']
                    for dn in gpath:
                        cmd.append('--path')
                        cmd.append(dn)
                    if check_quant is False:
                        cmd.append("--no-quant-check")
                    cmd.append(fn)
                    print()
                    WDL.CLI.main(cmd)

                    # also load & lint the document to verify the lint count
                    doc = WDL.load(fn, path=gpath, check_quant=check_quant)
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
    expected_lint={'StringCoercion': 3, 'UnusedDeclaration': 1}
)
class HCAskylab_task(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/HumanCellAtlas/skylab/pipelines/**"],
    path=[["test_corpi/HumanCellAtlas/skylab/library/tasks"]],
    expected_lint={'UnusedDeclaration': 15, 'NameCollision': 1, 'StringCoercion': 3}
)
class HCAskylab_workflow(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/gatk-workflows/five-dollar-genome-analysis-pipeline/**"],
    # path is needed expressly here as a wdl imports from "./tasks_pipelines/import.wdl"
    # when it itself is in ./tasks_pipelines
    path=[["test_corpi/gatk-workflows/five-dollar-genome-analysis-pipeline"]],
    blacklist=['fc_germline_single_sample_workflow'], # uses URI import
    expected_lint={'StringCoercion': 11, 'UnusedDeclaration': 4, 'NameCollision': 2, 'ArrayCoercion': 4, 'UnusedCall': 1}
)
class GATK_five_dollar(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/gatk-workflows/gatk4-germline-snps-indels/**"],
    expected_lint={'UnusedDeclaration': 3, 'StringCoercion': 15}
)
class gatk4_germline_snps_indels(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/gatk-workflows/gatk4-somatic-snvs-indels/**"],
    expected_lint={'OptionalCoercion': 50, 'UnusedDeclaration': 29, 'ForwardReference': 6, 'NonemptyArrayCoercion': 4, 'StringCoercion': 20},
    check_quant=False,
)
class gatk4_somatic_snvs_indels(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/gatk-workflows/gatk4-cnn-variant-filter/**"],
    expected_lint={'UnusedDeclaration': 21, 'OptionalCoercion': 23, 'StringCoercion': 3, 'UnusedCall': 1},
    check_quant=False,
)
class gatk4_cnn_variant_filter(unittest.TestCase):
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
    expected_lint={'IncompleteCall': 30, 'UnusedDeclaration': 3, 'NameCollision': 4},
)
class GTEx(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/DataBiosphere/topmed-workflows/**"],
    # need URI import
    blacklist=['CRAM_md5sum_checker_wrapper', 'checker-workflow-wrapping-alignment-workflow',
                'topmed_freeze3_calling', 'topmed_freeze3_calling_checker', 'u_of_michigan_aligner_checker'],
    expected_lint={'StringCoercion': 26, 'UnusedDeclaration': 74, 'OptionalCoercion': 1},
    check_quant=False,
)
class TOPMed(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/broadinstitute/viral-ngs/pipes/WDL/workflows"],
    path=[["test_corpi/broadinstitute/viral-ngs/pipes/WDL/workflows/tasks"]],
    expected_lint={'UnusedDeclaration': 23, 'NameCollision': 9, 'IncompleteCall': 44, 'UnusedImport': 1},
)
class ViralNGS(unittest.TestCase):
    pass

@test_corpus(
     ["test_corpi/ENCODE-DCC/chip-seq-pipeline2/**"],
     expected_lint={'StringCoercion': 224, 'NameCollision': 16, 'ArrayCoercion': 64, 'OptionalCoercion': 64},
)
class ENCODE_ChIPseq(unittest.TestCase):
    pass

@test_corpus(
     ["test_corpi/ENCODE-DCC/atac-seq-pipeline/**"],
     expected_lint={'StringCoercion': 182, 'ArrayCoercion': 41, 'OptionalCoercion': 26, 'UnusedCall': 13}
)
class ENCODE_ATACseq(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/ENCODE-DCC/rna-seq-pipeline/**"],
    expected_lint={'StringCoercion': 6, 'UnusedDeclaration': 3, 'IncompleteCall': 3}
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
        # circular imports
        "foo_toplevel", "foo_if_flag",
        # double quantifier
        "conditionals_base"
    ],
    expected_lint={'UnusedDeclaration': 22, 'UnusedCall': 15, 'NameCollision': 2, 'OptionalCoercion': 1},
    check_quant=False,
)
class dxWDL(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/contrived/**"],
    expected_lint={'UnusedImport': 2, 'ArrayCoercion': 2, 'NameCollision': 13, 'OptionalCoercion': 5, 'StringCoercion': 1, 'NonemptyArrayCoercion': 1, 'IncompleteCall': 1, 'UnusedDeclaration': 1},
    blacklist=["check_quant"],
)
class Contrived(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/contrived/**"],
    expected_lint={'UnusedImport': 2, 'ArrayCoercion': 2, 'NameCollision': 13, 'OptionalCoercion': 6, 'UnusedDeclaration': 2, 'StringCoercion': 1, 'NonemptyArrayCoercion': 1, 'IncompleteCall': 1},
    check_quant=False,
)
class Contrived2(unittest.TestCase):
    pass
