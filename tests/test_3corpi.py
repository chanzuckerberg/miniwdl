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
                    # load & lint the document to verify the lint count
                    doc = WDL.load(fn, path=gpath, check_quant=check_quant)
                    WDL.Lint.lint(doc)
                    for _, linter, _ in WDL.Lint.collect(doc):
                        test_klass._lint_count[linter] = 1 + test_klass._lint_count.get(linter, 0)
                    print("\n" + os.path.basename(fn))
                    WDL.CLI.outline(doc, 0)

                    # also attempt load with the opposite value of check_quant,
                    # exercising additional code paths
                    try:
                        doc = WDL.load(fn, path=gpath, check_quant=not check_quant)
                    except (WDL.Error.ImportError, WDL.Error.ValidationError, WDL.Error.MultipleValidationErrors):
                        pass
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
    expected_lint={'QuantityCoercion': 54, 'UnusedDeclaration': 29, 'ForwardReference': 6, 'StringCoercion': 20},
    check_quant=False,
)
class gatk4_somatic_snvs_indels(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/gatk-workflows/gatk4-cnn-variant-filter/**"],
    expected_lint={'UnusedDeclaration': 21, 'QuantityCoercion': 23, 'StringCoercion': 3, 'UnusedCall': 1},
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
    expected_lint={'StringCoercion': 26, 'UnusedDeclaration': 74, 'QuantityCoercion': 1},
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
    expected_lint={'StringCoercion': 224, 'NameCollision': 16, 'ArrayCoercion': 64, 'QuantityCoercion': 64},
    check_quant=False,
)
class ENCODE_ChIPseq(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/ENCODE-DCC/atac-seq-pipeline/**"],
    expected_lint={'StringCoercion': 182, 'ArrayCoercion': 41, 'QuantityCoercion': 26, 'UnusedCall': 13},
    check_quant=False,
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
    expected_lint={'UnusedDeclaration': 22, 'UnusedCall': 15, 'NameCollision': 2, 'QuantityCoercion': 1},
    check_quant=False,
)
class dxWDL(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/contrived/**"],
    expected_lint={'UnusedImport': 2, 'NameCollision': 13, 'ArrayCoercion': 2, 'StringCoercion': 2, 'QuantityCoercion': 3, 'UnnecessaryQuantifier': 2, 'UnusedDeclaration': 2, "IncompleteCall": 2},
    blacklist=["check_quant", "incomplete_call"],
)
class Contrived(unittest.TestCase):
    pass

@test_corpus(
    ["test_corpi/contrived/**"],
    expected_lint={'UnusedImport': 4, 'NameCollision': 28, 'ArrayCoercion': 4, 'StringCoercion': 4, 'QuantityCoercion': 8, 'UnnecessaryQuantifier': 4, 'UnusedDeclaration': 7, 'IncompleteCall': 3},
    check_quant=False,
    blacklist=["incomplete_call"],
)
class Contrived2(unittest.TestCase):
    pass
