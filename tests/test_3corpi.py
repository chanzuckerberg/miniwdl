import unittest, inspect, subprocess, tempfile, os, glob, json, urllib, urllib.request
from .context import WDL
import WDL.Lint


class Lint(unittest.TestCase):
    # test programmatic access to linter results
    # this code should be kept in sync with the example shown in the WDL/Lint.py docstring.
    def test_api(self):
        doc = WDL.load("ZarrUtils.wdl", path=["test_corpi/HumanCellAtlas/skylab/library/tasks"])
        WDL.Lint._shellcheck_available = False
        lint = WDL.Lint.collect(WDL.Lint.lint(doc, descend_imports=False))
        for (pos, lint_class, message, suppressed) in lint:
            assert isinstance(pos, WDL.SourcePosition)
            assert isinstance(lint_class, str) and isinstance(message, str)
            if not suppressed:
                print(
                    json.dumps(
                        {
                            "uri": pos.uri,
                            "abspath": pos.abspath,
                            "line": pos.line,
                            "end_line": pos.end_line,
                            "column": pos.column,
                            "end_column": pos.end_column,
                            "lint": lint_class,
                            "message": message,
                        }
                    )
                )
        self.assertEqual(len(lint), 2)


async def read_source(uri, path, importer_uri):
    if uri.startswith("http:") or uri.startswith("https:"):
        # Note: we should permit web imports only in corpi which are careful to pin a specific and
        # highly-available revision
        fn = os.path.join(tempfile.mkdtemp(prefix="miniwdl_import_uri_"), os.path.basename(uri))
        urllib.request.urlretrieve(uri, filename=fn)
        with open(fn, "r") as infile:
            return WDL.ReadSourceResult(infile.read(), os.path.abspath(fn))
    return await WDL.read_source_default(uri, path, importer_uri)


def wdl_corpus(dir, path=[], blocklist=[], expected_lint={}, check_quant=True):
    def decorator(test_klass):

        test_klass._lint_count = {}
        test_klass._expected_lint = expected_lint
        test_klass.tearDownClass = classmethod(check_lint)

        prefix = test_klass.__name__
        files = glob.glob(os.path.join(*(dir + ["*.wdl"])), recursive=True)
        assert (
            len(files) > 0
        ), "{} test corpus missing from {}; please `git submodule update --init --recursive`".format(
            prefix, os.path.join(*dir)
        )
        gpath = []
        for p in path:
            gpath = gpath + glob.glob(os.path.join(*p), recursive=True)
        for fn in files:
            name = os.path.split(fn)[1]
            name = name[:-4]
            if name not in blocklist:
                name = "test_" + prefix + "_" + name.replace(".", "_")
                while hasattr(test_klass, name):
                    name += "_"

                def t(self, fn=fn):
                    # load & lint the document to verify the lint count
                    try:
                        doc = WDL.load(
                            fn, path=gpath, check_quant=check_quant, read_source=read_source
                        )
                    except Exception as exn:
                        if isinstance(exn, WDL.Error.MultipleValidationErrors):
                            for subexn in exn.exceptions:
                                print(subexn.node.pos)
                        if hasattr(exn, "node"):
                            print(exn.node.pos)
                        raise
                    WDL.Lint.lint(doc)
                    for _, linter, _, suppressed in WDL.Lint.collect(doc):
                        test_klass._lint_count[linter] = 1 + test_klass._lint_count.get(linter, 0)
                        if suppressed:
                            test_klass._lint_count[
                                "_suppressions"
                            ] = 1 + test_klass._lint_count.get("_suppressions", 0)
                    print("\n" + os.path.basename(fn))
                    WDL.CLI.outline(doc, 0, show_called=(doc.workflow is not None))
                    WDL.copy_source(
                        doc, tempfile.mkdtemp(prefix=f"miniwdl_test_copy_source_{prefix}")
                    )

                    if doc.workflow:
                        validate_workflow_graph(doc.workflow)

                    # also attempt load with the opposite value of check_quant,
                    # exercising additional code paths
                    try:
                        doc = WDL.load(
                            fn, path=gpath, check_quant=not check_quant, read_source=read_source
                        )
                    except (
                        WDL.Error.ImportError,
                        WDL.Error.ValidationError,
                        WDL.Error.MultipleValidationErrors,
                    ):
                        pass

                setattr(test_klass, name, t)

        return test_klass

    return decorator


def check_lint(cls):
    if "CommandShellCheck" in cls._lint_count:
        # because we don't control installed shellcheck version
        del cls._lint_count["CommandShellCheck"]
    if cls._lint_count != cls._expected_lint:
        raise Exception(
            "Lint results changed for {}; expected: {} got: {}".format(
                cls.__name__, str(cls._expected_lint), str(cls._lint_count)
            )
        )


def validate_workflow_graph(workflow):
    def visit_section(body, outside_nodes):
        body_nodes = set()
        body_gathers = set()
        for node in body:
            body_nodes.add(node.workflow_node_id)
            if isinstance(node, WDL.WorkflowSection):
                for g in node.gathers.values():
                    body_gathers.add(g.workflow_node_id)
        assert not (body_nodes & body_gathers)
        assert not (outside_nodes & (body_nodes | body_gathers))
        for node in body:
            unk = node.workflow_node_dependencies - (outside_nodes | body_nodes | body_gathers)
            assert not unk, str((node.workflow_node_id, unk))
            if isinstance(node, WDL.WorkflowSection):
                visit_section(
                    node.body, (outside_nodes | body_nodes | body_gathers) - set(node.gathers)
                )

    visit_section(workflow.body, set(inp.workflow_node_id for inp in workflow.inputs or []))


@wdl_corpus(
    ["test_corpi/HumanCellAtlas/skylab/library/tasks/**"],
    expected_lint={"UnusedDeclaration": 2, "UnknownRuntimeKey": 1, "MissingVersion": 26},
)
class HCAskylab_task(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/HumanCellAtlas/skylab/pipelines/**"],
    path=[["test_corpi/HumanCellAtlas/skylab/library/tasks"]],
    expected_lint={
        "UnusedDeclaration": 12,
        "NameCollision": 3,
        "UnknownRuntimeKey": 3,
        "StringCoercion": 1,
        "MixedIndentation": 1,
        "FileCoercion": 1,
        "MissingVersion": 34,
        "UnnecessaryQuantifier": 3,
    },
)
class HCAskylab_workflow(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/gatk-workflows/five-dollar-genome-analysis-pipeline/**"],
    expected_lint={"UnusedDeclaration": 5, "NameCollision": 2, "UnusedImport": 8},
)
class GATK_five_dollar(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/gatk-workflows/gatk4-germline-snps-indels/**"],
    expected_lint={
        "UnusedDeclaration": 3,
        "StringCoercion": 20,
        "FileCoercion": 3,
        "UnknownRuntimeKey": 1,
        "MissingVersion": 4,
    },
)
class gatk4_germline_snps_indels(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/gatk-workflows/gatk4-somatic-snvs-indels/**"],
    expected_lint={
        "OptionalCoercion": 50,
        "NonemptyCoercion": 4,
        "UnusedDeclaration": 29,
        "ForwardReference": 6,
        "StringCoercion": 20,
        "MixedIndentation": 6,
        "MissingVersion": 6,
    },
    check_quant=False,
)
class gatk4_somatic_snvs_indels(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/gatk-workflows/gatk4-cnn-variant-filter/**"],
    expected_lint={
        "UnusedDeclaration": 21,
        "OptionalCoercion": 24,
        "FileCoercion": 8,
        "UnusedCall": 1,
        "StringCoercion": 4,
        "MissingVersion": 6,
    },
    check_quant=False,
)
class gatk4_cnn_variant_filter(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/gatk-workflows/broad-prod-wgs-germline-snps-indels/**"],
    blocklist=["JointGenotypingWf"],
    expected_lint={
        "StringCoercion": 50,
        "UnusedDeclaration": 10,
        "ArrayCoercion": 4,
        "MissingVersion": 2,
    },
    check_quant=False,
)
class broad_prod_wgs(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/broadinstitute/gtex-pipeline/**"],
    # need URI import
    blocklist=["rnaseq_pipeline_bam", "rnaseq_pipeline_fastq"],
    expected_lint={
        "IncompleteCall": 30,
        "UnusedDeclaration": 3,
        "NameCollision": 4,
        "MissingVersion": 23,
    },
)
class GTEx(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/DataBiosphere/topmed-workflows/**"],
    # need URI import
    blocklist=[
        "CRAM_md5sum_checker_wrapper",
        "checker-workflow-wrapping-alignment-workflow",
        "topmed_freeze3_calling",
        "topmed_freeze3_calling_checker",
        "u_of_michigan_aligner_checker",
    ],
    expected_lint={
        "StringCoercion": 27,
        "UnusedDeclaration": 74,
        "OptionalCoercion": 1,
        "MissingVersion": 8,
        "UnnecessaryQuantifier": 1,
    },
    check_quant=False,
)
class TOPMed(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/broadinstitute/viral-ngs/pipes/WDL/workflows"],
    path=[["test_corpi/broadinstitute/viral-ngs/pipes/WDL/workflows/tasks"]],
    expected_lint={
        "UnusedDeclaration": 23,
        "NameCollision": 9,
        "IncompleteCall": 44,
        "UnusedImport": 1,
        "SelectArray": 4,
        "MissingVersion": 62,
        "UnnecessaryQuantifier": 191,
    },
)
class ViralNGS(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/ENCODE-DCC/chip-seq-pipeline2/**"],
    expected_lint={
        "StringCoercion": 208,
        "FileCoercion": 154,
        "NameCollision": 16,
        "OptionalCoercion": 64,
        "MixedIndentation": 32,
        "MissingVersion": 31,
    },
    check_quant=False,
)
class ENCODE_ChIPseq(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/ENCODE-DCC/atac-seq-pipeline/**"],
    expected_lint={
        "UnusedDeclaration": 63,
        "MixedIndentation": 15,
        "OptionalCoercion": 1020,
        "UnusedCall": 45,
        "StringCoercion": 30,
        "FileCoercion": 71,
        "MissingVersion": 29,
    },
    check_quant=False,
)
class ENCODE_ATACseq(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/ENCODE-DCC/rna-seq-pipeline/**"],
    expected_lint={
        "StringCoercion": 2,
        "UnusedDeclaration": 3,
        "IncompleteCall": 3,
        "MissingVersion": 3,
    },
)
class ENCODE_RNAseq(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/ENCODE-DCC/wgbs-pipeline/**"],
    expected_lint={
        "StringCoercion": 9,
        "UnusedDeclaration": 1,
        "MixedIndentation": 1,
        "MissingVersion": 5,
    },
)
class ENCODE_WGBS(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/dnanexus/dxWDL/test/**"],
    blocklist=[
        # output/call name collision (draft-2)
        "conditionals2",
        # decl/output name collision
        "two_levels",
        # use dnanexus extensions
        "call_native",
        "call_native_app",
        "call_native_v1",
    ],
    path=[["test_corpi/dnanexus/dxWDL/test/imports/lib"]],
    expected_lint={
        "UnusedDeclaration": 34,
        "UnusedCall": 16,
        "NameCollision": 2,
        "OptionalCoercion": 3,
        "FileCoercion": 3,
        "StringCoercion": 2,
        "UnnecessaryQuantifier": 1,
        "MissingVersion": 52,
        "UnnecessaryQuantifier": 10,
    },
    check_quant=False,
)
class dxWDL(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/contrived/**"],
    expected_lint={
        "_suppressions": 8,
        "UnusedImport": 4,
        "NameCollision": 27,
        "StringCoercion": 6,
        "FileCoercion": 3,
        "NonemptyCoercion": 1,
        "UnnecessaryQuantifier": 5,
        "UnusedDeclaration": 2,
        "IncompleteCall": 2,
        "SelectArray": 1,
        "MissingVersion": 7,
        "UnboundDeclaration": 1,
        "UnverifiedStruct": 3,
        "Deprecated": 2,
    },
    blocklist=["check_quant", "incomplete_call"],
)
class Contrived(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/contrived/**"],
    expected_lint={
        "_suppressions": 16,
        "UnusedImport": 6,
        "NameCollision": 43,
        "StringCoercion": 11,
        "FileCoercion": 5,
        "OptionalCoercion": 3,
        "NonemptyCoercion": 2,
        "UnnecessaryQuantifier": 9,
        "UnusedDeclaration": 9,
        "IncompleteCall": 3,
        "ArrayCoercion": 2,
        "SelectArray": 4,
        "MissingVersion": 11,
        "UnboundDeclaration": 1,
        "UnverifiedStruct": 3,
        "Deprecated": 2,
    },
    check_quant=False,
    blocklist=["incomplete_call"],
)
class Contrived2(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/biowdl/tasks/**"],
    blocklist=[
        # these use the pattern 'input { Type? x = default }' and need check_quant=False
        "mergecounts",
        "somaticseq",
    ],
    expected_lint={
        "OptionalCoercion": 9,
        "UnusedDeclaration": 18,
        "NonemptyCoercion": 1,
        "NameCollision": 1,
        "SelectArray": 1,
        "UnverifiedStruct": 1,
        "UnnecessaryQuantifier": 8,
    },
)
class BioWDLTasks(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/biowdl/aligning/**"],
    expected_lint={
        "OptionalCoercion": 11,
        "UnusedDeclaration": 12,
        "NonemptyCoercion": 1,
        "NameCollision": 1,
        "UnverifiedStruct": 1,
        "UnnecessaryQuantifier": 13,
    },
    check_quant=False,
)
class BioWDLAligning(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/biowdl/expression-quantification/**"],
    expected_lint={
        "OptionalCoercion": 11,
        "UnusedDeclaration": 12,
        "NonemptyCoercion": 3,
        "NameCollision": 1,
        "UnverifiedStruct": 1,
        "UnnecessaryQuantifier": 9,
    },
    check_quant=False,
)
class BioWDLExpressionQuantification(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/biowdl/somatic-variantcalling"],
    expected_lint={
        "UnusedImport": 2,
        "OptionalCoercion": 11,
        "UnusedDeclaration": 11,
        "NonemptyCoercion": 37,
        "SelectArray": 5,
        "UnnecessaryQuantifier": 3,
    },
    check_quant=False,
)
class BioWDLSomaticVariantCalling(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/biowdl/small-rna"],
    expected_lint={
        "UnusedDeclaration": 8,
        "SelectArray": 2,
        "NonemptyCoercion": 3,
        "UnusedCall": 1,
        "UnverifiedStruct": 1,
        "UnnecessaryQuantifier": 7,
    },
    check_quant=False,
)
class BioWDLSmallRNA(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/broadinstitute/warp/pipelines/broad/**"],
    path=[["test_corpi/broadinstitute/warp/tasks"]],
    expected_lint={
        "UnusedImport": 22,
        "UnusedCall": 1,
        "StringCoercion": 86,
        "UnusedDeclaration": 106,
        "NameCollision": 12,
        "ForwardReference": 5,
        "NonemptyCoercion": 4,
        "FileCoercion": 17,
    },
)
class warp_pipelines_broad(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/broadinstitute/warp/pipelines/cemba/**"],
    expected_lint={
        "UnusedDeclaration": 1,
    },
)
class warp_pipelines_cemba(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/broadinstitute/warp/pipelines/skylab/**"],
    expected_lint={
        "UnusedDeclaration": 3,
        "UnnecessaryQuantifier": 3,
        "StringCoercion": 4,
        "FileCoercion": 3,
        "NameCollision": 3,
    },
)
class warp_pipelines_skylab(unittest.TestCase):
    pass
