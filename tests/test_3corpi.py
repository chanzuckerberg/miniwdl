import unittest
import unittest.mock
import tempfile
import os
import glob
import json
import urllib
import urllib.request
import logging
import shutil
import contextlib
import pathlib
import hashlib
import time
import textwrap
from .context import WDL
import WDL.Lint
import WDL._util


class Lint(unittest.TestCase):
    # test programmatic access to linter results
    # this code should be kept in sync with the example shown in the WDL/Lint.py docstring.
    def test_api(self):
        doc = WDL.load("ZarrUtils.wdl", path=["test_corpi/HumanCellAtlas/skylab/library/tasks"])
        WDL.Lint._shellcheck_available = False
        lint = WDL.Lint.collect(WDL.Lint.lint(doc, descend_imports=False))
        for pos, lint_class, message, suppressed in lint:
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

    def _file_coercion_lints(self, source, files=()):
        with tempfile.TemporaryDirectory(prefix="miniwdl_lint_test_") as testdir:
            src = os.path.join(testdir, "src")
            os.makedirs(src)
            for relpath, contents in files:
                path = os.path.join(src, relpath)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                if contents is None:
                    os.makedirs(path)
                else:
                    pathlib.Path(path).write_text(contents)
            wdl = os.path.join(src, "test.wdl")
            pathlib.Path(wdl).write_text(textwrap.dedent(source))

            doc = WDL.load(wdl)
            WDL.Lint._shellcheck_available = False
            WDL.Lint.lint(doc, descend_imports=False)
            return [
                message
                for _pos, lint_class, message, suppressed in WDL.Lint.collect(doc)
                if lint_class == "FileCoercion" and not suppressed
            ]

    def test_file_coercion_wdl12_source_relative_decl(self):
        lint = self._file_coercion_lints(
            """
            version 1.2
            struct FileBox {
                File f
            }
            workflow w {
                File existing_file = "data/input.txt"
                Directory existing_dir = "data/subdir"
                File computed = "data/" + "input.txt"
                File? missing_optional = "data/missing_optional.txt"
                File missing_required = "data/missing_required.txt"
                File escape = "../outside.txt"
                File absolute = "/__miniwdl_missing_input.txt"
                File uri = "s3://bucket/input.txt"
                File wrong_file = "data/subdir"
                Directory wrong_dir = "data/input.txt"
                Array[File] files = ["data/input.txt"]
                Array[File] bad_files = ["data/missing_array.txt"]
                Array[String] string_files = ["data/missing_unknown.txt"]
                Array[File] files_from_var = string_files
                Map[File, String] labels = {"data/input.txt": "ok"}
                Map[File, String] bad_labels = {"data/missing_map.txt": "bad"}
                Map[String, String] string_labels = {"data/missing_unknown.txt": "ok"}
                Map[File, String] labels_from_var = string_labels
                Pair[File, String] file_pair = ("data/input.txt", "ok")
                Pair[File, String] bad_file_pair = ("data/missing_pair.txt", "bad")
                Pair[String, String] string_pair = ("data/missing_unknown.txt", "ok")
                Pair[File, String] pair_from_var = string_pair
                FileBox file_box = FileBox { f: "data/input.txt" }
                FileBox bad_file_box = FileBox { f: "data/missing_struct.txt" }
            }
            """,
            files=[("data/input.txt", "input\n"), ("data/subdir", None), ("../outside.txt", "x")],
        )

        def lint_decl(message):
            return message.split(" = ")[0]

        self.assertEqual(
            {
                "File missing_required",
                "File escape",
                "File absolute",
                "File uri",
                "File wrong_file",
                "Directory wrong_dir",
                "Array[File] bad_files",
                "Map[File,String] bad_labels",
                "Pair[File,String] bad_file_pair",
                "FileBox bad_file_box",
            },
            {lint_decl(msg) for msg in lint},
        )

    def test_file_coercion_wdl11_relative_decl_unchanged(self):
        lint = self._file_coercion_lints(
            """
            version 1.1
            workflow w {
                File existing_file = "data/input.txt"
            }
            """,
            files=[("data/input.txt", "input\n")],
        )
        self.assertEqual(["File existing_file = :String:"], lint)

    def test_file_coercion_wdl12_buffer_relative_literal(self):
        doc = WDL.parse_document(
            """
            version 1.2
            workflow w {
                File relative = "data/input.txt"
            }
            """
        )
        doc.typecheck()
        WDL.Lint._shellcheck_available = False
        WDL.Lint.lint(doc, descend_imports=False)
        lint = [
            message
            for _pos, lint_class, message, suppressed in WDL.Lint.collect(doc)
            if lint_class == "FileCoercion" and not suppressed
        ]
        self.assertEqual([], lint)

    def test_guess_source_relative_path_kind_branches(self):
        with tempfile.TemporaryDirectory(prefix="miniwdl_lint_path_kind_") as testdir:
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "data", "subdir"))
            pathlib.Path(os.path.join(src, "data/input.txt")).write_text("input\n")
            pathlib.Path(os.path.join(testdir, "outside.txt")).write_text("outside\n")

            kind = WDL._util.SourceRelativePathKind
            guess = WDL._util.guess_source_relative_path_kind
            self.assertEqual(kind.ABSOLUTE, guess(src, "/tmp/input.txt"))
            self.assertEqual(kind.UNAVAILABLE, guess("", "data/input.txt"))
            self.assertEqual(kind.ESCAPES, guess(src, "../outside.txt"))
            self.assertEqual(kind.MISSING, guess(src, "data/missing.txt"))
            self.assertEqual(kind.OK, guess(src, "data/input.txt"))
            self.assertEqual(kind.OK, guess(src, "data/subdir/", directory=True))
            self.assertEqual(kind.WRONG_KIND, guess(src, "data/subdir"))
            self.assertEqual(kind.WRONG_KIND, guess(src, "data/input.txt", directory=True))


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
        assert len(files) > 0, (
            "{} test corpus missing from {}; please `git submodule update --init --recursive`".format(
                prefix, os.path.join(*dir)
            )
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
                            test_klass._lint_count["_suppressions"] = (
                                1 + test_klass._lint_count.get("_suppressions", 0)
                            )
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
        "StringCoercion": 15,
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
        "UnexpectedRuntimeValue": 4,
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
        "FileCoercion": 170,
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
        "FileCoercion": 236,
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
        "UnnecessaryQuantifier": 10,
        "MissingVersion": 52,
        "UnexpectedRuntimeValue": 1,
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
        "NameCollision": 29,
        "StringCoercion": 7,
        "FileCoercion": 5,
        "NonemptyCoercion": 1,
        "UnnecessaryQuantifier": 5,
        "UnusedDeclaration": 4,
        "IncompleteCall": 2,
        "SelectArray": 1,
        "MissingVersion": 7,
        "UnboundDeclaration": 1,
        "UnverifiedStruct": 3,
        "Deprecated": 5,
        "UnexpectedRuntimeValue": 1,
        "ImportNewerWDL": 1,
        "NonStringKeyMapJSON": 7,
    },
    blocklist=["check_quant", "incomplete_call", "issue596"],
)
class Contrived(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/contrived/**"],
    expected_lint={
        "_suppressions": 16,
        "UnusedImport": 6,
        "NameCollision": 45,
        "StringCoercion": 13,
        "FileCoercion": 7,
        "OptionalCoercion": 10,
        "NonemptyCoercion": 2,
        "UnnecessaryQuantifier": 9,
        "UnusedDeclaration": 11,
        "IncompleteCall": 3,
        "ArrayCoercion": 2,
        "SelectArray": 4,
        "MissingVersion": 11,
        "UnboundDeclaration": 1,
        "UnverifiedStruct": 3,
        "Deprecated": 6,
        "UnexpectedRuntimeValue": 1,
        "ImportNewerWDL": 2,
        "NonStringKeyMapJSON": 7,
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
        "bamstats",
        "biopet",
        "sampleconfig",
        "seqstat",
    ],
    expected_lint={
        "OptionalCoercion": 2,
        "UnusedDeclaration": 15,
        "NonemptyCoercion": 1,
        "SelectArray": 1,
        "UnnecessaryQuantifier": 8,
    },
)
class BioWDLTasks(unittest.TestCase):
    pass


@wdl_corpus(
    ["test_corpi/biowdl/aligning/**"],
    expected_lint={
        "FileCoercion": 1,
        "OptionalCoercion": 11,
        "UnusedDeclaration": 12,
        "NonemptyCoercion": 2,
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
        "FileCoercion": 1,
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


@wdl_corpus(
    ["test_corpi/broadinstitute/gatk/scripts/**"],
    check_quant=False,
    expected_lint={"UnusedDeclaration": 11, "OptionalCoercion": 4, "UnnecessaryQuantifier": 1},
)
class gatk_scripts(unittest.TestCase):
    pass


class TestZip(unittest.TestCase):
    def _roundtrip(self, doc, inputs=None):
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            meta = {"foo": "bar"}
            main_wdl = os.path.basename(doc.pos.abspath)
            zip_fn = os.path.join(testdir, main_wdl + ".zip")
            WDL.Zip.build(
                doc, zip_fn, logging.getLogger("miniwdl_zip_test"), meta=meta, inputs=inputs
            )

            source_dir, main_wdl, inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            assert not inputs or inputs_file
            WDL.load(os.path.join(source_dir, main_wdl))

            # cover misc code paths through WDL.Zip.unpack()
            WDL.load(cleanup.enter_context(WDL.Zip.unpack(source_dir)).main_wdl)
            WDL.load(
                cleanup.enter_context(
                    WDL.Zip.unpack(os.path.join(source_dir, "MANIFEST.json"))
                ).main_wdl
            )
            os.unlink(os.path.join(source_dir, "MANIFEST.json"))
            with self.assertRaises(WDL.Error.InputError):
                cleanup.enter_context(WDL.Zip.unpack(source_dir))

    def test_empty(self):
        self._roundtrip(WDL.load("test_corpi/contrived/empty.wdl"))

    def test_scatter_collisions(self):
        # import single-quoted URI (issue #601)
        self._roundtrip(WDL.load("test_corpi/contrived/scatter_collisions.wdl"))

    def test_biowdl_aligning(self):
        self._roundtrip(WDL.load("test_corpi/biowdl/aligning/align-star.wdl"))

    def test_wgs(self):
        # multiple nested subworkflows
        self._roundtrip(
            WDL.load(
                "test_corpi/broadinstitute/warp/pipelines/broad/reprocessing/wgs/WholeGenomeReprocessing.wdl",
            ),
            inputs={"foo": ["bar", "baz"]},
        )

    def test_assemble_refbased(self):
        self._roundtrip(
            WDL.load(
                "test_corpi/broadinstitute/viral-ngs/pipes/WDL/workflows/assemble_denovo.wdl",
                path=["test_corpi/broadinstitute/viral-ngs/pipes/WDL/workflows/tasks"],
            ),
        )

    def test_additional_files_source_relative(self):
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "data/glob"))
            os.makedirs(os.path.join(src, "data/dir"))
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                'version 1.2\nworkflow w { File f = "data/input.txt" }\n'
            )
            pathlib.Path(os.path.join(src, "data/input.txt")).write_text("input\n")
            pathlib.Path(os.path.join(src, "data/glob/a.txt")).write_text("a\n")
            pathlib.Path(os.path.join(src, "data/glob/b.txt")).write_text("b\n")
            pathlib.Path(os.path.join(src, "data/dir/c.txt")).write_text("c\n")
            os.symlink("input.txt", os.path.join(src, "data/link.txt"))

            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            WDL.Zip.build(
                doc,
                zip_fn,
                logging.getLogger("miniwdl_zip_test"),
                additional_files=[
                    os.path.join(src, "data/input.txt"),
                    os.path.join(src, "data/glob/*.txt"),
                    os.path.join(src, "data/dir"),
                    os.path.join(src, "data/link.txt"),
                ],
            )

            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            for relpath in [
                "data/input.txt",
                "data/glob/a.txt",
                "data/glob/b.txt",
                "data/dir/c.txt",
                "data/link.txt",
            ]:
                self.assertTrue(os.path.isfile(os.path.join(source_dir, relpath)), relpath)
            self.assertEqual(
                pathlib.Path(os.path.join(source_dir, "data/link.txt")).read_text(), "input\n"
            )

    def test_additional_files_outside_import(self):
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            main_dir = os.path.join(testdir, "main")
            lib_dir = os.path.join(testdir, "lib")
            os.makedirs(main_dir)
            os.makedirs(os.path.join(lib_dir, "data"))
            pathlib.Path(os.path.join(main_dir, "main.wdl")).write_text(
                'version 1.2\nimport "../lib/lib.wdl"\nworkflow w {}\n'
            )
            pathlib.Path(os.path.join(lib_dir, "lib.wdl")).write_text(
                'version 1.2\ntask t { File f = "data/ref.txt" command {} }\n'
            )
            pathlib.Path(os.path.join(lib_dir, "data/ref.txt")).write_text("ref\n")

            doc = WDL.load(os.path.join(main_dir, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            WDL.Zip.build(
                doc,
                zip_fn,
                logging.getLogger("miniwdl_zip_test"),
                additional_files=[os.path.join(lib_dir, "data/ref.txt")],
            )

            source_dir, main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            WDL.load(main_wdl)
            self.assertTrue(
                os.path.isfile(os.path.join(source_dir, "__outside_wdl/lib/data/ref.txt"))
            )

    def test_additional_files_errors(self):
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "data"))
            pathlib.Path(os.path.join(src, "main.wdl")).write_text("version 1.2\nworkflow w {}\n")
            pathlib.Path(os.path.join(src, "data/input.txt")).write_text("input\n")
            pathlib.Path(os.path.join(testdir, "outside.txt")).write_text("outside\n")
            os.symlink(os.path.join(testdir, "outside.txt"), os.path.join(src, "data/bad_link.txt"))

            doc = WDL.load(os.path.join(src, "main.wdl"))
            logger = logging.getLogger("miniwdl_zip_test")
            zip_fn = os.path.join(testdir, "source.zip")
            with self.assertRaisesRegex(WDL.Error.InputError, "overwrites existing path"):
                WDL.Zip.build(doc, zip_fn, logger, additional_files=[os.path.join(src, "main.wdl")])
            with self.assertRaisesRegex(WDL.Error.InputError, "matched nothing"):
                WDL.Zip.build(
                    doc, zip_fn, logger, additional_files=[os.path.join(src, "data/*.missing")]
                )
            with self.assertRaisesRegex(WDL.Error.InputError, "WDL source directory"):
                WDL.Zip.build(
                    doc,
                    zip_fn,
                    logger,
                    additional_files=[os.path.join(testdir, "outside.txt")],
                )
            with self.assertRaisesRegex(WDL.Error.InputError, "WDL source directory"):
                WDL.Zip.build(
                    doc,
                    zip_fn,
                    logger,
                    additional_files=[os.path.join(src, "data/bad_link.txt")],
                )

    def test_additional_files_helper_branches(self):
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "data"))
            pathlib.Path(os.path.join(src, "main.wdl")).write_text("version 1.2\nworkflow w {}\n")
            pathlib.Path(os.path.join(src, "other.wdl")).write_text("version 1.2\nworkflow w2 {}\n")
            pathlib.Path(os.path.join(src, "data/input.txt")).write_text("input\n")
            pathlib.Path(os.path.join(src, "data/other.txt")).write_text("other\n")
            outside = os.path.join(testdir, "outside")
            os.makedirs(outside)
            pathlib.Path(os.path.join(outside, "escape.txt")).write_text("escape\n")
            os.symlink("missing.txt", os.path.join(src, "data/broken.txt"))
            os.symlink(outside, os.path.join(src, "data/unsafe_dir"))
            os.symlink(os.path.join(src, "data"), os.path.join(src, "data/self"))
            os.makedirs(os.path.join(src, "data/sub"))
            pathlib.Path(os.path.join(src, "data/sub/nested.txt")).write_text("nested\n")
            os.symlink(os.path.join(src, "data/sub"), os.path.join(src, "data/sub_again"))

            doc = WDL.load(os.path.join(src, "main.wdl"))
            other_doc = WDL.load(os.path.join(src, "other.wdl"))
            logger = logging.getLogger("miniwdl_zip_test")

            with self.assertRaisesRegex(WDL.Error.InputError, "require local WDL source"):
                WDL.Zip.add_additional_files(
                    testdir,
                    [os.path.join(src, "data/input.txt")],
                    {},
                    {},
                    logger,
                )

            with self.assertRaisesRegex(WDL.Error.InputError, "ambiguous WDL source directory"):
                WDL.Zip._additional_source_dirs(
                    {
                        doc.pos.abspath: "main.wdl",
                        other_doc.pos.abspath: "nested/other.wdl",
                    },
                    {doc.pos.abspath: doc, other_doc.pos.abspath: other_doc},
                )

            zip_paths = {
                doc.pos.abspath: "main.wdl",
                "/not/a/loaded/doc.wdl": "ignored.wdl",
            }
            wdls = {doc.pos.abspath: doc}
            self.assertEqual(WDL.Zip._additional_source_dirs(zip_paths, wdls), [(src, "")])
            missing_doc_abspath = os.path.join(src, "missing.wdl")
            fake_missing_doc = type("FakeMissingDoc", (), {"source_dir": src + os.sep})()
            self.assertEqual(
                WDL.Zip._additional_source_dirs(
                    {missing_doc_abspath: "missing.wdl"}, {missing_doc_abspath: fake_missing_doc}
                ),
                [],
            )
            buffer_doc = WDL.parse_document("version 1.2\nworkflow w {}\n")
            self.assertEqual(
                WDL.Zip._additional_source_dirs(
                    {buffer_doc.pos.abspath: "buffer.wdl"},
                    {buffer_doc.pos.abspath: buffer_doc},
                ),
                [],
            )

            with self.assertRaisesRegex(WDL.Error.InputError, "not found"):
                WDL.Zip.add_additional_files(
                    cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_")),
                    [os.path.join(src, "data/missing_literal.txt")],
                    {doc.pos.abspath: "main.wdl"},
                    wdls,
                    logger,
                )

            staging = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_"))
            WDL.Zip.add_additional_files(
                staging,
                [
                    os.path.join(src, "data/input.txt"),
                    os.path.join(src, "data/i*.txt"),
                    os.path.join(src, "data/other.txt"),
                ],
                {doc.pos.abspath: "main.wdl"},
                wdls,
                logger,
            )
            self.assertEqual(
                pathlib.Path(os.path.join(staging, "data/input.txt")).read_text(), "input\n"
            )
            self.assertEqual(
                pathlib.Path(os.path.join(staging, "data/other.txt")).read_text(), "other\n"
            )
            self.assertEqual(
                WDL.Zip._additional_dest(
                    os.path.join(src, "data/input.txt"), [(outside, "outside"), (src, "")]
                ),
                os.path.normpath("data/input.txt"),
            )

            src2 = os.path.join(testdir, "src2")
            os.makedirs(os.path.join(src2, "data"))
            pathlib.Path(os.path.join(src2, "lib.wdl")).write_text("version 1.2\nworkflow w3 {}\n")
            pathlib.Path(os.path.join(src2, "data/input.txt")).write_text("different\n")
            doc2 = WDL.load(os.path.join(src2, "lib.wdl"))
            with self.assertRaisesRegex(WDL.Error.InputError, "overwrites existing path"):
                WDL.Zip.add_additional_files(
                    cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_")),
                    [
                        os.path.join(src, "data/input.txt"),
                        os.path.join(src2, "data/input.txt"),
                    ],
                    {doc.pos.abspath: "main.wdl", doc2.pos.abspath: "lib.wdl"},
                    {doc.pos.abspath: doc, doc2.pos.abspath: doc2},
                    logger,
                )

            with self.assertRaisesRegex(WDL.Error.InputError, "neither a file nor a directory"):
                WDL.Zip.add_additional_files(
                    cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_")),
                    [os.path.join(src, "data/broken.txt")],
                    {doc.pos.abspath: "main.wdl"},
                    wdls,
                    logger,
                )
            os.unlink(os.path.join(src, "data/broken.txt"))

            with self.assertRaisesRegex(WDL.Error.InputError, "unsafe symlink"):
                WDL.Zip.add_additional_files(
                    cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_")),
                    [os.path.join(src, "data")],
                    {doc.pos.abspath: "main.wdl"},
                    wdls,
                    logger,
                )

            os.unlink(os.path.join(src, "data/unsafe_dir"))
            loop_staging = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_"))
            WDL.Zip.add_additional_files(
                loop_staging,
                [os.path.join(src, "data")],
                {doc.pos.abspath: "main.wdl"},
                wdls,
                logger,
            )
            self.assertTrue(os.path.isfile(os.path.join(loop_staging, "data/input.txt")))

            with self.assertRaisesRegex(
                WDL.Error.InputError, "Invalid additional file destination"
            ):
                WDL.Zip.add_additional_files(
                    cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_")),
                    [os.path.join(src, "data/input.txt")],
                    {doc.pos.abspath: os.path.join(testdir, "absolute", "main.wdl")},
                    wdls,
                    logger,
                )

    def test_unpack_tempdir_parent(self):
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(src)
            pathlib.Path(os.path.join(src, "main.wdl")).write_text("version 1.2\nworkflow w {}\n")
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            WDL.Zip.build(doc, zip_fn, logging.getLogger("miniwdl_zip_test"))

            source_dir = cleanup.enter_context(contextlib.ExitStack())
            staged = WDL.Zip.build_source_dir(
                source_dir, doc, logging.getLogger("miniwdl_zip_test")
            )
            self.assertTrue(os.path.isfile(os.path.join(staged, "main.wdl")))
            with self.assertRaisesRegex(ValueError, "Unknown format"):
                WDL.Zip.create_reproducible_archive(
                    staged, os.path.join(testdir, "unknown.out"), "bogus"
                )
            self.assertEqual(
                WDL.Zip.build_zip_paths(
                    "/tmp/foo/bar/",
                    {"/tmp/foo/baz/lib.wdl": doc},
                    logging.getLogger("miniwdl_zip_test"),
                ),
                {"/tmp/foo/baz/lib.wdl": "__outside_wdl/baz/lib.wdl"},
            )

            cwd = os.getcwd()
            try:
                os.chdir(testdir)
                WDL.Zip.build(doc, "relative.zip", logging.getLogger("miniwdl_zip_test"))
                self.assertTrue(os.path.isfile(os.path.join(testdir, "relative.zip")))
            finally:
                os.chdir(cwd)

            parent = os.path.join(testdir, "extract")
            os.makedirs(parent)
            unpacked = cleanup.enter_context(WDL.Zip.unpack(zip_fn, tempdir_parent=parent))
            self.assertTrue(unpacked.dir.startswith(parent))
            with self.assertRaisesRegex(WDL.Error.InputError, "TMPDIR"):
                cleanup.enter_context(
                    WDL.Zip.unpack(zip_fn, tempdir_parent=os.path.join(testdir, "missing"))
                )
            with unittest.mock.patch.object(
                WDL.Zip.tempfile, "TemporaryDirectory", side_effect=OSError("no temp")
            ):
                with self.assertRaisesRegex(
                    WDL.Error.InputError, "Unable to create temporary directory"
                ) as exn:
                    cleanup.enter_context(WDL.Zip.unpack(zip_fn))
                self.assertNotIn("TMPDIR", str(exn.exception))
            bad_zip = os.path.join(testdir, "bad.zip")
            pathlib.Path(bad_zip).write_text("not a zip\n")
            with self.assertRaisesRegex(WDL.Error.InputError, "Unreadable source archive"):
                cleanup.enter_context(WDL.Zip.unpack(bad_zip))

            bad_manifest = os.path.join(testdir, "bad_manifest")
            os.makedirs(bad_manifest)
            pathlib.Path(os.path.join(bad_manifest, "MANIFEST.json")).write_text(
                '{"mainWorkflowURL": "missing.wdl"}'
            )
            with self.assertRaisesRegex(WDL.Error.InputError, "missing or invalid"):
                cleanup.enter_context(WDL.Zip.unpack(bad_manifest))

    def test_cli_unpack_source_zip_tempdir_selection(self):
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(src)
            pathlib.Path(os.path.join(src, "main.wdl")).write_text("version 1.2\nworkflow w {}\n")
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            WDL.Zip.build(doc, zip_fn, logging.getLogger("miniwdl_zip_test"))

            logger = logging.getLogger("miniwdl_run_zip_test")
            root = os.path.join(testdir, "root")
            os.makedirs(root)
            main_wdl, input_file = WDL.CLI.unpack_source_zip(logger, cleanup, zip_fn, root)
            self.assertIsNone(input_file)
            self.assertTrue(main_wdl.startswith(root + os.sep))

            main_wdl, _input_file = WDL.CLI.unpack_source_zip(
                logger, cleanup, zip_fn, tempfile.gettempdir()
            )
            self.assertTrue(main_wdl.startswith(tempfile.gettempdir()))
            self.assertFalse(main_wdl.startswith(root + os.sep))

            self.assertEqual(
                WDL.CLI.unpack_source_zip(logger, cleanup, os.path.join(src, "main.wdl"), root),
                (os.path.join(src, "main.wdl"), None),
            )

            unpacked = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertEqual(
                WDL.CLI.unpack_source_zip(logger, cleanup, unpacked.dir, root),
                (unpacked.main_wdl, None),
            )

    def test_reproducible_zip(self):
        self._reproducible_test("zip")

    def test_reproducible_tar(self):
        self._reproducible_test("tar")

    def _reproducible_test(self, format):
        original_wdl = "test_corpi/biowdl/expression-quantification/multi-bam-quantify.wdl"
        doc = WDL.load(original_wdl)
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            meta = {"foo": "bar"}
            main_wdl = os.path.basename(doc.pos.abspath)
            zip_fn = os.path.join(testdir, main_wdl + f".{format}")
            WDL.Zip.build(
                doc, zip_fn, logging.getLogger("miniwdl_zip_test"), meta=meta, archive_format=format
            )
            zip_contents = pathlib.Path(zip_fn).read_bytes()
            zip_checksum = hashlib.sha1(zip_contents).hexdigest()

            time.sleep(2)  # Sleep 2 seconds to make sure modification times are different.

            copy_pipeline_dir = cleanup.enter_context(
                tempfile.TemporaryDirectory(prefix="miniwdl_reproducible_zip_test")
            )
            copy_pipeline_dir = os.path.join(copy_pipeline_dir, "contents")
            # Copy file contents, but not file metadata.
            copied_pipeline_dir = shutil.copytree(
                os.path.dirname(original_wdl), copy_pipeline_dir, copy_function=shutil.copyfile
            )
            copied_wdl = os.path.join(copied_pipeline_dir, "multi-bam-quantify.wdl")
            copied_doc = WDL.load(copied_wdl)
            copied_zip_fn = os.path.join(testdir, main_wdl + f".copied.{format}")
            WDL.Zip.build(
                copied_doc,
                copied_zip_fn,
                logging.getLogger("miniwdl_zip_test"),
                meta=meta,
                archive_format=format,
            )
            copied_zip_contents = pathlib.Path(copied_zip_fn).read_bytes()
            copied_zip_checksum = hashlib.sha1(copied_zip_contents).hexdigest()
            self.assertEqual(zip_checksum, copied_zip_checksum)
