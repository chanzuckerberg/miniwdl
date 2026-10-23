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
import subprocess
import contextlib
import pathlib
import hashlib
import time
import textwrap
import zipfile
from .context import WDL
import WDL.Lint


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
                Directory existing_dir_slash = "data/subdir/"
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
                Array[File] uri_files = ["s3://bucket/input.txt"]
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
                "Array[File] uri_files",
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

    def _git(self, cwd, *args):
        # test fixtures may drive the git executable directly
        subprocess.run(["git", "-C", cwd, *args], check=True, capture_output=True)

    def _init_git_repo(self, path):
        self._git(path, "init", "-q")

    @contextlib.contextmanager
    def _git_unavailable(self):
        # simulate a machine with no usable git executable: every attempt to run git
        # raises FileNotFoundError, as exec of a missing program does
        real_run = subprocess.run

        def run(cmd, *args, **kwargs):
            if cmd and cmd[0] == "git":
                raise FileNotFoundError(2, "No such file or directory", "git")
            return real_run(cmd, *args, **kwargs)

        with unittest.mock.patch.object(subprocess, "run", run):
            yield

    def _source_relative_fixture(self, cleanup):
        # WDL 1.2 workflow exercising the source-relative reference classes of issue #878:
        # literal File/Directory defaults, literals nested in an Array, a computed path,
        # an optional missing default, a call input binding, and an imported task default
        testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
        src = os.path.join(testdir, "src")
        os.makedirs(os.path.join(src, "data", "refdir"))
        os.makedirs(os.path.join(src, "lib"))
        for relpath, contents in [
            ("data/literal.txt", "literal\n"),
            ("data/refdir/a.txt", "a\n"),
            ("data/refdir/b.txt", "b\n"),
            ("data/ref.fa", "fa\n"),
            ("data/ref.fa.fai", "fai\n"),
            ("data/dynamic_A.txt", "dynamic\n"),
            ("data/callbound.txt", "callbound\n"),
            ("lib/helper.txt", "helper\n"),
        ]:
            pathlib.Path(os.path.join(src, relpath)).write_text(contents)
        pathlib.Path(os.path.join(src, "lib", "tasks.wdl")).write_text(
            "version 1.2\n"
            "task helper_task {\n"
            "    input {\n"
            '        File helper = "helper.txt"\n'
            "        String msg\n"
            "    }\n"
            "    command { cat ~{helper} }\n"
            "    output { String out = read_string(stdout()) }\n"
            "}\n"
        )
        pathlib.Path(os.path.join(src, "main.wdl")).write_text(
            "version 1.2\n"
            'import "lib/tasks.wdl" as lib\n'
            "workflow zipprobe {\n"
            "    input {\n"
            '        File literal_ref = "data/literal.txt"\n'
            '        Directory ref_dir = "data/refdir"\n'
            '        Array[File] ref_set = ["data/ref.fa", "data/ref.fa.fai"]\n'
            '        String sample = "A"\n'
            '        File computed_ref = "data/dynamic_" + sample + ".txt"\n'
            '        File? maybe_ref = "data/not_present.txt"\n'
            "        File json_ref\n"
            "    }\n"
            '    call lib.helper_task as h { input: helper = "data/callbound.txt", msg = "x" }\n'
            "    output { File o_json = json_ref }\n"
            "}\n"
        )
        return testdir, src

    def test_zip_auto_add_source_relative(self):
        with contextlib.ExitStack() as cleanup:
            testdir, src = self._source_relative_fixture(cleanup)
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(doc, zip_fn, logger)
            warnings = "\n".join(logs.output)
            # the computed path can't be known statically; warn about it
            self.assertIn("computed_ref", warnings)
            # the optional missing default is acceptable, and covered literals are quiet
            self.assertNotIn("maybe_ref", warnings)
            self.assertNotIn("literal_ref", warnings)

            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            for relpath in [
                "data/literal.txt",
                "data/refdir/a.txt",
                "data/refdir/b.txt",
                "data/ref.fa",
                "data/ref.fa.fai",
                "data/callbound.txt",
                "lib/helper.txt",
            ]:
                self.assertTrue(os.path.isfile(os.path.join(source_dir, relpath)), relpath)
            # the computed path's would-be target is not swept in
            self.assertFalse(os.path.exists(os.path.join(source_dir, "data/dynamic_A.txt")))

    def test_zip_auto_add_warnings(self):
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(src)
            pathlib.Path(os.path.join(src, "plain.txt")).write_text("plain\n")
            pathlib.Path(os.path.join(testdir, "outside.txt")).write_text("outside\n")
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                "version 1.2\n"
                "workflow w {\n"
                "    input {\n"
                '        File gone = "data/absent.txt"\n'
                '        File esc = "../outside.txt"\n'
                '        Directory wrong_kind = "plain.txt"\n'
                "    }\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(doc, zip_fn, logger)
            warnings = "\n".join(logs.output)
            self.assertIn("does not exist", warnings)
            self.assertIn("data/absent.txt", warnings)
            self.assertIn("resolves outside", warnings)
            self.assertIn("is not a directory", warnings)
            # warnings never fail the archive, and uncovered files stay out of it
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "main.wdl")))
            self.assertFalse(os.path.exists(os.path.join(source_dir, "plain.txt")))

    def test_zip_no_auto_add(self):
        with contextlib.ExitStack() as cleanup:
            testdir, src = self._source_relative_fixture(cleanup)
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(doc, zip_fn, logger, auto_add=False)
            warnings = "\n".join(logs.output)
            self.assertIn("data/literal.txt", warnings)
            self.assertIn("automatic inclusion disabled", warnings)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertFalse(os.path.exists(os.path.join(source_dir, "data/literal.txt")))
            self.assertFalse(os.path.exists(os.path.join(source_dir, "lib/helper.txt")))

    @unittest.skipUnless(shutil.which("git"), "git executable not available")
    def test_zip_add_all(self):
        # in a git work tree, --add-all sweeps exactly the files git accounts for:
        # tracked files (even ones also matched by an ignore pattern), plus untracked
        # files not excluded by .gitignore rules at any level
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "sub"))
            os.makedirs(os.path.join(src, "ignored_dir"))
            pathlib.Path(os.path.join(src, "main.wdl")).write_text("version 1.2\nworkflow w {}\n")
            # stage this file before the ignore pattern naming it exists: git accounts
            # for tracked files regardless of ignore rules
            pathlib.Path(os.path.join(src, "tracked.bin")).write_text("x\n")
            self._init_git_repo(src)
            self._git(src, "add", "tracked.bin")
            pathlib.Path(os.path.join(src, ".gitignore")).write_text(
                "*.log\nignored.bin\ntracked.bin\n/deep.txt\nignored_dir/\n!keep.log\n"
            )
            pathlib.Path(os.path.join(src, "sub", ".gitignore")).write_text("inner_skip.txt\n")
            for relpath in [
                "keep.txt",
                "keep.log",
                "skip.log",
                "ignored.bin",
                "deep.txt",
                "sub/inner_keep.txt",
                "sub/inner_skip.txt",
                "ignored_dir/x.txt",
            ]:
                pathlib.Path(os.path.join(src, relpath)).write_text("x\n")
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            WDL.Zip.build(doc, zip_fn, logging.getLogger("miniwdl_zip_test"), add_all=True)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            for relpath in [
                "keep.txt",
                "keep.log",
                "tracked.bin",
                "sub/inner_keep.txt",
                ".gitignore",
                "sub/.gitignore",
            ]:
                self.assertTrue(os.path.isfile(os.path.join(source_dir, relpath)), relpath)
            for relpath in [
                "skip.log",
                "ignored.bin",
                "deep.txt",
                "sub/inner_skip.txt",
                "ignored_dir/x.txt",
                ".git/HEAD",
            ]:
                self.assertFalse(os.path.exists(os.path.join(source_dir, relpath)), relpath)

    def test_zip_cli_flags(self):
        with contextlib.ExitStack() as cleanup:
            testdir, src = self._source_relative_fixture(cleanup)
            main_wdl = os.path.join(src, "main.wdl")
            zip_fn = os.path.join(testdir, "cli.zip")
            WDL.CLI.zip_wdl(main_wdl, output=zip_fn, force=True, no_auto_add=True)
            with zipfile.ZipFile(zip_fn) as zf:
                self.assertNotIn("data/literal.txt", zf.namelist())
            WDL.CLI.zip_wdl(main_wdl, output=zip_fn, force=True, add_all=True)
            with zipfile.ZipFile(zip_fn) as zf:
                names = zf.namelist()
            self.assertIn("data/literal.txt", names)
            self.assertIn("data/dynamic_A.txt", names)

    def test_zip_symlink_literal_keeps_its_path(self):
        # a source-relative literal naming a symlink (or a path through a symlinked
        # directory) is archived under the literal's own path, not its realpath
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "data", "realdir"))
            pathlib.Path(os.path.join(src, "data", "real.txt")).write_text("real\n")
            pathlib.Path(os.path.join(src, "data", "realdir", "inner.txt")).write_text("inner\n")
            os.symlink("real.txt", os.path.join(src, "data", "link.txt"))
            os.symlink("realdir", os.path.join(src, "data", "linkdir"))
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                "version 1.2\n"
                "workflow w {\n"
                "    input {\n"
                '        File via_link = "data/link.txt"\n'
                '        File via_linkdir = "data/linkdir/inner.txt"\n'
                "    }\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertNoLogs(logger, level="WARNING"):
                WDL.Zip.build(doc, zip_fn, logger)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertEqual(
                pathlib.Path(os.path.join(source_dir, "data/link.txt")).read_text(), "real\n"
            )
            self.assertEqual(
                pathlib.Path(os.path.join(source_dir, "data/linkdir/inner.txt")).read_text(),
                "inner\n",
            )

    def test_zip_imported_doc_placement(self):
        # each reference is placed relative to the document that mentions it: the imported
        # document lives outside the main document's tree (in a string-prefix sibling
        # directory, proj-shared/ next to proj/), so its files belong under its own
        # __outside_wdl/ archive directory even when the main tree has a same-named path
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            main_dir = os.path.join(testdir, "proj")
            app_dir = os.path.join(testdir, "proj-shared", "app")
            os.makedirs(os.path.join(main_dir, "data"))
            os.makedirs(os.path.join(app_dir, "data"))
            pathlib.Path(os.path.join(main_dir, "data", "a.txt")).write_text("main\n")
            pathlib.Path(os.path.join(app_dir, "data", "a.txt")).write_text("app\n")
            pathlib.Path(os.path.join(app_dir, "tasks.wdl")).write_text(
                "version 1.2\n"
                "task t {\n"
                "    input {\n"
                '        File f = "data/a.txt"\n'
                "    }\n"
                "    command { cat ~{f} }\n"
                "    output { String o = read_string(stdout()) }\n"
                "}\n"
            )
            pathlib.Path(os.path.join(main_dir, "main.wdl")).write_text(
                "version 1.2\n"
                'import "../proj-shared/app/tasks.wdl" as app\n'
                "workflow w {\n"
                "    input {\n"
                '        File g = "data/a.txt"\n'
                "    }\n"
                "    call app.t\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(main_dir, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(doc, zip_fn, logger)
            warnings = "\n".join(logs.output)
            # the outside-import notice is expected; no coverage warnings may fire
            self.assertNotIn("not included in the archive", warnings)
            self.assertNotIn("already staged", warnings)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertEqual(
                pathlib.Path(os.path.join(source_dir, "data/a.txt")).read_text(), "main\n"
            )
            self.assertEqual(
                pathlib.Path(
                    os.path.join(
                        source_dir, "__outside_wdl", "proj-shared", "app", "data", "a.txt"
                    )
                ).read_text(),
                "app\n",
            )

    def test_zip_reserved_name_collision(self):
        # a user file colliding with a generated reserved name (MANIFEST.json) is not
        # silently replaced by the generated entry: the build warns about the collision,
        # and the generated manifest still lands in the archive
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(src)
            pathlib.Path(os.path.join(src, "MANIFEST.json")).write_text('{"user": true}\n')
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                "version 1.2\n"
                "workflow w {\n"
                "    input {\n"
                '        File f = "MANIFEST.json"\n'
                "    }\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(doc, zip_fn, logger)
            warnings = "\n".join(logs.output)
            self.assertIn("MANIFEST.json", warnings)
            self.assertIn("already staged", warnings)
            # the collision warning stands in for the generic uncovered warning
            self.assertNotIn("is not included in the archive", warnings)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            manifest = json.loads(pathlib.Path(os.path.join(source_dir, "MANIFEST.json")).read_text())
            self.assertEqual(manifest["mainWorkflowURL"], "main.wdl")

    def test_zip_empty_directory(self):
        # empty Directory targets (and empty subdirectories of staged trees) are archived
        # as directory entries, in both zip and tar, instead of vanishing
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "data", "emptydir"))
            os.makedirs(os.path.join(src, "data", "tree", "hollow"))
            os.makedirs(os.path.join(src, "data", "tree", "full"))
            pathlib.Path(os.path.join(src, "data", "tree", "full", "x.txt")).write_text("x\n")
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                "version 1.2\n"
                "workflow w {\n"
                "    input {\n"
                '        Directory d = "data/emptydir"\n'
                '        Directory t = "data/tree"\n'
                "    }\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "main.wdl"))
            logger = logging.getLogger("miniwdl_zip_test")

            zip_fn = os.path.join(testdir, "source.zip")
            WDL.Zip.build(doc, zip_fn, logger)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertTrue(os.path.isdir(os.path.join(source_dir, "data", "emptydir")))
            self.assertTrue(os.path.isdir(os.path.join(source_dir, "data", "tree", "hollow")))
            self.assertTrue(
                os.path.isfile(os.path.join(source_dir, "data", "tree", "full", "x.txt"))
            )

            tar_fn = os.path.join(testdir, "source.tar")
            WDL.Zip.build(doc, tar_fn, logger, archive_format="tar")
            import tarfile

            with tarfile.open(tar_fn) as tar:
                members = {member.name: member for member in tar.getmembers()}
            self.assertTrue(members["data/emptydir"].isdir())
            self.assertTrue(members["data/tree/hollow"].isdir())
            self.assertTrue(members["data/tree/full/x.txt"].isfile())
            # directory entries need a searchable mode (TarInfo's default 0644 is not)
            self.assertEqual(members["data/emptydir"].mode, 0o755)

    def test_zip_legacy_call_binding(self):
        # a call binding into a task without an input{} section (pre-1.0 style "implicit"
        # inputs) is scanned through the callee's available_inputs, like typechecking
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "data"))
            os.makedirs(os.path.join(src, "lib"))
            pathlib.Path(os.path.join(src, "data", "legacy.txt")).write_text("legacy\n")
            pathlib.Path(os.path.join(src, "lib", "legacy.wdl")).write_text(
                "version 1.2\n"
                "task legacy {\n"
                "    File f\n"
                "    command { cat ~{f} }\n"
                "    output { String o = read_string(stdout()) }\n"
                "}\n"
            )
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                "version 1.2\n"
                'import "lib/legacy.wdl" as lib\n'
                "workflow w {\n"
                '    call lib.legacy { input: f = "data/legacy.txt" }\n'
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertNoLogs(logger, level="WARNING"):
                WDL.Zip.build(doc, zip_fn, logger)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "data", "legacy.txt")))

    def test_zip_any_typed_computed_warns(self):
        # a File whose value is computed with static type Any (e.g. a read_json result)
        # gets the unprovable-reference warning; the function's own arguments are not
        # scanned as source-relative references (issue #878 phase 1 scope)
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(src)
            pathlib.Path(os.path.join(src, "config.json")).write_text('{"f": "x"}\n')
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                "version 1.2\n"
                "workflow w {\n"
                "    input {\n"
                '        File f = read_json("config.json")\n'
                '        Array[File] fs = [read_json("config.json")]\n'
                "    }\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(doc, zip_fn, logger)
            warnings = "\n".join(logs.output)
            self.assertIn("Cannot determine statically", warnings)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertFalse(os.path.exists(os.path.join(source_dir, "config.json")))

    def test_zip_derived_from_file_input_no_warning(self):
        # paths derived from a File/Directory input (bam + ".bai", select_first over
        # Files) evaluate to absolute paths at run time, not source-relative ones, so
        # they draw no unprovable-reference warning -- unlike paths computed purely
        # from strings, which still warn (see the fixture & read_json tests)
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(src)
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                "version 1.2\n"
                "task t {\n"
                "    input {\n"
                "        File bam\n"
                "    }\n"
                '    File bai = bam + ".bai"\n'
                "    command { cat ~{bam} }\n"
                "    output { File o = bai }\n"
                "}\n"
                "workflow w {\n"
                "    input {\n"
                "        File bam\n"
                "        File idx = select_first([bam])\n"
                "    }\n"
                "    call t { input: bam = bam }\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertNoLogs(logger, level="WARNING"):
                WDL.Zip.build(doc, zip_fn, logger)

    def test_zip_computed_default_literal_included(self):
        # "default unless overridden": string literals in candidate positions of a
        # computed File expression (select_first elements, conditional branches) are
        # harvested and classified like plain literals, so the default is auto-included
        # instead of passing silently -- while a candidate naming a directory at a
        # File leaf draws the usual wrong-kind warning and isn't included
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "data", "refdir"))
            pathlib.Path(os.path.join(src, "data", "default.fa")).write_text("fa\n")
            pathlib.Path(os.path.join(src, "data", "refdir", "a.txt")).write_text("a\n")
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                "version 1.2\n"
                "workflow w {\n"
                "    input {\n"
                "        File? ref_override\n"
                '        File ref = select_first([ref_override, "data/default.fa"])\n'
                "        File ref2 = if defined(ref_override) then select_first([ref_override])"
                ' else "data/default.fa"\n'
                '        File wrong = select_first([ref_override, "data/refdir"])\n'
                "    }\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(doc, zip_fn, logger)
            warnings = "\n".join(logs.output)
            # the wrong-kind candidate warns specifically; the defaults pass silently
            self.assertIn("data/refdir", warnings)
            self.assertIn("is not a file", warnings)
            self.assertNotIn("data/default.fa", warnings)
            self.assertNotIn("Cannot determine statically", warnings)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "data", "default.fa")))
            self.assertFalse(os.path.exists(os.path.join(source_dir, "data", "refdir", "a.txt")))
            # with automatic inclusion off, the harvested defaults are reported like
            # any other scanned literal reference
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(doc, zip_fn, logger, auto_add=False)
            warnings = "\n".join(logs.output)
            self.assertIn("data/default.fa", warnings)
            self.assertIn("automatic inclusion disabled", warnings)

    def test_zip_auto_add_summary_log(self):
        # automatic inclusion logs a one-line summary of files/bytes added, since it
        # can substantially grow an archive that previously held only WDL sources
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "data"))
            pathlib.Path(os.path.join(src, "data", "a.txt")).write_text("hello\n")
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                "version 1.2\n"
                "workflow w {\n"
                "    input {\n"
                '        File f = "data/a.txt"\n'
                "    }\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="INFO") as logs:
                WDL.Zip.build(doc, zip_fn, logger)
            self.assertIn("files: 1, bytes: 6", "\n".join(logs.output))

    @unittest.skipUnless(shutil.which("git"), "git executable not available")
    def test_zip_add_all_ancestor_gitignore(self):
        # the --add-all sweep defers to git: a .gitignore in the work-tree root, above
        # the WDL source directories, applies to files under them (the case the old
        # in-tree pathspec emulation missed), as do nested .gitignore files and
        # negations -- all with git's own outcomes
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "wf")
            os.makedirs(os.path.join(src, "lib"))
            pathlib.Path(os.path.join(testdir, ".gitignore")).write_text("*.secret\n")
            pathlib.Path(os.path.join(src, "lib", ".gitignore")).write_text(
                "!visible.secret\ninner_skip.txt\n"
            )
            pathlib.Path(os.path.join(src, "lib", "hidden.secret")).write_text("shh\n")
            pathlib.Path(os.path.join(src, "lib", "visible.secret")).write_text("ok\n")
            pathlib.Path(os.path.join(src, "lib", "visible.txt")).write_text("ok\n")
            pathlib.Path(os.path.join(src, "lib", "inner_skip.txt")).write_text("skip\n")
            pathlib.Path(os.path.join(src, "lib", "tasks.wdl")).write_text(
                "version 1.2\ntask t { command {} }\n"
            )
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                'version 1.2\nimport "lib/tasks.wdl"\nworkflow w {}\n'
            )
            self._init_git_repo(testdir)
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            WDL.Zip.build(doc, zip_fn, logging.getLogger("miniwdl_zip_test"), add_all=True)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "lib", "visible.txt")))
            # re-included by the negation in lib/.gitignore
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "lib", "visible.secret")))
            self.assertFalse(os.path.exists(os.path.join(source_dir, "lib", "hidden.secret")))
            self.assertFalse(os.path.exists(os.path.join(source_dir, "lib", "inner_skip.txt")))

    def test_zip_add_all_computed_warning_qualified(self):
        # under --add-all a computed path still warns (qualified): the sweep is not
        # proof of coverage for a statically unknowable path
        with contextlib.ExitStack() as cleanup:
            testdir, src = self._source_relative_fixture(cleanup)
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(doc, zip_fn, logger, add_all=True)
            warnings = "\n".join(logs.output)
            self.assertIn("computed_ref", warnings)
            self.assertIn("--add-all includes it only if", warnings)
            # the sweep did in fact include the computed path's target here
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "data/dynamic_A.txt")))

    def test_zip_partial_directory_warns(self):
        # whole-Directory coverage is verified file by file: staging one of two files
        # via --additional (with automatic inclusion off) does not cover the reference
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "data"))
            pathlib.Path(os.path.join(src, "data", "a")).write_text("a\n")
            pathlib.Path(os.path.join(src, "data", "b")).write_text("b\n")
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                "version 1.2\n"
                "workflow w {\n"
                "    input {\n"
                '        Directory d = "data"\n'
                "    }\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(
                    doc,
                    zip_fn,
                    logger,
                    auto_add=False,
                    additional_files=[os.path.join(src, "data", "a")],
                )
            warnings = "\n".join(logs.output)
            self.assertIn("not included in the archive", warnings)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "data", "a")))
            self.assertFalse(os.path.exists(os.path.join(source_dir, "data", "b")))

    def test_zip_directory_alias(self):
        # a Directory tree containing both a subdirectory and a symlink alias to it is
        # archived under both pathnames (aliases are not cycles)
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "data", "real"))
            pathlib.Path(os.path.join(src, "data", "real", "a")).write_text("a\n")
            os.symlink("real", os.path.join(src, "data", "alias"))
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                "version 1.2\n"
                "workflow w {\n"
                "    input {\n"
                '        Directory d = "data"\n'
                "    }\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertNoLogs(logger, level="WARNING"):
                WDL.Zip.build(doc, zip_fn, logger)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "data", "real", "a")))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "data", "alias", "a")))

    def test_zip_unsafe_tree_member_skips_whole_ref(self):
        # a Directory tree containing a symlink escaping the source directory is skipped
        # as a whole (with warnings), not partially included up to the bad member
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "data"))
            pathlib.Path(os.path.join(src, "data", "a.txt")).write_text("a\n")
            pathlib.Path(os.path.join(testdir, "outside.txt")).write_text("outside\n")
            os.symlink(
                os.path.join(testdir, "outside.txt"), os.path.join(src, "data", "evil.txt")
            )
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                "version 1.2\n"
                "workflow w {\n"
                "    input {\n"
                '        Directory d = "data"\n'
                "    }\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(doc, zip_fn, logger)
            warnings = "\n".join(logs.output)
            self.assertIn("Skipping source-relative path", warnings)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertFalse(os.path.exists(os.path.join(source_dir, "data", "a.txt")))
            self.assertFalse(os.path.exists(os.path.join(source_dir, "data", "evil.txt")))

    @unittest.skipUnless(shutil.which("git"), "git executable not available")
    def test_zip_add_all_excluded_import_root(self):
        # git never descends into a directory excluded by an ancestor .gitignore, so a
        # negation in the excluded directory's own .gitignore can't reopen it -- even
        # though the directory is also swept as an imported document's source root
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "lib"))
            pathlib.Path(os.path.join(src, ".gitignore")).write_text("lib/\n")
            pathlib.Path(os.path.join(src, "lib", ".gitignore")).write_text("!hidden.secret\n")
            pathlib.Path(os.path.join(src, "lib", "hidden.secret")).write_text("shh\n")
            pathlib.Path(os.path.join(src, "lib", "tasks.wdl")).write_text(
                "version 1.2\ntask t { command {} }\n"
            )
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                'version 1.2\nimport "lib/tasks.wdl"\nworkflow w {}\n'
            )
            self._init_git_repo(src)
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            WDL.Zip.build(doc, zip_fn, logging.getLogger("miniwdl_zip_test"), add_all=True)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertFalse(os.path.exists(os.path.join(source_dir, "lib", "hidden.secret")))

    def test_zip_add_all_outside_git_repo(self):
        # outside a git work tree, --add-all doesn't emulate .gitignore: every file is
        # swept (even ones a .gitignore names), and an info note says so
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(src)
            # the fixture premise is that src isn't in a git work tree; guard it, since
            # a shared machine could have a repository above the temp directory
            if shutil.which("git"):
                proc = subprocess.run(
                    ["git", "-C", src, "rev-parse", "--git-dir"],
                    capture_output=True,
                    check=False,
                )
                if proc.returncode == 0:
                    self.skipTest("temporary directory is inside a git work tree")
            pathlib.Path(os.path.join(src, ".gitignore")).write_text("*.log\n")
            pathlib.Path(os.path.join(src, "skip.log")).write_text("log\n")
            pathlib.Path(os.path.join(src, "keep.txt")).write_text("ok\n")
            pathlib.Path(os.path.join(src, "main.wdl")).write_text("version 1.2\nworkflow w {}\n")
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="INFO") as logs:
                WDL.Zip.build(doc, zip_fn, logger, add_all=True)
            self.assertIn("not in a git work tree", "\n".join(logs.output))
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "skip.log")))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "keep.txt")))

    @unittest.skipUnless(shutil.which("git"), "git executable not available")
    def test_zip_add_all_non_ascii_names(self):
        # tracked files with non-ASCII names are swept: git quotes such names in
        # ls-files output unless -z is used, and the quoted name matches nothing on
        # disk (the GitPython-based enumeration silently dropped them)
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(src)
            pathlib.Path(os.path.join(src, "main.wdl")).write_text("version 1.2\nworkflow w {}\n")
            pathlib.Path(os.path.join(src, "tracked café.txt")).write_text("tracked\n")
            pathlib.Path(os.path.join(src, "données.txt")).write_text("untracked\n")
            self._init_git_repo(src)
            self._git(src, "add", "tracked café.txt")
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            WDL.Zip.build(doc, zip_fn, logging.getLogger("miniwdl_zip_test"), add_all=True)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "tracked café.txt")))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "données.txt")))

    def test_zip_add_all_git_unavailable_plain_dir(self):
        # with no usable git executable and no evidence of a repository, --add-all
        # falls back to sweeping everything, exactly as outside a work tree (bug:
        # GitPython raised ImportError at import, crashing even plain directories)
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(src)
            if shutil.which("git"):
                proc = subprocess.run(
                    ["git", "-C", src, "rev-parse", "--git-dir"],
                    capture_output=True,
                    check=False,
                )
                if proc.returncode == 0:
                    self.skipTest("temporary directory is inside a git work tree")
            pathlib.Path(os.path.join(src, "keep.txt")).write_text("ok\n")
            pathlib.Path(os.path.join(src, "main.wdl")).write_text("version 1.2\nworkflow w {}\n")
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="INFO") as logs, self._git_unavailable():
                WDL.Zip.build(doc, zip_fn, logger, add_all=True)
            self.assertIn("not in a git work tree", "\n".join(logs.output))
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "keep.txt")))

    def test_zip_add_all_git_unavailable_repo_evidence(self):
        # with no usable git executable, evidence within the swept tree that it belongs
        # to a repository -- a .git directory in it, or a .git file below it (as in a
        # submodule) -- makes --add-all stop with an error instead of sweeping
        # everything and bypassing the ignore rules the user relies on
        for evidence in ["git_dir", "nested_git_file"]:
            with self.subTest(evidence=evidence), contextlib.ExitStack() as cleanup:
                testdir = cleanup.enter_context(
                    tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_")
                )
                src = os.path.join(testdir, "src")
                os.makedirs(src)
                if evidence == "git_dir":
                    os.makedirs(os.path.join(src, ".git"))
                else:
                    os.makedirs(os.path.join(src, "sub"))
                    pathlib.Path(os.path.join(src, "sub", ".git")).write_text(
                        "gitdir: ../.git/modules/sub\n"
                    )
                pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                    "version 1.2\nworkflow w {}\n"
                )
                doc = WDL.load(os.path.join(src, "main.wdl"))
                zip_fn = os.path.join(testdir, "source.zip")
                logger = logging.getLogger("miniwdl_zip_test")
                with self._git_unavailable(), self.assertRaisesRegex(
                    WDL.Error.InputError, "install git, or use --add instead of --add-all"
                ):
                    WDL.Zip.build(doc, zip_fn, logger, add_all=True)

    def test_zip_add_all_git_unavailable_tree_gitignore(self):
        # a .gitignore anywhere in the swept tree is also repository evidence when
        # git can't be run (here it's the only evidence: no .git anywhere)
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "sub"))
            pathlib.Path(os.path.join(src, "sub", ".gitignore")).write_text("*.log\n")
            pathlib.Path(os.path.join(src, "main.wdl")).write_text("version 1.2\nworkflow w {}\n")
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self._git_unavailable(), self.assertRaisesRegex(
                WDL.Error.InputError, "install git, or use --add instead of --add-all"
            ):
                WDL.Zip.build(doc, zip_fn, logger, add_all=True)

    def test_zip_add_all_git_unavailable_ancestor_evidence_ignored(self):
        # repository evidence is only sought where the sweep looks anyway: a .git or
        # .gitignore in an ancestor of the source directory doesn't stop the fallback
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            os.makedirs(os.path.join(testdir, ".git"))
            pathlib.Path(os.path.join(testdir, ".gitignore")).write_text("*.txt\n")
            src = os.path.join(testdir, "src")
            os.makedirs(src)
            pathlib.Path(os.path.join(src, "keep.txt")).write_text("ok\n")
            pathlib.Path(os.path.join(src, "main.wdl")).write_text("version 1.2\nworkflow w {}\n")
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="INFO") as logs, self._git_unavailable():
                WDL.Zip.build(doc, zip_fn, logger, add_all=True)
            self.assertIn("git is unavailable", "\n".join(logs.output))
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "keep.txt")))

    def test_zip_add_all_git_messages_untranslated(self):
        # git runs with LC_ALL=C, so its "not a git repository" message can be
        # recognized even where git would otherwise print it translated; a .gitignore
        # in a plain (non-repository) tree then doesn't stop the fallback sweep
        real_run = subprocess.run

        def run(cmd, *args, **kwargs):
            if cmd and cmd[0] == "git":
                if (kwargs.get("env") or {}).get("LC_ALL") == "C":
                    stderr = b"fatal: not a git repository (or any of the parent directories): .git\n"
                else:
                    stderr = b"fatal: Kein Git-Repository (oder irgendeines der Elternverzeichnisse): .git\n"
                return subprocess.CompletedProcess(cmd, 128, b"", stderr)
            return real_run(cmd, *args, **kwargs)

        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(src)
            pathlib.Path(os.path.join(src, ".gitignore")).write_text("*.log\n")
            pathlib.Path(os.path.join(src, "keep.txt")).write_text("ok\n")
            pathlib.Path(os.path.join(src, "main.wdl")).write_text("version 1.2\nworkflow w {}\n")
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with unittest.mock.patch.object(subprocess, "run", run):
                WDL.Zip.build(doc, zip_fn, logger, add_all=True)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "keep.txt")))

    @unittest.skipUnless(shutil.which("git"), "git executable not available")
    def test_zip_add_all_submodule_not_swept(self):
        # git lists a submodule as a single gitlink entry: --add-all doesn't sweep
        # its contents, and warns about the skip
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            sub_src = os.path.join(testdir, "sub_src")
            os.makedirs(sub_src)
            pathlib.Path(os.path.join(sub_src, "inner.txt")).write_text("inner\n")
            src = os.path.join(testdir, "src")
            os.makedirs(src)
            pathlib.Path(os.path.join(src, "main.wdl")).write_text("version 1.2\nworkflow w {}\n")
            try:
                self._init_git_repo(sub_src)
                self._git(
                    sub_src,
                    "-c",
                    "user.email=miniwdl@test",
                    "-c",
                    "user.name=miniwdl",
                    "add",
                    "inner.txt",
                )
                self._git(
                    sub_src,
                    "-c",
                    "user.email=miniwdl@test",
                    "-c",
                    "user.name=miniwdl",
                    "commit",
                    "-q",
                    "-m",
                    "inner",
                )
                self._init_git_repo(src)
                self._git(
                    src,
                    "-c",
                    "protocol.file.allow=always",
                    "submodule",
                    "add",
                    "-q",
                    sub_src,
                    "sub",
                )
            except subprocess.CalledProcessError:
                self.skipTest("could not build a submodule fixture in this environment")
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(doc, zip_fn, logger, add_all=True)
            self.assertIn("submodule", "\n".join(logs.output))
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertFalse(os.path.exists(os.path.join(source_dir, "sub", "inner.txt")))

    def test_zip_missing_empty_descendant_warns(self):
        # directory coverage checks directories too: staging every file of a Directory
        # target but not its empty subdirectory does not cover the reference
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "data", "hollow"))
            pathlib.Path(os.path.join(src, "data", "a")).write_text("a\n")
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                "version 1.2\n"
                "workflow w {\n"
                "    input {\n"
                '        Directory d = "data"\n'
                "    }\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(
                    doc,
                    zip_fn,
                    logger,
                    auto_add=False,
                    additional_files=[os.path.join(src, "data", "a")],
                )
            warnings = "\n".join(logs.output)
            self.assertIn("not included in the archive", warnings)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "data", "a")))
            self.assertFalse(os.path.exists(os.path.join(source_dir, "data", "hollow")))

    @unittest.skipUnless(shutil.which("git"), "git executable not available")
    def test_zip_add_all_ignored_intermediate(self):
        # an intermediate directory excluded by a shallower .gitignore is never
        # descended into: its own .gitignore can't negate a deeper imported sweep
        # root back into the sweep
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "lib", "deep"))
            pathlib.Path(os.path.join(src, ".gitignore")).write_text("lib/\n")
            pathlib.Path(os.path.join(src, "lib", ".gitignore")).write_text("!deep/\n")
            pathlib.Path(os.path.join(src, "lib", "deep", "hidden.secret")).write_text("shh\n")
            pathlib.Path(os.path.join(src, "lib", "deep", "tasks.wdl")).write_text(
                "version 1.2\ntask t { command {} }\n"
            )
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                'version 1.2\nimport "lib/deep/tasks.wdl"\nworkflow w {}\n'
            )
            self._init_git_repo(src)
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            WDL.Zip.build(doc, zip_fn, logging.getLogger("miniwdl_zip_test"), add_all=True)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertFalse(
                os.path.exists(os.path.join(source_dir, "lib", "deep", "hidden.secret"))
            )

    @unittest.skipUnless(shutil.which("git"), "git executable not available")
    def test_zip_add_all_negation_cannot_reopen_pruned_subtree(self):
        # a negation for a deeper directory in the same .gitignore (!lib/deep/) cannot
        # reopen it while its parent (lib/) stays excluded -- git prunes at the parent
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "lib", "deep"))
            pathlib.Path(os.path.join(src, ".gitignore")).write_text("lib/\n!lib/deep/\n")
            pathlib.Path(os.path.join(src, "lib", "deep", "hidden.secret")).write_text("shh\n")
            pathlib.Path(os.path.join(src, "lib", "deep", "tasks.wdl")).write_text(
                "version 1.2\ntask t { command {} }\n"
            )
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                'version 1.2\nimport "lib/deep/tasks.wdl"\nworkflow w {}\n'
            )
            self._init_git_repo(src)
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            WDL.Zip.build(doc, zip_fn, logging.getLogger("miniwdl_zip_test"), add_all=True)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            self.assertFalse(
                os.path.exists(os.path.join(source_dir, "lib", "deep", "hidden.secret"))
            )

    def test_zip_nested_compound_any_warns(self):
        # an Any-typed value nested inside a compound expression (here, select_first
        # over an Array[Any]) feeding File positions is unprovable and warns
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "data"))
            pathlib.Path(os.path.join(src, "config.json")).write_text('["data/a"]\n')
            pathlib.Path(os.path.join(src, "data", "a")).write_text("a\n")
            pathlib.Path(os.path.join(src, "main.wdl")).write_text(
                "version 1.2\n"
                "workflow w {\n"
                "    input {\n"
                '        Array[Array[File]] fs = select_first([[read_json("config.json")]])\n'
                "    }\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(doc, zip_fn, logger)
            warnings = "\n".join(logs.output)
            self.assertIn("Cannot determine statically", warnings)
            self.assertIn("fs", warnings)

    def test_zip_placement_containment(self):
        # the mentioning document sits at the tree root and the importing document in
        # a subdirectory of it: placement follows the mentioning document's own
        # archive directory, wherever the archive layout puts that document
        with contextlib.ExitStack() as cleanup:
            testdir = cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            src = os.path.join(testdir, "src")
            os.makedirs(os.path.join(src, "app", "data"))
            pathlib.Path(os.path.join(src, "app", "data", "a.txt")).write_text("shared\n")
            pathlib.Path(os.path.join(src, "tasks.wdl")).write_text(
                "version 1.2\n"
                "task t {\n"
                "    input {\n"
                '        File f = "app/data/a.txt"\n'
                "    }\n"
                "    command { cat ~{f} }\n"
                "    output { String o = read_string(stdout()) }\n"
                "}\n"
            )
            pathlib.Path(os.path.join(src, "app", "main.wdl")).write_text(
                "version 1.2\n"
                'import "../tasks.wdl" as lib\n'
                "workflow w {\n"
                "    call lib.t\n"
                "}\n"
            )
            doc = WDL.load(os.path.join(src, "app", "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            with self.assertLogs(logger, level="WARNING") as logs:
                WDL.Zip.build(doc, zip_fn, logger)
            warnings = "\n".join(logs.output)
            # the outside-import notice is expected; no coverage warnings may fire
            self.assertNotIn("not included in the archive", warnings)
            with zipfile.ZipFile(zip_fn) as zf:
                names = zf.namelist()
            # tasks.wdl is stored under __outside_wdl/, and its literal resolves
            # relative to its own (unpacked) directory: __outside_wdl/app/data/a.txt.
            # A global prefix match against the main document's directory would have
            # placed the very same file at data/a.txt, where the task never looks.
            self.assertIn("__outside_wdl/app/data/a.txt", names)
            self.assertNotIn("data/a.txt", names)
            self.assertNotIn("app/app/data/a.txt", names)

    def test_zip_copy_failure_warns(self):
        # a failure copying an automatically-discovered file warns and continues (with no
        # partial file staged), instead of failing the archive; other files still land
        with contextlib.ExitStack() as cleanup:
            testdir, src = self._source_relative_fixture(cleanup)
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            logger = logging.getLogger("miniwdl_zip_test")
            real_copyfile = shutil.copyfile

            def flaky_copyfile(src_path, dst_path, *args, **kwargs):
                if str(src_path).endswith("literal.txt"):
                    pathlib.Path(dst_path).write_text("partial\n")
                    raise PermissionError("mock copy failure")
                return real_copyfile(src_path, dst_path, *args, **kwargs)

            with (
                unittest.mock.patch.object(shutil, "copyfile", flaky_copyfile),
                self.assertLogs(logger, level="WARNING") as logs,
            ):
                WDL.Zip.build(doc, zip_fn, logger)
            warnings = "\n".join(logs.output)
            self.assertIn("could not copy it", warnings)
            self.assertIn("literal.txt", warnings)
            source_dir, _main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            # no partial file left behind, and the rest of the archive is intact
            self.assertFalse(os.path.exists(os.path.join(source_dir, "data", "literal.txt")))
            self.assertTrue(os.path.isfile(os.path.join(source_dir, "data", "callbound.txt")))

    def test_zip_unpack_and_resolve(self):
        # end-to-end: with the original source tree moved away and an unrelated working
        # directory, the unpacked archive's literal defaults resolve through the runner's
        # own source-relative resolution path to files inside the unpacked tree
        from WDL.runtime import _io_helpers as runtime_io_helpers

        with contextlib.ExitStack() as cleanup:
            testdir, src = self._source_relative_fixture(cleanup)
            doc = WDL.load(os.path.join(src, "main.wdl"))
            zip_fn = os.path.join(testdir, "source.zip")
            WDL.Zip.build(doc, zip_fn, logging.getLogger("miniwdl_zip_test"))
            os.rename(src, os.path.join(testdir, "src_gone"))
            elsewhere = cleanup.enter_context(
                tempfile.TemporaryDirectory(prefix="miniwdl_zip_elsewhere_")
            )
            cfg = WDL.runtime.config.Loader(logging.getLogger("miniwdl_zip_test"))
            cwd = os.getcwd()
            try:
                # unpack, load, and resolve all while chdir'd into the unrelated
                # directory, with the original source tree renamed away
                os.chdir(elsewhere)
                source_dir, main_wdl, _inputs_file = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
                unpacked_doc = WDL.load(os.path.join(source_dir, main_wdl))
                decls = {b.name: b.value for b in unpacked_doc.workflow.available_inputs}
                resolved_file = runtime_io_helpers.resolve_source_relative_paths(
                    cfg,
                    decls["literal_ref"].source_dir,
                    WDL.Value.File("data/literal.txt"),
                    decls["literal_ref"].type,
                    "Workflow input",
                )
                resolved_dir = runtime_io_helpers.resolve_source_relative_paths(
                    cfg,
                    decls["ref_dir"].source_dir,
                    WDL.Value.Directory("data/refdir"),
                    decls["ref_dir"].type,
                    "Workflow input",
                )
            finally:
                os.chdir(cwd)
            self.assertEqual(
                resolved_file.value.value, os.path.join(source_dir, "data", "literal.txt")
            )
            self.assertTrue(os.path.isfile(resolved_file.value.value))
            self.assertEqual(resolved_dir.value.value, os.path.join(source_dir, "data", "refdir"))
            self.assertTrue(os.path.isdir(resolved_dir.value.value))

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
            # realpath() because unpack_source_zip() resolves file_io_root with realpath to choose
            # the unpack tempdir; on macOS the system temp dir is under symlinks (/tmp, /var ->
            # /private/...) so the unpacked paths would otherwise not match the lexical testdir.
            testdir = os.path.realpath(
                cleanup.enter_context(tempfile.TemporaryDirectory(prefix="miniwdl_zip_test_"))
            )
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

            # an already-unpacked directory under file_io.root is used in place
            unpacked = cleanup.enter_context(WDL.Zip.unpack(zip_fn, tempdir_parent=root))
            self.assertEqual(
                WDL.CLI.unpack_source_zip(logger, cleanup, unpacked.dir, root),
                (unpacked.main_wdl, None),
            )

            # but one outside file_io.root fails fast (issue #878)
            outside = cleanup.enter_context(WDL.Zip.unpack(zip_fn))
            with self.assertRaisesRegex(WDL.Error.InputError, "outside file_io.root") as exn:
                WDL.CLI.unpack_source_zip(logger, cleanup, outside.dir, root)
            self.assertIn(outside.dir, str(exn.exception))
            self.assertIn(root, str(exn.exception))

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
