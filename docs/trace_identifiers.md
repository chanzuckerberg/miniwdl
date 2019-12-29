# Syntax tree traversal

In this lab, we'll develop a short Python script to traverse miniwdl's abstract syntax tree (AST) for a WDL document. Our script will identify each instance of an identifier expression naming an `Array` value, and report the source code location of the value's original definition.

Begin by installing the miniwdl package with either pip3 or conda, as you prefer. Then start a new Python3 script `trace_identifiers.py`:

```python3
#!/usr/bin/env python3
import os
import sys
import WDL


def main(args):
    doc = WDL.load(args[0] if args else "/dev/stdin")
    trace_identifiers(doc)
```

This prologue loads the WDL document from either a given filename or standard input. Then we call our to-be-shown `trace_identifiers()` function to traverse the document AST.

```python3
def trace_identifiers(obj):
    if isinstance(obj, WDL.Document):
        if obj.workflow:
            trace_identifiers(obj.workflow)
        for task in obj.tasks:
            trace_identifiers(task)
    elif isinstance(obj, WDL.Workflow):
        for ch in (obj.inputs or []) + obj.body + (obj.outputs or []):
            trace_identifiers(ch)
    elif isinstance(obj, WDL.Task):
        for ch in (
            (obj.inputs or [])
            + obj.postinputs
            + [obj.command]
            + obj.outputs
            + list(obj.runtime.values())
        ):
            trace_identifiers(ch)
```

`trace_identifiers` recursively descends through the WDL document to find all Array identifier expressions. It begins with the workflow and tasks in the top-level `Document` object, if any. The `Document` object may also include imports and struct type definitions, which we won't use here.

For the workflow, we descend into its body and its `input{}` and `output{}` sections, if any. For each task, we descend into all its value declarations, the command template, and any expressions in the runtime section. 

```python3
    elif isinstance(obj, WDL.WorkflowSection):
        trace_identifiers(obj.expr)
        for ch in obj.body:
            trace_identifiers(ch)
    elif isinstance(obj, WDL.Call):
        for rhs in obj.inputs.values():
            trace_identifiers(rhs)
    elif isinstance(obj, WDL.Decl):
        trace_identifiers(obj.expr)
```

At the next level, we may need to descend into `scatter` and `if` sections within a workflow (which can be nested), the expressions in `call` inputs, and the expressions in value declarations.

```python3
    elif isinstance(obj, WDL.Expr.Base):
        for ch in obj.children:
            trace_identifiers(ch)
```

WDL expressions form their own tree structure, e.g. `x * size(some_call.file_output, "GB") + 1`. Each expression object (derived from `WDL.Expr.Base`) exposes an iterable `children` attribute.

```python3
    if isinstance(obj, WDL.Expr.Ident) and isinstance(obj.type, WDL.Type.Array):
        print(
            f"L{obj.pos.line} Array[{obj.type.item_type}] {obj.name}"
            + " defined on "
            + f"L{obj.referee.pos.line}"
        )


if __name__ == "__main__":
    main(sys.argv[1:])
```

At last, we may reach individual identifier expressions ([`WDL.Expr.Ident`](https://miniwdl.readthedocs.io/en/latest/WDL.html#WDL.Expr.Ident)), which miniwdl decorates with the referred-to object ("referee") and data type. Here we check if the data type, represented as an object deriving from `WDL.Type.Base`, is an array; and if so, we can also get its parametric `item_type`. Each AST node exposes a `pos` with the original source code position (`line`, `column`, `end_line` and `end_column`), which we use to report the identifier and referee line numbers.

The referee object might be any of the following:

1. `Decl` for a value declaration
2. `Call` for a call output (the identifier is namespaced by the call name)
3. `Scatter` for use of a scatter variable
4. `Gather` for reference to a value defined inside a `scatter`/`if` section

The last case [`Gather`](https://miniwdl.readthedocs.io/en/latest/WDL.html#WDL.Tree.Gather) is a concept miniwdl synthesizes to model WDL's implicit special meaning of a value inside a `scatter`/`if` section, when seen from outside of that section. For example, an `Int` value in a `scatter` section is an `Array[Int]` elsewhere. Hence, a tricky detail: the identifier's `obj.type` may differ from that of the original value or call output.

**Try it out**

```bash
$ python3 trace_identifiers.py << 'EOF'
    version 1.0
    workflow sum_sq {
        input {
            Int x
        }
        scatter (i in range(x)) {
            Int sq = (i+1)*(i+1)
        }
        call sum {
            input: x = sq
        }
    }
    task sum {
        input {
            Array[Int] x
        }
        command <<<
            awk 'BEGIN {z=0;} {z+=$0;} END {print z;}' "~{write_lines(x)}"
        >>>
        output {
            Int z = read_int(stdout())
        }
    }
EOF
L10 Array[Int] sq defined on L7
L18 Array[Int] x defined on L15
```

Or on a real workflow:

```
$ wget https://raw.githubusercontent.com/gatk-workflows/gatk4-germline-snps-indels/master/joint-discovery-gatk4-local.wdl
$ python3 trace_identifiers.py joint-discovery-gatk4-local.wdl
L103 Array[File] input_gvcfs defined on L48
L117 Array[String] unpadded_intervals defined on L115
L124 Array[String] sample_names defined on L47
L125 Array[String] unpadded_intervals defined on L115
L127 Array[File] input_gvcfs defined on L48
L128 Array[File] input_gvcfs_indices defined on L49
L138 Array[String] unpadded_intervals defined on L115
L165 Array[File] HardFilterAndMakeSitesOnlyVcf.sites_only_vcf defined on L150
L166 Array[File] HardFilterAndMakeSitesOnlyVcf.sites_only_vcf_index defined on L150
L179 Array[String] indel_recalibration_tranche_values defined on L61
L180 Array[String] indel_recalibration_annotation_values defined on L62
L199 Array[String] snp_recalibration_tranche_values defined on L59
L200 Array[String] snp_recalibration_annotation_values defined on L60
L216 Array[File] HardFilterAndMakeSitesOnlyVcf.sites_only_vcf defined on L150
L219 Array[File] HardFilterAndMakeSitesOnlyVcf.sites_only_vcf defined on L150
L220 Array[File] HardFilterAndMakeSitesOnlyVcf.sites_only_vcf_index defined on L150
L223 Array[String] snp_recalibration_tranche_values defined on L59
L224 Array[String] snp_recalibration_annotation_values defined on L60
L241 Array[File] SNPsVariantRecalibratorScattered.tranches defined on L217
L256 Array[String] snp_recalibration_tranche_values defined on L59
L257 Array[String] snp_recalibration_annotation_values defined on L60
L276 Array[File] HardFilterAndMakeSitesOnlyVcf.variant_filtered_vcf defined on L150
L280 Array[File] HardFilterAndMakeSitesOnlyVcf.variant_filtered_vcf defined on L150
L281 Array[File] HardFilterAndMakeSitesOnlyVcf.variant_filtered_vcf_index defined on L150
L285 Array[File] SNPsVariantRecalibratorScattered.recalibration defined on L217
L285 Array[File] SNPsVariantRecalibratorScattered.recalibration defined on L217
L286 Array[File] SNPsVariantRecalibratorScattered.recalibration_index defined on L217
L286 Array[File] SNPsVariantRecalibratorScattered.recalibration_index defined on L217
L317 Array[File] ApplyRecalibration.recalibrated_vcf defined on L277
L318 Array[File] ApplyRecalibration.recalibrated_vcf_index defined on L277
L344 Array[File?] CollectMetricsSharded.detail_metrics_file defined on L297
L345 Array[File?] CollectMetricsSharded.summary_metrics_file defined on L297
L401 Array[File] input_gvcfs defined on L385
L402 Array[String] sample_names defined on L384
L560 Array[String] recalibration_tranche_values defined on L536
L561 Array[String] recalibration_annotation_values defined on L537
L614 Array[String] recalibration_tranche_values defined on L588
L615 Array[String] recalibration_annotation_values defined on L589
L668 Array[String] recalibration_tranche_values defined on L642
L669 Array[String] recalibration_annotation_values defined on L643
L707 Array[File] input_fofn defined on L693
L794 Array[File] input_vcfs_fofn defined on L777
L868 Array[File] input_details_fofn defined on L852
```

**Complete code listing**

```python3
#!/usr/bin/env python3
import os
import sys
import WDL


def main(args):
    doc = WDL.load(args[0] if args else "/dev/stdin")
    trace_identifiers(doc)


def trace_identifiers(obj):
    if isinstance(obj, WDL.Document):
        if obj.workflow:
            trace_identifiers(obj.workflow)
        for task in obj.tasks:
            trace_identifiers(task)
    elif isinstance(obj, WDL.Workflow):
        for ch in (obj.inputs or []) + obj.body + (obj.outputs or []):
            trace_identifiers(ch)
    elif isinstance(obj, WDL.Task):
        for ch in (
            (obj.inputs or [])
            + obj.postinputs
            + [obj.command]
            + obj.outputs
            + list(obj.runtime.values())
        ):
            trace_identifiers(ch)
    elif isinstance(obj, WDL.WorkflowSection):
        trace_identifiers(obj.expr)
        for ch in obj.body:
            trace_identifiers(ch)
    elif isinstance(obj, WDL.Call):
        for rhs in obj.inputs.values():
            trace_identifiers(rhs)
    elif isinstance(obj, WDL.Decl):
        trace_identifiers(obj.expr)
    elif isinstance(obj, WDL.Expr.Base):
        for ch in obj.children:
            trace_identifiers(ch)

    if isinstance(obj, WDL.Expr.Ident) and isinstance(obj.type, WDL.Type.Array):
        print(
            f"L{obj.pos.line} Array[{obj.type.item_type}] {obj.name}"
            + " defined on "
            + f"L{obj.referee.pos.line}"
        )


if __name__ == "__main__":
    main(sys.argv[1:])
```
