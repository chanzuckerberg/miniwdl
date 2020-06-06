#!/usr/bin/env python3
"""
Visualize a WDL workflow using miniwdl and graphviz
"""
# black -l 100 wdlviz.py && pylint wdlviz.py
import os
import sys
import argparse
import urllib
import tempfile
import WDL
import graphviz


def main(args=None):
    # read command-line arguments
    parser = argparse.ArgumentParser(
        description="Visualize a WDL workflow using miniwdl and graphviz"
    )
    parser.add_argument(
        "wdl", metavar="FILE", help="WDL workflow file or URL (- for standard input)"
    )
    parser.add_argument("--inputs", action="store_true", help="include input declarations")
    parser.add_argument("--outputs", action="store_true", help="include output declarations")
    parser.add_argument(
        "--no-quant-check",
        dest="check_quant",
        action="store_false",
        help="relax static typechecking of optional types, and permit coercion of T to Array[T] (discouraged; for backwards compatibility with older WDL)",
    )
    parser.add_argument(
        "-p",
        "--path",
        metavar="DIR",
        type=str,
        action="append",
        help="local directory to search for imports",
    )
    args = parser.parse_args(args if args is not None else sys.argv[1:])

    # load WDL document
    doc = WDL.load(
        args.wdl if args.wdl != "-" else "/dev/stdin",
        args.path or [],
        check_quant=args.check_quant,
        read_source=read_source,
    )
    assert doc.workflow, "No workflow in WDL document"

    # visualize workflow
    dot = wdlviz(doc.workflow, args.inputs, args.outputs)
    print(dot.source)
    dot.render(doc.workflow.name + ".dot", view=True)


def wdlviz(workflow: WDL.Workflow, inputs=False, outputs=False):
    """
    Project the workflow's built-in dependency graph onto a graphviz representation
    """
    # References:
    # 1. WDL object model -- https://miniwdl.readthedocs.io/en/latest/WDL.html#module-WDL.Tree
    # 2. graphviz API -- https://graphviz.readthedocs.io/en/stable/manual.html

    # initialiaze Digraph
    top = graphviz.Digraph(comment=workflow.name)
    top.attr(compound="true", rankdir="LR")
    fontname = "Roboto"
    top.attr("node", fontname=fontname)
    top.attr("edge", color="#00000080")
    node_ids = set()

    # recursively add graphviz nodes for each decl/call/scatter/conditional workflow node.

    def add_node(graph: graphviz.Digraph, node: WDL.WorkflowNode):
        nonlocal node_ids
        if isinstance(node, WDL.WorkflowSection):
            # scatter/conditional section: add a cluster subgraph to contain its body
            with graph.subgraph(name="cluster-" + node.workflow_node_id) as sg:
                label = "scatter" if isinstance(node, WDL.Scatter) else "if"
                sg.attr(label=label + f"({str(node.expr)})", fontname=fontname, rank="same")
                for child in node.body:
                    add_node(sg, child)
                # Add an invisible node inside the subgraph, which provides a sink for dependencies
                # of the scatter/conditional expression itself
                sg.node(node.workflow_node_id, "", style="invis", height="0", width="0", margin="0")
            node_ids.add(node.workflow_node_id)
            node_ids |= set(g.workflow_node_id for g in node.gathers.values())
        elif isinstance(node, WDL.Call) or (
            isinstance(node, WDL.Decl)
            and (inputs or node_ids.intersection(node.workflow_node_dependencies))
        ):
            # node for call or decl
            graph.node(
                node.workflow_node_id,
                node.name,
                shape=("cds" if isinstance(node, WDL.Call) else "plaintext"),
            )
            node_ids.add(node.workflow_node_id)

    for node in workflow.body:
        add_node(top, node)

    # cluster of the input decls
    if inputs:
        with top.subgraph(name="cluster-inputs") as sg:
            for inp in workflow.inputs or []:
                assert inp.workflow_node_id.startswith("decl-")
                sg.node(inp.workflow_node_id, inp.workflow_node_id[5:], shape="plaintext")
                node_ids.add(inp.workflow_node_id)
            sg.attr(label="inputs", fontname=fontname)

    # cluster of the output decls
    if outputs:
        with top.subgraph(name="cluster-outputs") as sg:
            for outp in workflow.outputs or []:
                assert outp.workflow_node_id.startswith("output-")
                sg.node(outp.workflow_node_id, outp.workflow_node_id[7:], shape="plaintext")
                node_ids.add(outp.workflow_node_id)
            sg.attr(label="outputs", fontname=fontname)

    # add edge for each dependency between workflow nodes
    def add_edges(node):
        for dep_id in node.workflow_node_dependencies:
            dep = workflow.get_node(dep_id)
            # leave Gather nodes invisible by replacing any dependencies on them with their
            # final_referee
            if isinstance(dep, WDL.Tree.Gather):
                dep = dep.final_referee
                dep_id = dep.workflow_node_id
            if dep_id in node_ids and node.workflow_node_id in node_ids:
                lhead = None
                if isinstance(node, WDL.WorkflowSection):
                    lhead = "cluster-" + node.workflow_node_id
                top.edge(dep_id, node.workflow_node_id, lhead=lhead)
        if isinstance(node, WDL.WorkflowSection):
            for child in node.body:
                add_edges(child)

    for node in (workflow.inputs or []) + workflow.body + (workflow.outputs or []):
        add_edges(node)

    return top


async def read_source(uri, path, importer):
    """
    This function helps miniwdl read the WDL source code directly from http[s] URIs.
    """
    if uri.startswith("http:") or uri.startswith("https:"):
        fn = os.path.join(
            tempfile.mkdtemp(prefix="miniwdl_import_uri_"),
            os.path.basename(urllib.parse.urlsplit(uri).path),
        )
        urllib.request.urlretrieve(uri, filename=fn)
        with open(fn, "r") as infile:
            return WDL.ReadSourceResult(infile.read(), uri)
    elif importer and (
        importer.pos.abspath.startswith("http:") or importer.pos.abspath.startswith("https:")
    ):
        assert not os.path.isabs(uri), "absolute import from downloaded WDL"
        return await read_source(urllib.parse.urljoin(importer.pos.abspath, uri), [], importer)
    return await WDL.read_source_default(uri, path, importer)


if __name__ == "__main__":
    main()
