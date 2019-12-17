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
    parser.add_argument("wdl", metavar="FILE", help="WDL document containing a workflow")
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
    doc = WDL.load(args.wdl, args.path or [], check_quant=args.check_quant, read_source=read_source)
    assert doc.workflow, "No workflow in WDL document"

    # visualize workflow
    dot = wdlviz(doc)
    print(dot.source)
    dot.render(os.path.basename(args.wdl) + ".dot", view=True)


def wdlviz(doc: WDL.Document):
    """
    Project the workflow's built-in dependency graph onto a graphviz representation
    """
    # References:
    # 1. WDL object model -- https://miniwdl.readthedocs.io/en/latest/WDL.html#module-WDL.Tree
    # 2. graphviz API -- https://graphviz.readthedocs.io/en/stable/manual.html

    # initialiaze Digraph
    top = graphviz.Digraph(comment=doc.workflow.name)
    top.attr(compound="true", rankdir="LR")

    # recursively add graphviz nodes for each decl/call/scatter/conditional workflow node.
    # some global bookkeeping:
    gather_referees = {}  # final_referee of each Gather workflow node
    cluster_lheads = {}  # cluster subgraph name for each scatter/conditional section

    def add_node(graph: graphviz.Digraph, node: WDL.WorkflowNode):
        if isinstance(node, WDL.WorkflowSection):
            # scatter/conditional section: add a cluster subgraph to contain its body
            with graph.subgraph(name="cluster-" + node.workflow_node_id) as sg:
                label = "scatter" if isinstance(node, WDL.Scatter) else "if"
                sg.attr(label=label + f"({str(node.expr)})", rank="same")
                for child in node.body:
                    add_node(sg, child)
                # Gather node bookkeeping
                for gather in node.gathers.values():
                    gather_referees[gather.workflow_node_id] = gather.final_referee.workflow_node_id
                # Add an invisible node inside the subgraph which is just to provide a sink for the
                # dependency edges with this cluster as their lhead
                sg.node(node.workflow_node_id, "", style="invis", height="0", width="0", margin="0")
                cluster_lheads[node.workflow_node_id] = sg.name
        elif isinstance(node, WDL.Decl):
            # non-input value declaration
            assert node.workflow_node_id.startswith("decl-")
            graph.node(node.workflow_node_id, node.workflow_node_id[5:], shape="plaintext")
        elif isinstance(node, WDL.Call):
            # task/subworkflow call
            graph.node(node.workflow_node_id, node.workflow_node_id, shape="cds")
        else:
            assert False, node.__class__.__name__

    for elt in doc.workflow.body:
        add_node(top, elt)

    # cluster of the input decls
    with top.subgraph(name="cluster-inputs") as sg:
        for inp in doc.workflow.inputs or []:
            assert inp.workflow_node_id.startswith("decl-")
            sg.node(inp.workflow_node_id, inp.workflow_node_id[5:], shape="plaintext")
        sg.attr(label="inputs")

    # cluster of the output decls
    with top.subgraph(name="cluster-outputs") as sg:
        for outp in doc.workflow.outputs or []:
            assert outp.workflow_node_id.startswith("output-")
            sg.node(outp.workflow_node_id, outp.workflow_node_id[7:], shape="plaintext")
        sg.attr(label="outputs")

    # add edge for each dependency between workflow nodes
    def add_edges(node):
        for pred in node.workflow_node_dependencies:
            # leave Gather nodes invisible by replacing any dependencies on them with their
            # final_referee
            pred = gather_referees.get(pred, pred)
            top.edge(
                pred, node.workflow_node_id, lhead=cluster_lheads.get(node.workflow_node_id, None)
            )
        if isinstance(node, WDL.WorkflowSection):
            for child in node.body:
                add_edges(child)

    for node in (doc.workflow.inputs or []) + doc.workflow.body + (doc.workflow.outputs or []):
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
