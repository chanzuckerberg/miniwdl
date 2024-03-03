#!/usr/bin/env python3
"""
Visualize a WDL workflow using miniwdl and graphviz

pip3 install miniwdl graphviz
python3 wdlviz.py my_workflow.wdl
"""

# ruff check wdlviz.py && ruff format --line-length 100 wdlviz.py
import os
import sys
import argparse
import tempfile
import WDL
import graphviz
from urllib import request, parse


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
        "--rankdir",
        choices=("LR", "RL", "TB", "BT"),
        default="LR",
        help="layout orientation (default: LR)",
    )
    parser.add_argument(
        "--splines",
        choices=("spline", "curved", "compound", "ortho"),
        default="compound",
        help="edge shape (default: compound)",
    )
    parser.add_argument(
        "--no-subworkflow-edges",
        dest="subworkflow_edges",
        action="store_false",
        help="hide dotted edges from call to subworkflow",
    )
    parser.add_argument(
        "--no-render",
        dest="render",
        action="store_false",
        help="skip rendering; just print the graphviz source",
    )
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
    dot = wdlviz(
        doc.workflow, args.rankdir, args.splines, args.inputs, args.outputs, args.subworkflow_edges
    )
    print(dot.source)
    if args.render:
        dot.render(doc.workflow.name + ".dot", view=True)


def wdlviz(
    workflow: WDL.Workflow, rankdir, splines, inputs=False, outputs=False, subworkflow_edges=True
):
    """
    Project the workflow's built-in dependency graph onto a graphviz representation
    """
    # References:
    # 1. WDL AST object model -- https://miniwdl.readthedocs.io/en/latest/WDL.html#module-WDL.Tree
    # 2. graphviz API -- https://graphviz.readthedocs.io/en/stable/manual.html

    # initialiaze Digraph
    fontname = "Roboto"
    top = graphviz.Digraph()
    top.attr(
        label=f"<<B>{workflow.name}</B>>",
        labelloc="t",
        fontname=fontname,
        compound="true",
        rankdir=rankdir,
        splines=splines,
    )
    top.attr("node", fontname=fontname)
    top.attr("edge", color="#00000080")

    # Recursively process the workflow AST into the Digraph.
    # We'll identify nodes by their python id() instead of the workflow_node_id strings exposed
    # in the miniwdl AST. The latter are only unique within a given workflow, while for graphviz
    # we need globally unique id's (even when we have nested subworkflows).
    nodes_visited = set()

    def add_node(graph, workflow, node):
        nonlocal nodes_visited
        if isinstance(node, WDL.WorkflowSection):
            # scatter/conditional section: add a cluster subgraph to contain its body
            with graph.subgraph(name=f"cluster-{id(node)}") as sg:
                label = "scatter" if isinstance(node, WDL.Scatter) else "if"
                sg.attr(label=f"{label}({node.expr})", fontname=fontname, rank="same")
                for child in node.body:
                    add_node(sg, workflow, child)
                # Add an invisible node inside the subgraph, which provides a sink for dependencies
                # of the scatter/conditional expression itself
                sg.node(str(id(node)), "", style="invis", height="0", width="0", margin="0")
            graph.edge(str(id(workflow)), str(id(node)), style="invis")  # helps layout
            nodes_visited.add(id(node))
            nodes_visited |= set(id(g) for g in node.gathers.values())
        elif isinstance(node, WDL.Call) or (
            isinstance(node, WDL.Decl)
            and (
                inputs
                or nodes_visited.intersection(
                    id(workflow.get_node(it)) for it in node.workflow_node_dependencies
                )
            )
        ):
            name = node.name
            if isinstance(node, WDL.Call) and isinstance(node.callee, WDL.Workflow):
                # subworkflow call: add a cluster subgraph for the called workflow; only once, if
                # the subworkflow is called in multiple places.
                if id(node.callee) not in nodes_visited:
                    nodes_visited.add(id(node.callee))
                    with top.subgraph(name=f"cluster-{id(node.callee)}") as sg:
                        sg.attr(label=f"<<B>{node.callee.name}</B>>", fontname=fontname)
                        add_workflow(sg, node.callee)
                graph.edge(str(id(workflow)), str(id(node.callee)), style="invis")  # helps layout
                # dotted edge from call to subworkflow
                if subworkflow_edges:
                    graph.edge(
                        f"{id(node)}:s",
                        str(id(node.callee)),
                        lhead=f"cluster-{id(node.callee)}",
                        style="dotted",
                        arrowhead="none",
                    )
                name = f"<<B>{node.callee.name}</B> <I>as</I> {name}>"
            # node for call or decl
            graph.node(
                str(id(node)),
                name,
                shape=("cds" if isinstance(node, WDL.Call) else "plaintext"),
            )
            graph.edge(str(id(workflow)), str(id(node)), style="invis")  # helps layout
            nodes_visited.add(id(node))

    # add edge for each dependency between (visited) workflow nodes
    def add_edges(graph, workflow, node):
        for dep_id in node.workflow_node_dependencies:
            dep = workflow.get_node(dep_id)
            # leave Gather nodes invisible by replacing any dependencies on them with their
            # final_referee
            if isinstance(dep, WDL.Tree.Gather):
                dep = dep.final_referee
            if id(dep) in nodes_visited and id(node) in nodes_visited:
                lhead = None
                if isinstance(node, WDL.WorkflowSection):
                    lhead = f"cluster-{id(node)}"
                graph.edge(str(id(dep)), str(id(node)), lhead=lhead)
        if isinstance(node, WDL.WorkflowSection):
            for child in node.body:
                add_edges(graph, workflow, child)

    def add_workflow(graph, workflow):
        # invisible source/sink node for the workflow subgraph
        graph.node(
            str(id(workflow)),
            "",
            style="invis",
            height="0",
            width="0",
            margin="0",
        )

        # workflow body nodes
        for node in workflow.body:
            add_node(graph, workflow, node)

        # cluster of the input decls
        if inputs:
            with graph.subgraph(name=f"cluster-inputs-{id(workflow)}") as sg:
                for inp in workflow.inputs or []:
                    assert inp.workflow_node_id.startswith("decl-")
                    sg.node(str(id(inp)), inp.workflow_node_id[5:], shape="plaintext")
                    nodes_visited.add(id(inp))
                sg.attr(label="<<I>inputs</I>>", fontname=fontname)

        # cluster of the output decls
        if outputs:
            with graph.subgraph(name=f"cluster-outputs-{id(workflow)}") as sg:
                for outp in workflow.outputs or []:
                    assert outp.workflow_node_id.startswith("output-")
                    sg.node(str(id(outp)), outp.workflow_node_id[7:], shape="plaintext")
                    nodes_visited.add(id(outp))
                sg.attr(label="<<I>outputs</I>>", fontname=fontname)

        # edges
        for node in (workflow.inputs or []) + workflow.body + (workflow.outputs or []):
            add_edges(graph, workflow, node)

    add_workflow(top, workflow)

    return top


async def read_source(uri, path, importer):
    """
    This function helps miniwdl read the WDL source code directly from http[s] URIs.
    """
    if uri.startswith("http:") or uri.startswith("https:"):
        fn = os.path.join(
            tempfile.mkdtemp(prefix="miniwdl_import_uri_"),
            os.path.basename(parse.urlsplit(uri).path),
        )
        request.urlretrieve(uri, filename=fn)
        with open(fn, "r") as infile:
            return WDL.ReadSourceResult(infile.read(), uri)
    elif importer and (
        importer.pos.abspath.startswith("http:") or importer.pos.abspath.startswith("https:")
    ):
        assert not os.path.isabs(uri), "absolute import from downloaded WDL"
        return await read_source(parse.urljoin(importer.pos.abspath, uri), [], importer)
    return await WDL.read_source_default(uri, path, importer)


if __name__ == "__main__":
    main()
