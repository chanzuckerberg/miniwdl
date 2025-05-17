#!/usr/bin/env python3
"""
This example has grown into its own project, see:
https://github.com/miniwdl-ext/wdlviz
"""
import ast

with open(__file__, "r") as file:
    tree = ast.parse(file.read())
    print(ast.get_docstring(tree))
