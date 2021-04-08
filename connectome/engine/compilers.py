from collections import defaultdict
from typing import Dict, Any

from . import NodeHash
from .base import TreeNode, NodesMask, TreeNodes
from .utils import ExpirationCache
from .execution import GraphTask, execute_graph_async


def execute_sequential(arguments: Dict[str, Any], inputs: TreeNodes, output: TreeNode,
                       hashes: Dict[TreeNode, NodeHash], masks: Dict[TreeNode, NodesMask]):
    operations, counts = compile_sequential(output, masks)
    cache = ExpirationCache(counts)

    for node in inputs:
        if node in counts:
            cache[node] = arguments[node.name]

    operations = operations[::-1]
    try:
        while operations:
            node, inputs = operations.pop()
            cache[node] = node.edge.evaluate(tuple(cache[x] for x in inputs), masks[node], hashes[node])

        return cache[output]

    except BaseException:
        # cleanup
        while operations:
            node, inputs = operations.pop()
            node.edge.handle_exception(masks[node], hashes[node])

        raise


def compile_sequential(output: TreeNode, masks: Dict[TreeNode, NodesMask]):
    def _compile(node):
        counts[node] += 1
        if node in visited:
            return

        visited.add(node)
        if node.is_leaf:
            return

        inputs = node.parents
        inputs = [inputs[idx] for idx in masks[node]]
        for x in inputs:
            yield from _compile(x)

        yield node, inputs

    counts = defaultdict(int)
    visited = set()
    operations = list(_compile(output))
    return operations, dict(counts)


def sequential_to_graph(arguments: Dict[str, Any], inputs: TreeNodes, output: TreeNode,
                        hashes: Dict[TreeNode, NodeHash], masks: Dict[TreeNode, NodesMask]):
    operations, counts = compile_sequential(output, masks)
    graph = {}

    def wrap_constant(values):
        def f(x):
            return values

        return f

    def wrap_edge(edge, m, h):
        def f(*args):
            return edge(*args, m, h)

        return f

    for node in inputs:
        if node in counts:
            graph[node] = GraphTask(evaluate=wrap_constant(arguments[node.name]), dependencies=[])

    for node, dependencies in operations:
        graph[node] = GraphTask(evaluate=wrap_edge(node.edge.evaluate, masks[node], hashes[node]),
                                dependencies=dependencies)
    return graph, output


def execute_sequential_async(arguments: Dict[str, Any], inputs: TreeNodes, output: TreeNode,
                             hashes: Dict[TreeNode, NodeHash], masks: Dict[TreeNode, NodesMask]):
    dep_graph, output = sequential_to_graph(arguments, inputs, output,
                                            hashes, masks)

    res = execute_graph_async(dep_graph, [output], replace_by_persistent_ids=True)
    return res[output]
