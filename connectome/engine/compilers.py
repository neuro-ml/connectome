from collections import defaultdict
from typing import Dict, Any

from . import NodeHash
from .base import TreeNode, NodesMask, TreeNodes
from .utils import ExpirationCache


def execute_sequential(arguments: Dict[str, Any], inputs: TreeNodes, output: TreeNode,
                       hashes: Dict[TreeNode, NodeHash], masks: Dict[TreeNode, NodesMask]):
    operations, counts = compile_sequential(output, masks)
    cache = ExpirationCache(counts)

    for node in inputs:
        if node in counts:
            cache[node] = arguments[node.name]

    for node, inputs in operations:
        cache[node] = node.edge.evaluate(tuple(cache[x] for x in inputs), masks[node], hashes[node])

    return cache[output]


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
