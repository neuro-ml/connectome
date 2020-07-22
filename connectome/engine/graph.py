import inspect

from collections import defaultdict
from typing import Sequence, Union

from .base import TreeNode, NodeHash
from .utils import ExpirationCache

__all__ = 'compile_graph',


def compile_graph(inputs: Sequence[TreeNode], outputs: Union[TreeNode, Sequence[TreeNode]]):
    squeeze = isinstance(outputs, TreeNode)
    if squeeze:
        outputs = [outputs]

    validate_graph(inputs, outputs)
    counts = count_entries(inputs, outputs)
    inputs = [x for x in inputs if counts.get(x, 0)]
    inputs_map = {x.name: x for x in inputs}

    signature = inspect.Signature([
        inspect.Parameter(name, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        for name in inputs_map
    ])

    def caller(*args, **kwargs):
        scope = signature.bind(*args, **kwargs)
        # drop unnecessary branches
        masks, hashes = prune(inputs_map, outputs, scope.arguments)
        # prepare for render
        local_counts = counts.copy()
        hashes = ExpirationCache(local_counts, hashes)

        local_counts = (count_entries(inputs, outputs, masks))
        cache = ExpirationCache(local_counts)

        for name, n in inputs_map.items():
            if n in local_counts:
                cache[n] = scope.arguments[name]

        # render
        result = tuple(render(node, cache, masks, hashes) for node in outputs)
        if squeeze:
            result = result[0]
        return result

    caller.__signature__ = signature
    return caller


def validate_graph(inputs, outputs):
    def visitor(nodes):
        for node in nodes:
            # input doesn't need parents
            if node in inputs:
                continue

            # no edges - must be an input
            if not node.edges:
                # assert node in inputs, (node, inputs)
                continue

            else:
                group, = node.edges.values()
                visitor(group)

    visitor(outputs)


def count_entries(inputs: Sequence[TreeNode], outputs: Sequence[TreeNode], masks=None):
    def visitor(node: TreeNode):
        entry_counts[node] += 1
        # input doesn't need parents
        if node in inputs:
            return

        group, = node.edges.values()
        if masks is not None:
            group = [group[idx] for idx in masks[node]]

        for n in group:
            visitor(n)

    entry_counts = defaultdict(int)
    for x in outputs:
        visitor(x)
    return dict(entry_counts)


def prune(inputs_map, outputs, arguments):
    def visitor(node: TreeNode):
        if node in cache:
            return cache[node]

        (edge, group), = node.edges.items()
        result, mask = edge.process_hashes([visitor(x) for x in group])
        masks[node] = mask
        cache[node] = result
        return result

    masks = {}
    cache = {}
    for name, n in inputs_map.items():
        cache[n] = NodeHash.from_leaf(arguments[name])
    for n in outputs:
        visitor(n)

    return masks, cache


def render(node, cache, masks, hashes):
    if node not in cache:
        (edge, inputs), = node.edges.items()
        mask = masks[node]

        inputs = [inputs[idx] for idx in mask]
        cache[node] = edge.evaluate([render(x, cache, masks, hashes) for x in inputs], mask, hashes[node])

    return cache[node]
