"""
The computation is made in 3 passes:
1. Compute all node hashes
2. Use hashes to compute the required input nodes for each edge
3. Use hashes and masks to compute the output
"""
import inspect
from collections import defaultdict
from typing import Sequence, Dict

from .base import TreeNode, NodeHash, TreeNodes
from .utils import ExpirationCache


class Graph:
    def __init__(self, inputs: TreeNodes, output: TreeNode):
        validate_graph(inputs, output)
        counts = count_entries(inputs, output)
        inputs = [x for x in inputs if counts.get(x, 0)]
        inputs_map = {x.name: x for x in inputs}
        signature = inspect.Signature([
            inspect.Parameter(name, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            for name in inputs_map
        ])
        use_hash = uses_hash(output)
        self.inputs = inputs
        self.output = output

        def caller(*args, **kwargs):
            scope = signature.bind(*args, **kwargs)
            # put objects into inputs if hashes are not required
            input_hashes = {
                node: NodeHash.from_leaf(scope.arguments[name] if use_hash else object())
                for name, node in inputs_map.items()
            }
            hashes = compute_hashes(input_hashes, output)
            masks = compute_masks(output, hashes)
            # prepare for render
            local_counts = counts.copy()
            # TODO: use masks to drop unneeded hashes?
            hashes = ExpirationCache(local_counts, hashes)

            local_counts = count_entries(inputs, output, masks)
            cache = ExpirationCache(local_counts)

            for name, n in inputs_map.items():
                if n in local_counts:
                    cache[n] = scope.arguments[name]

            return render(output, cache, masks, hashes)

        caller.__signature__ = signature
        self.eval = caller

    def eval_hash(self, *inputs: NodeHash):
        assert len(inputs) == len(self.inputs)
        hashes = compute_hashes(dict(zip(self.inputs, inputs)), self.output)
        return hashes[self.output]

    def hash(self):
        return hash_graph(self.inputs, self.output)


# TODO: deprecate?
def compile_graph(inputs: Sequence[TreeNode], outputs: TreeNode):
    return Graph(inputs, outputs).eval


def uses_hash(node: TreeNode) -> bool:
    if node.edge is None:
        return False
    return node.edge[0].uses_hash or any(map(uses_hash, node.edge[1]))


def validate_graph(inputs: TreeNodes, output: TreeNode):
    def visitor(node):
        # input doesn't need parents
        if node in inputs:
            return
        # no edges - must be an input
        if not node.edge:
            assert node in inputs, (node, inputs)
        else:
            for inp in node.edge[1]:
                visitor(inp)

    visitor(output)


def count_entries(inputs: TreeNodes, output: TreeNode, masks=None):
    def visitor(node: TreeNode):
        entry_counts[node] += 1
        # input doesn't need parents
        if node in inputs:
            return

        group = node.edge[1]
        if masks is not None:
            group = [group[idx] for idx in masks[node]]

        for n in group:
            visitor(n)

    entry_counts = defaultdict(int)
    visitor(output)
    return dict(entry_counts)


# def precompute_hashes(inputs, outputs):
#     def visitor(node: TreeNode):
#         if node in hashes:
#             return True
#
#         if not node.edge:
#             assert node in inputs
#             return False
#
#         # we visit the root nodes and build a cache of immutable hashes
#         edge, group = node.edge
#         visited = all(visitor(x) for x in group)
#
#         if edge.uses_hash or not visited:
#             # the edge doesn't have a constant hash
#             return False
#
#         hashes[node], masks[node] = edge.process_hashes([hashes[x] for x in group])
#         return True
#
#     hashes, masks = {}, {}
#     for n in outputs:
#         visitor(n)
#     return hashes, masks


def compute_hashes(inputs: Dict[TreeNode, NodeHash], output: TreeNode):
    def visitor(node: TreeNode):
        if node not in cache:
            edge, group = node.edge
            cache[node] = edge.propagate_hash(list(map(visitor, group)))

        return cache[node]

    cache = inputs.copy()
    visitor(output)
    return cache


def compute_masks(output: TreeNode, hashes):
    def visitor(node: TreeNode):
        if node not in cache:
            if node.is_leaf:
                cache[node] = []
                return

            edge, group = node.edge
            cache[node] = mask = edge.compute_mask([hashes[n] for n in group], hashes[node])
            for idx in mask:
                visitor(group[idx])

    cache = {}
    visitor(output)
    return cache


def hash_graph(inputs: Sequence[TreeNode], output: TreeNode):
    def visitor(node: TreeNode):
        if node not in hashes:
            edge, group = node.edge
            hashes[node] = edge.hash_graph(list(map(visitor, group)))

        return hashes[node]

    hashes = dict.fromkeys(inputs, NodeHash.from_leaf(Placeholder))
    return visitor(output)


def render(node, cache, masks, hashes):
    if node not in cache:
        edge, inputs = node.edge
        mask = masks[node]

        inputs = [inputs[idx] for idx in mask]
        cache[node] = edge.evaluate(
            tuple(render(x, cache, masks, hashes) for x in inputs),
            mask, hashes[node]
        )

    return cache[node]


class Placeholder:
    """
    A placeholder used to calculate the graph hash without inputs.
    """

    # TODO: singleton
    def __init__(self):
        raise RuntimeError("Don't init me!")
