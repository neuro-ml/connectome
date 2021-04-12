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
from .compilers import execute_sequential, execute_sequential_async
from .node_hash import LeafHash, GraphHash


class Graph:
    def __init__(self, inputs: TreeNodes, output: TreeNode):
        validate_graph(inputs, output)
        counts = count_entries(inputs, output)
        inputs = [x for x in inputs if counts.get(x, 0)]
        signature = inspect.Signature([
            inspect.Parameter(x.name, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            for x in inputs
        ])
        use_hash = uses_hash(output)
        self.inputs = inputs
        self.output = output

        def caller(*args, **kwargs):
            scope = signature.bind(*args, **kwargs)
            # put objects into inputs if hashes are not required
            input_hashes = {
                node: LeafHash(scope.arguments[node.name] if use_hash else object())
                for node in inputs
            }
            hashes, hash_payload = compute_hashes(input_hashes, output)
            masks, mask_payload = compute_masks(output, hashes)

            # return execute_sequential_async(scope.arguments, inputs, output, hashes, masks)
            return execute_sequential(scope.arguments, inputs, output, hashes, masks, hash_payload, mask_payload)

        caller.__signature__ = signature
        self.call = caller
        self.eval = 1

    # TODO: remove duplicates
    def propagate_hash(self, *inputs: NodeHash):
        assert len(inputs) == len(self.inputs)
        hashes, payload = compute_hashes(dict(zip(self.inputs, inputs)), self.output)
        return hashes[self.output], (hashes, payload)

    def evaluate(self, inputs: Sequence, hashes, payload):
        assert len(inputs) == len(self.inputs)
        masks, mask_payload = compute_masks(self.output, hashes)
        inputs = {node.name: x for node, x in zip(self.inputs, inputs)}

        # return execute_sequential_async(scope.arguments, inputs, output, hashes, masks)
        return execute_sequential(inputs, self.inputs, self.output, hashes, masks, payload, mask_payload)

    def hash(self):
        return GraphHash(hash_graph(self.inputs, self.output))


# TODO: deprecate?
def compile_graph(inputs: Sequence[TreeNode], outputs: TreeNode):
    return Graph(inputs, outputs).call


def uses_hash(node: TreeNode) -> bool:
    if node.is_leaf:
        return False
    return node.edge.uses_hash or any(map(uses_hash, node.parents))


def validate_graph(inputs: TreeNodes, output: TreeNode):
    def visitor(node):
        # input doesn't need parents
        if node in inputs:
            return
        # no edges - must be an input
        if node.is_leaf:
            assert node in inputs, (node, inputs)
        else:
            for inp in node.parents:
                visitor(inp)

    visitor(output)


def count_entries(inputs: TreeNodes, output: TreeNode, masks=None):
    def visitor(node: TreeNode):
        entry_counts[node] += 1
        # input doesn't need parents
        if node in inputs:
            return

        parents = node.parents
        if masks is not None:
            parents = [parents[idx] for idx in masks[node]]

        for n in parents:
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
            cache[node], payload[node] = node.edge.propagate_hash(list(map(visitor, node.parents)))

        return cache[node]

    cache = inputs.copy()
    payload = dict.fromkeys(cache, None)
    visitor(output)
    return cache, payload


def compute_masks(output: TreeNode, hashes):
    def visitor(node: TreeNode):
        if node not in masks:
            if node.is_leaf:
                masks[node] = ()
                payloads[node] = None
                return

            parents = node.parents
            masks[node], payloads[node] = mask, _ = node.edge.compute_mask([hashes[n] for n in parents], hashes[node])
            for idx in mask:
                visitor(parents[idx])

    masks, payloads = {}, {}
    visitor(output)
    return masks, payloads


def hash_graph(inputs: Sequence[TreeNode], output: TreeNode):
    def visitor(node: TreeNode):
        if node not in hashes:
            hashes[node] = node.edge.hash_graph(list(map(visitor, node.parents)))

        return hashes[node]

    hashes = dict.fromkeys(inputs, _PLACEHOLDER)
    return visitor(output)


# TODO: how safe is this?
# a placeholder used to calculate the graph hash without inputs
_PLACEHOLDER = LeafHash(object())
