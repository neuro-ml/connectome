"""
The computation is made in 3 passes:
1. Compute all node hashes
2. Use hashes to compute the required input nodes for each edge
3. Use hashes and masks to compute the output
"""
import inspect
from collections import defaultdict
from typing import Sequence, Dict, Any

from .base import TreeNode, NodeHash, TreeNodes, Command
# from .compilers import execute_sequential, execute_sequential_async
from .node_hash import LeafHash, GraphHash
from .utils import EvictionCache
from .vm import execute


class Graph:
    def __init__(self, inputs: TreeNodes, output: TreeNode):
        validate_graph(inputs, output)
        # TODO: need a cumulative eviction policy
        counts = count_entries(inputs, output, multiplier=2)
        inputs = sorted([x for x in inputs if counts.get(x, 0)], key=lambda x: x.name)
        signature = inspect.Signature([
            inspect.Parameter(x.name, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            for x in inputs
        ])
        # TODO: do we need this optimization?
        self.use_hash = True  # uses_hash(output)
        self.inputs = inputs
        self.output = output
        self.counts = counts

        def caller(*args, **kwargs):
            scope = signature.bind(*args, **kwargs)
            hashes, cache = self._prepare_cache(scope.arguments)
            return evaluate(output, hashes, cache)

        caller.__signature__ = signature
        self.call = caller

    def _prepare_cache(self, arguments):
        # put objects into inputs if hashes are not required
        hashes = EvictionCache(self.counts.copy(), {
            node: (LeafHash(arguments[node.name] if self.use_hash else object()), None)
            for node in self.inputs
        })
        cache = EvictionCache(self.counts.copy(), {node: arguments[node.name] for node in self.inputs})
        return hashes, cache

    def get_hash(self, *inputs: Any):
        assert len(inputs) == len(self.inputs)
        assert all(not isinstance(v, NodeHash) for v in inputs)

        hashes, cache = self._prepare_cache({n.name: v for n, v in zip(self.inputs, inputs)})
        result, _ = compute_hash(self.output, hashes, cache)
        return result, (hashes, cache)

    def get_value(self, hashes, cache):
        return evaluate(self.output, hashes, cache)

    def hash(self):
        return GraphHash(hash_graph(self.inputs, self.output))


def evaluate(node: TreeNode, hashes: EvictionCache, cache: EvictionCache):
    return execute(Command.Evaluate, node, hashes, cache)


def compute_hash(node: TreeNode, hashes: EvictionCache, cache: EvictionCache):
    return execute(Command.ComputeHash, node, hashes, cache)


# TODO: deprecate?
def compile_graph(inputs: TreeNodes, outputs: TreeNode):
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


def count_entries(inputs: TreeNodes, output: TreeNode, masks=None, multiplier: int = 1):
    def visitor(node: TreeNode):
        entry_counts[node] += multiplier
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
