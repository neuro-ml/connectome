import inspect
from collections import defaultdict
from typing import Any, Sequence

from .base import Command, TreeNode, TreeNodes
from .node_hash import GraphHash, LeafHash, NodeHash
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
        self.inputs = inputs
        self.output = output
        self.counts = counts
        self.__signature__ = signature

    def __call__(*args, **kwargs):
        self, *args = args
        scope = self.__signature__.bind(*args, **kwargs)
        hashes, cache = self._prepare_cache(scope.arguments)
        return evaluate(self.output, hashes, cache)

    def __str__(self):
        inputs = ', '.join(x.name for x in self.inputs)
        if len(self.inputs) != 1:
            inputs = f'({inputs})'
        return f'Graph({inputs} -> {self.output.name})'

    def _prepare_cache(self, arguments):
        # put objects into inputs if hashes are not required
        hashes = EvictionCache(self.counts.copy(), {
            node: (LeafHash(arguments[node.name]), None)
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

    def get_value(self, hashes, cache) -> Any:
        return evaluate(self.output, hashes, cache)

    def hash(self) -> GraphHash:
        return GraphHash(hash_graph(self.inputs, self.output))


def evaluate(node: TreeNode, hashes: EvictionCache, cache: EvictionCache):
    return execute(Command.Evaluate, node, hashes, cache)


def compute_hash(node: TreeNode, hashes: EvictionCache, cache: EvictionCache):
    return execute(Command.ComputeHash, node, hashes, cache)


def validate_graph(inputs: TreeNodes, output: TreeNode):
    def visitor(node):
        # input doesn't need parents
        if node in inputs:
            return
        # no edges - must be an input
        assert not node.is_leaf, (node, inputs)

        for inp in node.parents:
            visitor(inp)

    visitor(output)


def count_entries(inputs: TreeNodes, output: TreeNode, multiplier: int = 1):
    def visitor(node: TreeNode):
        entry_counts[node] += multiplier
        # input doesn't need parents
        if node in inputs:
            return

        for n in node.parents:
            visitor(n)

    entry_counts = defaultdict(int)
    visitor(output)
    return dict(entry_counts)


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
