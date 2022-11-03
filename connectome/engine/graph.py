import inspect
from collections import defaultdict
from concurrent.futures import Executor
from typing import Sequence, Any

from .base import TreeNode, NodeHash, TreeNodes, Command
from .executor import DefaultExecutor
from .node_hash import LeafHash, GraphHash
from .utils import EvictionCache
from .vm import execute


class Graph:
    def __init__(self, inputs: TreeNodes, output: TreeNode, executor: Executor = None):
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
        self.executor = DefaultExecutor if executor is None else executor

        def caller(*args, **kwargs):
            scope = signature.bind(*args, **kwargs)
            hashes, cache = self._prepare_cache(scope.arguments)
            return evaluate(output, hashes, cache, self.executor)

        caller.__signature__ = signature
        self.call = caller

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
        result, _ = compute_hash(self.output, hashes, cache, self.executor)
        return result, (hashes, cache)

    def get_value(self, hashes, cache):
        return evaluate(self.output, hashes, cache, self.executor)

    def hash(self):
        return GraphHash(hash_graph(self.inputs, self.output))


def evaluate(node: TreeNode, hashes: EvictionCache, cache: EvictionCache, executor: Executor):
    return execute(Command.Evaluate, node, hashes, cache, executor)


def compute_hash(node: TreeNode, hashes: EvictionCache, cache: EvictionCache, executor: Executor):
    return execute(Command.ComputeHash, node, hashes, cache, executor)


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
