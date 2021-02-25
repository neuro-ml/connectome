from typing import Sequence, Callable, Any

from .base import NodeHash, Edge, NodesMask, FULL_MASK, NodeHashes
from ..cache import Cache


class FunctionEdge(Edge):
    def __init__(self, function: Callable, arity: int):
        super().__init__(arity, uses_hash=False)
        self.function = function
        # TODO:
        # self._function_hash = NodeHash.from_leaf(function)

    def _calc_hash(self, hashes):
        return NodeHash.from_hash_nodes(NodeHash.from_leaf(self.function), *hashes)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash) -> Any:
        return self.function(*arguments)

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return self._calc_hash(inputs)

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return self._calc_hash(inputs)

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return FULL_MASK


class IdentityEdge(Edge):
    def __init__(self):
        super().__init__(arity=1, uses_hash=False)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        return arguments[0]

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return inputs[0]

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return FULL_MASK


class ConstantEdge(Edge):
    """
    Used in interface to provide constant parameters.
    """

    def __init__(self, value):
        super().__init__(arity=0, uses_hash=False)
        self.value = value
        self._hash = NodeHash.from_leaf(self.value)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        return self.value

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return self._hash

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return self._hash

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return FULL_MASK


class CacheEdge(Edge):
    def __init__(self, storage: Cache):
        super().__init__(arity=1, uses_hash=True)
        self.storage = storage

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        if self.storage.contains(output):
            return []
        return FULL_MASK

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash) -> Any:
        # no arguments means that the value is cached
        if not arguments:
            return self.storage.get(node_hash)

        value, = arguments
        self.storage.set(node_hash, value)
        return value

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return inputs[0]


class ProductEdge(Edge):
    def __init__(self, arity: int):
        super().__init__(arity, uses_hash=True)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        return tuple(arguments)

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return FULL_MASK

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return NodeHash.from_hash_nodes(*inputs)

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return NodeHash.from_hash_nodes(*inputs)
