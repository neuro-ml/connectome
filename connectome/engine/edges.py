from typing import Sequence, Callable, Any

from .base import NodeHash, Edge, NodesMask, FULL_MASK, NodeHashes
from .node_hash import LeafHash, CompoundHash
from ..cache import Cache


class FunctionEdge(Edge):
    def __init__(self, function: Callable, arity: int):
        super().__init__(arity, uses_hash=False)
        self.function = function
        # TODO:
        # self._function_hash = LeafHash(function)

    def _calc_hash(self, hashes):
        return CompoundHash(LeafHash(self.function), *hashes)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash) -> Any:
        return self.function(*arguments)

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return self._calc_hash(inputs)

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return self._calc_hash(inputs)

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return FULL_MASK


class ComputableHashEdge(FunctionEdge):
    def __init__(self, function: Callable, arity: int):
        super().__init__(function, arity)
        self._uses_hash = True

    def _calc_hash(self, inputs: NodeHashes):
        args = []
        for h in inputs:
            assert isinstance(h, LeafHash), h
            args.append(h.data)

        return LeafHash(self.function(*args))


class ImpureFunctionEdge(Edge):
    def __init__(self, function: Callable, arity: int):
        super().__init__(arity, uses_hash=True)
        self.function = function

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        args = []
        for h in inputs:
            assert isinstance(h, LeafHash), h
            args.append(h.data)

        return LeafHash(self.function(*args))

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash) -> Any:
        assert isinstance(node_hash, LeafHash), node_hash
        return node_hash.data

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return []

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        raise RuntimeError("Impure edges can't be a part of a subgraph")


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
        self._hash = LeafHash(self.value)

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
        super().__init__(arity, uses_hash=False)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        return tuple(arguments)

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return FULL_MASK

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return CompoundHash(*inputs)

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return CompoundHash(*inputs)
