from abc import ABC
from typing import Sequence, Callable, Any

from .base import NodeHash, Edge, NodesMask, FULL_MASK, NodeHashes, MaskOutput, HashOutput, HashError
from .node_hash import LeafHash, ApplyHash, TupleHash
from ..cache import Cache


class FullMask:
    @staticmethod
    def _compute_mask(inputs: NodeHashes, output: NodeHash) -> MaskOutput:
        return FULL_MASK, None


class FunctionEdge(FullMask, Edge):
    def __init__(self, function: Callable, arity: int):
        super().__init__(arity, uses_hash=False)
        self.function = function

    def _calc_hash(self, hashes):
        return ApplyHash(self.function, *hashes)

    def _evaluate(self, inputs: Sequence, output: NodeHash, hash_payload: Any, mask_payload: Any) -> Any:
        return self.function(*inputs)

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return self._calc_hash(inputs)

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return self._calc_hash(inputs)


class ComputableHashBase(Edge, ABC):
    def __init__(self, function: Callable, arity: int):
        super().__init__(arity, uses_hash=True)
        self.function = function

    def _call_function(self, inputs: NodeHashes):
        args = []
        for h in inputs:
            assert isinstance(h, LeafHash), h
            args.append(h.data)

        return self.function(*args)

    def _propagate_hash(self, inputs: NodeHashes) -> HashOutput:
        value = self._call_function(inputs)
        return LeafHash(value), value

    def _evaluate(self, arguments: Sequence, output: NodeHash, hash_payload: Any, mask_payload: Any) -> Any:
        return hash_payload

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return [], None


class ComputableHashEdge(ComputableHashBase):
    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return ApplyHash(self.function, *inputs)


class ImpureFunctionEdge(ComputableHashBase):
    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        raise HashError("Impure edges can't be a part of a subgraph")


class IdentityEdge(FullMask, Edge):
    def __init__(self):
        super().__init__(arity=1, uses_hash=False)

    def _evaluate(self, arguments: Sequence, output: NodeHash, hash_payload: Any, mask_payload: Any):
        return arguments[0]

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return inputs[0]


class ConstantEdge(FullMask, Edge):
    """
    Used in interface to provide constant parameters.
    """

    def __init__(self, value):
        super().__init__(arity=0, uses_hash=False)
        self.value = value
        self._hash = LeafHash(self.value)

    def _evaluate(self, arguments: Sequence, output: NodeHash, hash_payload: Any, mask_payload: Any):
        return self.value

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return self._hash

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return self._hash


class CacheEdge(Edge):
    def __init__(self, storage: Cache):
        super().__init__(arity=1, uses_hash=True)
        self.cache = storage

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        if self.cache.reserve_read(output):
            return [], True
        return FULL_MASK, False

    def _evaluate(self, arguments: Sequence, output: NodeHash, hash_payload: Any, mask_payload: Any) -> Any:
        # no arguments means that the value is cached
        if not arguments:
            return self.cache.get(output)

        value, = arguments
        # TODO: what to do in case of a collision:
        #   overwrite?
        #   add consistency check?
        #   get the value from cache?
        self.cache.set(output, value)
        return value

    def handle_exception(self, output: NodeHash, payload: Any):
        self.cache.fail(output, payload)

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]


class ProductEdge(FullMask, Edge):
    def __init__(self, arity: int):
        super().__init__(arity, uses_hash=False)

    def _evaluate(self, inputs: Sequence, output: NodeHash, hash_payload: Any, mask_payload: Any):
        return tuple(inputs)

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return TupleHash(*inputs)

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return TupleHash(*inputs)
