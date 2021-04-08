from typing import Sequence, Callable, Any

from .base import NodeHash, Edge, NodesMask, FULL_MASK, NodeHashes, MaskOutput
from .node_hash import LeafHash, CompoundHash
from ..cache import Cache


class FullMask:
    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> MaskOutput:
        return FULL_MASK, None


class FunctionEdge(FullMask, Edge):
    def __init__(self, function: Callable, arity: int):
        super().__init__(arity, uses_hash=False)
        self.function = function
        # TODO:
        # self._function_hash = LeafHash(function)

    def _calc_hash(self, hashes):
        return CompoundHash(LeafHash(self.function), *hashes)

    def _evaluate(self, arguments: Sequence, output: NodeHash, payload: Any) -> Any:
        return self.function(*arguments)

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return self._calc_hash(inputs)

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return self._calc_hash(inputs)


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

    def _evaluate(self, arguments: Sequence, output: NodeHash, payload: Any) -> Any:
        assert isinstance(output, LeafHash), output
        return output.data

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return [], None

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        raise RuntimeError("Impure edges can't be a part of a subgraph")


class IdentityEdge(FullMask, Edge):
    def __init__(self):
        super().__init__(arity=1, uses_hash=False)

    def _evaluate(self, arguments: Sequence, output: NodeHash, payload: Any):
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

    def _evaluate(self, arguments: Sequence, output: NodeHash, payload: Any):
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
        empty, transaction = self.cache.reserve_write_or_read(output)
        if empty:
            return FULL_MASK, transaction
        return [], transaction

    def _evaluate(self, arguments: Sequence, output: NodeHash, payload: Any) -> Any:
        # no arguments means that the value is cached
        if not arguments:
            return self.cache.get(output, payload)

        value, = arguments
        # TODO: what to do in case of a collision:
        #   overwrite?
        #   add consistency check?
        #   get the value from cache?
        self.cache.set(output, value, payload)
        return value

    def handle_exception(self, output: NodeHash, payload: Any):
        self.cache.fail(output, payload)

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]


class ProductEdge(FullMask, Edge):
    def __init__(self, arity: int):
        super().__init__(arity, uses_hash=False)

    def _evaluate(self, arguments: Sequence, output: NodeHash, payload: Any):
        return tuple(arguments)

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return CompoundHash(*inputs)

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return CompoundHash(*inputs)
