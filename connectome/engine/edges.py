from abc import ABC, abstractmethod
from typing import Sequence, Callable, Any, Generator

from .base import NodeHash, Edge, NodeHashes, HashOutput, HashError, Request, Response
from .graph import Command
from .node_hash import LeafHash, ApplyHash, TupleHash
from ..cache import Cache


class StaticHash(Edge):
    def compute_hash(self) -> Generator[Request, Response, HashOutput]:
        inputs = []
        for idx in range(self.arity):
            value = yield Command.ParentHash, idx
            inputs.append(value)

        return self._compute_hash(inputs)

    @abstractmethod
    def _compute_hash(self, inputs: NodeHashes) -> HashOutput:
        pass


class StaticGraph:
    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        raise NotImplementedError

    def _compute_hash(self, inputs: NodeHashes) -> HashOutput:
        return self._make_hash(inputs), None

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return self._make_hash(inputs)


class StaticEdge(StaticHash):
    def evaluate(self) -> Generator[Request, Response, Any]:
        if False:
            inputs = []
            for idx in range(self.arity):
                value = yield Command.ParentValue, idx
                inputs.append(value)

        else:
            inputs = yield (Command.Await, *(
                (Command.ParentValue, idx)
                for idx in range(self.arity)
            ))
        return self._evaluate(inputs)

    @abstractmethod
    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        pass


class FunctionEdge(StaticGraph, StaticEdge):
    def __init__(self, function: Callable, arity: int):
        super().__init__(arity, uses_hash=False)
        self.function = function

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return ApplyHash(self.function, *inputs)

    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        return self.function(*inputs)


class ComputableHashBase(Edge, ABC):
    def __init__(self, function: Callable, arity: int):
        super().__init__(arity, uses_hash=True)
        self.function = function

    def compute_hash(self) -> Generator[Request, Response, HashOutput]:
        inputs = []
        for idx in range(self.arity):
            value = yield Command.ParentValue, idx
            inputs.append(value)

        result = self.function(*inputs)
        return LeafHash(result), result

    def evaluate(self) -> Generator[Request, Response, Any]:
        payload = yield Command.Payload,
        return payload


class ComputableHashEdge(ComputableHashBase):
    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return ApplyHash(self.function, *inputs)


class ImpureFunctionEdge(ComputableHashBase):
    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        raise HashError("Impure edges can't be a part of a subgraph")


class IdentityEdge(StaticGraph, StaticEdge):
    def __init__(self):
        super().__init__(arity=1, uses_hash=False)

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]

    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        return inputs[0]


class ConstantEdge(StaticEdge):
    """
    Used in interface to provide constant parameters.
    """

    def __init__(self, value):
        super().__init__(arity=0, uses_hash=False)
        self.value = value
        self._hash = LeafHash(self.value)

    def _compute_hash(self, inputs: NodeHashes) -> HashOutput:
        return self._hash, None

    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        return self.value

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return self._hash


class CacheEdge(StaticGraph, StaticHash):
    def __init__(self, storage: Cache):
        super().__init__(arity=1, uses_hash=True)
        self.cache = storage

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]

    def evaluate(self) -> Generator[Request, Response, Any]:
        output = yield Command.CurrentHash,
        value, exists = self.cache.get(output)
        if exists:
            return value

        value = yield Command.ParentValue, 0
        # TODO: what to do in case of a collision:
        #   overwrite?
        #   add consistency check?
        #   get the value from cache?
        self.cache.set(output, value)
        return value


class ProductEdge(StaticGraph, StaticEdge):
    def __init__(self, arity: int):
        super().__init__(arity, uses_hash=False)

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return TupleHash(*inputs)

    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        return tuple(inputs)
