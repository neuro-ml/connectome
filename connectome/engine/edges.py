from abc import ABC, abstractmethod
from typing import Sequence, Callable, Any, Generator, Tuple

from .base import Edge, HashOutput, HashError, Request, Response
from .node_hash import NodeHash, NodeHashes
from .graph import Command
from .node_hash import LeafHash, ApplyHash, TupleHash

__all__ = (
    'StaticHash', 'StaticGraph', 'StaticEdge',
    'ImpureEdge', 'CacheEdge', 'IdentityEdge', 'FunctionEdge', 'ComputableHashEdge',
    'ConstantEdge', 'ComputableHashBase', 'ProductEdge',
)


class StaticHash(Edge):
    """ Computes the current hash from all the parents' hashes. """

    def compute_hash(self) -> Generator[Request, Response, HashOutput]:
        inputs = yield (Command.Await, *(
            (Command.ParentHash, idx)
            for idx in range(self.arity)
        ))
        return self._compute_hash(inputs)

    @abstractmethod
    def _compute_hash(self, inputs: NodeHashes) -> HashOutput:
        pass


class StaticGraph:
    """ Mixin for edges which share a the same hash computation for `_compute_hash` and `_hash_graph`. """

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        raise NotImplementedError

    def _compute_hash(self, inputs: NodeHashes) -> HashOutput:
        return self._make_hash(inputs), None

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return self._make_hash(inputs)


class StaticEdge(StaticHash):
    """
    Computes the current value from all the parents' values and current hash from all parents' hashes.
    """

    def evaluate(self) -> Generator[Request, Response, Any]:
        inputs = yield (Command.Await, *(
            (Command.ParentValue, idx)
            for idx in range(self.arity)
        ))
        return self._evaluate(inputs)

    @abstractmethod
    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        """ Computes the output value. """


class FunctionEdge(StaticGraph, StaticHash):
    def __init__(self, function: Callable, arity: int, kw_names: Tuple[str, ...] = (),
                 silent: Tuple[int, ...] = ()):
        super().__init__(arity)
        assert len(kw_names) <= arity
        if kw_names:
            assert kw_names == tuple(sorted(kw_names)), kw_names
        if silent:
            assert 0 <= min(silent) and max(silent) < self.arity
        self.kw_names = kw_names
        self.silent = silent
        self.function = function

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        # TODO: optimize this
        if self.silent:
            silent_hash = LeafHash(None)
            inputs = list(inputs)
            for idx in self.silent:
                # just overwrite the real hash with a constant
                inputs[idx] = silent_hash

        return ApplyHash(self.function, *inputs, kw_names=self.kw_names)

    def evaluate(self) -> Generator[Request, Response, Any]:
        inputs = yield (Command.Await, *(
            (Command.ParentValue, idx)
            for idx in range(self.arity)
        ))
        # TODO: does this speed up anything?
        if self.kw_names:
            args = inputs[:-len(self.kw_names)]
            kwargs = {k: v for k, v in zip(self.kw_names, inputs[-len(self.kw_names):])}
        else:
            args, kwargs = inputs, {}

        return (yield Command.Call, self.function, args, kwargs)


class ComputableHashBase(Edge, ABC):
    def __init__(self, edge: Edge):
        super().__init__(edge.arity)
        self.edge = edge

    def compute_hash(self) -> Generator[Request, Response, HashOutput]:
        iterator, value = self.edge.evaluate(), None
        try:
            while True:
                value = yield iterator.send(value)
        except StopIteration as e:
            return LeafHash(e.value), e.value

    def evaluate(self) -> Generator[Request, Response, Any]:
        return (yield Command.Payload,)


class ComputableHashEdge(ComputableHashBase):
    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return self.edge.hash_graph(inputs)


class ImpureEdge(ComputableHashBase):
    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        raise HashError("Impure edges can't be a part of a subgraph")


class IdentityEdge(StaticGraph, StaticEdge):
    def __init__(self):
        super().__init__(arity=1)

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]

    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        return inputs[0]


class ConstantEdge(StaticEdge):
    """
    Used in interface to provide constant parameters.
    """

    def __init__(self, value):
        super().__init__(arity=0)
        self.value = value
        self._hash = LeafHash(self.value)

    def _compute_hash(self, inputs: NodeHashes) -> HashOutput:
        return self._hash, None

    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        return self.value

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return self._hash


class CacheEdge(StaticGraph, StaticHash):
    def __init__(self, storage):
        super().__init__(arity=1)
        self.cache = storage

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]

    def evaluate(self) -> Generator[Request, Response, Any]:
        output = yield Command.CurrentHash,
        key, context = self.cache.prepare(output)
        value, exists = self.cache.get(key, context)
        if exists:
            return value

        value = yield Command.ParentValue, 0
        # TODO: what to do in case of a collision:
        #   overwrite?
        #   add consistency check?
        #   get the value from cache?
        self.cache.set(key, value, context)
        return value


class ProductEdge(StaticGraph, StaticEdge):
    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return TupleHash(*inputs)

    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        return tuple(inputs)
