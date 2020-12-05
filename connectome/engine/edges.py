from typing import Sequence, Tuple, Callable

from .base import NodeHash, Edge, NodesMask, FULL_MASK, HashType
from .graph import Graph
from ..cache import Cache, MemoryCache, DiskCache


# TODO: maybe the engine itself should deal with these
class Nothing:
    """
    A unity-like which is propagated through functional edges.
    """

    # TODO: singleton
    def __init__(self):
        raise RuntimeError("Don't init me!")

    @staticmethod
    def in_data(data):
        return any(x is Nothing for x in data)

    @staticmethod
    def in_hashes(hashes: Sequence[NodeHash]):
        # TODO: do we want always to evaluate all hashes
        return any(x.data is Nothing for x in list(hashes))


class Placeholder:
    """
    A placeholder used to calculate the graph hash without inputs.
    """

    # TODO: singleton
    def __init__(self):
        raise RuntimeError("Don't init me!")


class PropagateNothing(Edge):
    def _process(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        raise NotImplementedError

    def _eval(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        raise NotImplementedError

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        if Nothing.in_hashes(hashes):
            return NodeHash.from_leaf(Nothing), FULL_MASK

        return self._process(hashes)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        if Nothing.in_data(arguments) or Nothing.in_hashes([node_hash]):
            return Nothing

        return self._eval(arguments, mask, node_hash)


class FunctionEdge(PropagateNothing):
    def __init__(self, function: Callable, arity: int):
        super().__init__(arity, uses_hash=False)
        self.function = function

    def _eval(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        return self.function(*arguments)

    def _process(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        return NodeHash.from_hash_nodes(
            NodeHash.from_leaf(self.function), *hashes, prev_edge=self
        ), FULL_MASK


class IdentityEdge(Edge):
    def __init__(self):
        super().__init__(arity=1, uses_hash=False)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        return arguments[0]

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        return hashes[0], FULL_MASK


class CacheEdge(PropagateNothing):
    def __init__(self, storage: Cache):
        super().__init__(arity=1, uses_hash=True)
        self.storage = storage

    def _process(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        node_hash, = hashes
        if self.storage.contains(node_hash):
            mask = []
        else:
            mask = FULL_MASK

        return node_hash, mask

    def _eval(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        # no arguments means that the value is cached
        if not arguments:
            return self.storage.get(node_hash)

        value, = arguments
        self.storage.set(node_hash, value)
        return value


class ProductEdge(Edge):
    def __init__(self, arity: int):
        super().__init__(arity, uses_hash=True)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        return arguments

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        return NodeHash.from_hash_nodes(*hashes, prev_edge=self), FULL_MASK


# TODO: are Switch and Projection the only edges that need Nothing?
# TODO: does Nothing live only in hashes?
class SwitchEdge(Edge):
    def __init__(self, selector: Callable):
        super().__init__(arity=1, uses_hash=True)
        self.selector = selector

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        if node_hash.data is Nothing:
            return Nothing

        return arguments[0]

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        node_hash, = hashes
        if not self.selector(node_hash):
            # TODO: need a special type for hash of nothing
            node_hash = NodeHash.from_leaf(Nothing)
        return node_hash, FULL_MASK


class ProjectionEdge(Edge):
    def __init__(self):
        super().__init__(arity=1, uses_hash=True)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        # take the only non-Nothing value
        real = []
        for v in arguments[0]:
            if v is not Nothing:
                real.append(v)

        assert len(real) == 1, real
        return real[0]

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        # take the only non-Nothing hash
        real = []
        for v in hashes[0].children:
            if v.data is not Nothing:
                real.append(v)

        assert len(real) == 1, real
        return real[0], FULL_MASK


class CachedRow(PropagateNothing):
    def __init__(self, disk: DiskCache, ram: MemoryCache, graph: Graph):
        super().__init__(arity=3, uses_hash=True)
        self.graph = graph
        self.disk = disk
        self.ram = ram

    def _process(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        """
        Hashes
        ------
        entry: the hash for the entry at ``key``
        key: a unique key for each entry in the tuple
        keys: all available keys
        """
        entry = hashes[0]
        if self.ram.contains(entry):
            return entry, []

        return entry, [1, 2]

    def _eval(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        if not arguments:
            return self.ram.get(node_hash)

        key, keys = arguments
        keys = sorted(keys)
        assert key in keys

        hashes = []
        for k in keys:
            h = self.graph.eval_hash([NodeHash.from_leaf(k)])
            hashes.append(h)
            if k == key:
                assert node_hash == h
        compound = NodeHash.from_hash_nodes(*hashes)

        if not self.disk.contains(compound):
            values = [self.graph.eval(k) for k in keys]
            self.disk.set(compound, values)
        else:
            values = self.disk.get(compound)

        for k, h, value in zip(keys, hashes, values):
            self.ram.set(h, value)
            if k == key:
                result = value

        return result


class FilterEdge(PropagateNothing):
    def __init__(self, func: Callable, graph: Graph):
        super().__init__(arity=1, uses_hash=False)
        self.graph = graph
        self.func = func

    def _process(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        keys, = hashes
        args = self.graph.eval_hash([NodeHash.from_leaf(Placeholder)])
        return NodeHash.from_hash_nodes(
            NodeHash.from_leaf(self.func), keys, args,
            kind=HashType.FILTER,
        ), FULL_MASK

    def _eval(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        keys, = arguments
        result = []
        for key in keys:
            args = self.graph.eval(key)
            if self.func(*args):
                result.append(key)

        return tuple(result)
