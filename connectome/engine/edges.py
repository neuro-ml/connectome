from abc import abstractmethod
from typing import Sequence, Callable

from .base import NodeHash, Edge, NodesMask, FULL_MASK, HashType, TreeNodes, NodeHashes
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
        return any(x.data is Nothing for x in hashes)


class PropagateNothing(Edge):
    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        if Nothing.in_hashes(inputs):
            # TODO: singleton
            return NodeHash.from_leaf(Nothing)

        return self._propagate(inputs)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        if Nothing.in_data(arguments) or Nothing.in_hashes([node_hash]):
            return Nothing

        return self._eval(arguments, mask, node_hash)

    @abstractmethod
    def _propagate(self, inputs: Sequence[NodeHash]) -> NodeHash:
        pass

    @abstractmethod
    def _eval(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        pass


class FunctionEdge(PropagateNothing):
    def __init__(self, function: Callable, arity: int):
        super().__init__(arity, uses_hash=False)
        self.function = function

    def _calc_hash(self, hashes):
        return NodeHash.from_hash_nodes(NodeHash.from_leaf(self.function), *hashes, prev_edge=self)

    def _eval(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        return self.function(*arguments)

    def _propagate(self, inputs: Sequence[NodeHash]) -> NodeHash:
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


class ValueEdge(Edge):
    """
    Used in interface to provide constant parameters.
    """

    def __init__(self, value):
        super().__init__(arity=0, uses_hash=False)
        self.value = value
        self._hash = NodeHash.from_leaf(self.value)

    def _evaluate(self, arguments: Sequence, essential_inputs: TreeNodes, parameter: NodeHash):
        return self.value

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return self._hash

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return self._hash

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return FULL_MASK


class CacheEdge(PropagateNothing):
    def __init__(self, storage: Cache):
        super().__init__(arity=1, uses_hash=True)
        self.storage = storage

    def _propagate(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return inputs[0]

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        if self.storage.contains(output):
            return []
        return FULL_MASK

    def _eval(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
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
        return NodeHash.from_hash_nodes(*inputs, prev_edge=self)

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return NodeHash.from_hash_nodes(*inputs, prev_edge=self)


# TODO: are Switch and Projection the only edges that need Nothing?
# TODO: does Nothing live only in hashes?
class SwitchEdge(Edge):
    def __init__(self, selector: Callable):
        super().__init__(arity=1, uses_hash=True)
        self.selector = selector

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        node_hash, = inputs
        if not self.selector(node_hash):
            # TODO: need a special type for hash of nothing
            node_hash = NodeHash.from_leaf(Nothing)
        return node_hash

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return FULL_MASK

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        if node_hash.data is Nothing:
            return Nothing

        return arguments[0]

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return inputs[0]


class ProjectionEdge(Edge):
    def __init__(self):
        super().__init__(arity=1, uses_hash=True)

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        # take the only non-Nothing hash
        real = []
        for v in inputs[0].children:
            if v.data is not Nothing:
                real.append(v)

        assert len(real) == 1, real
        return real[0]

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return FULL_MASK

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        # take the only non-Nothing value
        real = []
        for v in arguments[0]:
            if v is not Nothing:
                real.append(v)

        assert len(real) == 1, real
        return real[0]

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return inputs[0]


class CachedRow(PropagateNothing):
    def __init__(self, disk: DiskCache, ram: MemoryCache, graph: Graph):
        super().__init__(arity=3, uses_hash=True)
        self.graph = graph
        self.disk = disk
        self.ram = ram

    def _propagate(self, inputs: Sequence[NodeHash]) -> NodeHash:
        """
        Hashes
        ------
        entry: the hash for the entry at ``key``
        key: a unique key for each entry in the tuple
        keys: all available keys
        """
        return inputs[0]

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        if self.ram.contains(output):
            return []
        return [1, 2]

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return inputs[0]

    def _eval(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        if not arguments:
            return self.ram.get(node_hash)

        key, keys = arguments
        keys = sorted(keys)
        assert key in keys

        hashes = []
        for k in keys:
            h = self.graph.eval_hash(NodeHash.from_leaf(k))
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

    def _hash(self, hashes):
        keys, = hashes
        args = self.graph.hash()
        return NodeHash.from_hash_nodes(
            NodeHash.from_leaf(self.func), keys, args,
            kind=HashType.FILTER,
        )

    def _propagate(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return self._hash(inputs)

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return FULL_MASK

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return self._hash(inputs)

    def _eval(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        keys, = arguments
        result = []
        for key in keys:
            args = self.graph.eval(key)
            if self.func(*args):
                result.append(key)

        return tuple(result)
