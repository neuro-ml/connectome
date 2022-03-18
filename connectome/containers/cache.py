from abc import ABC, abstractmethod
from itertools import starmap
from math import ceil
from typing import Any, Generator, Union

from tqdm.auto import tqdm

from .base import Nodes, Tuple, BoundEdges, EdgesBag, Container, Context
from ..engine import NodeHash
from ..engine.base import Node, TreeNode, Edge, HashOutput, Request, Response, Command
from ..engine.edges import CacheEdge, IdentityEdge, ImpureEdge
from ..engine.graph import Graph
from ..engine.node_hash import NodeHashes, TupleHash
from ..exceptions import DependencyError
from ..utils import node_to_dict
from ..cache import Cache, MemoryCache, DiskCache


class IdentityContext(Context):
    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        # just propagate everything
        return outputs, edges

    def update(self, mapping: dict) -> 'Context':
        return self


class CacheBase(Container):
    pass


class CacheContainer(CacheBase, ABC):
    def __init__(self, names, allow_impure):
        self.cache_names = names
        self.allow_impure = allow_impure

    @abstractmethod
    def get_storage(self) -> Cache:
        """ Create a cache storage instance """

    def wrap(self, container: 'EdgesBag') -> 'EdgesBag':
        state = container.freeze()
        forward_outputs = node_to_dict(state.outputs)

        edges = list(state.edges)
        outputs = [Node(name) for name in forward_outputs]
        mapping = TreeNode.from_edges(state.edges)

        for node in outputs:
            if self.cache_names is None or node.name in self.cache_names:
                if not self.allow_impure:
                    self._detect_impure(mapping[forward_outputs[node.name]], node.name)
                edges.append(CacheEdge(self.get_storage()).bind(forward_outputs[node.name], node))
            else:
                edges.append(IdentityEdge().bind(forward_outputs[node.name], node))

        return EdgesBag(state.inputs, outputs, edges, IdentityContext(), persistent_nodes=state.persistent_nodes)

    @staticmethod
    def _detect_impure(node: TreeNode, name: str):
        if node.is_leaf:
            return

        if isinstance(node.edge, ImpureEdge):
            raise ValueError(f'You are trying to cache the field "{name}", '
                             f'which has an `impure` dependency - "{node.name}"')

        for parent in node.parents:
            CacheContainer._detect_impure(parent, name)


class MemoryCacheContainer(CacheContainer):
    def __init__(self, names, size, allow_impure, cache_instances):
        super().__init__(names, allow_impure)
        self.cache_instances = cache_instances
        self.size = size

    def get_storage(self):
        cache = MemoryCache(self.size)
        self.cache_instances.add(cache)
        return cache


class DiskCacheContainer(CacheContainer):
    def __init__(self, names, local, remote, allow_impure):
        super().__init__(names, allow_impure)
        self.storage = DiskCache(local, remote, fetch=bool(remote))

    def get_storage(self):
        return self.storage


class CacheColumnsContainer(CacheBase):
    """
    CacheRow = Product + CacheToDisk + CacheToRam + Projection
    """

    def __init__(self, names, local, remote, verbose, shard_size):
        self.shard_size = shard_size
        self.verbose = verbose
        self.cache_names = names
        self.disk = DiskCache(local, remote, bool(remote))
        self.ram = MemoryCache(None)

    def wrap(self, container: EdgesBag) -> EdgesBag:
        main = container.freeze()
        edges = list(main.edges)
        key, = main.inputs
        main_outputs = node_to_dict(main.outputs)
        # TODO: containers must know about property names
        property_name = 'ids'
        if property_name not in main_outputs:
            raise DependencyError(f'The previous layer must contain the "{property_name}" property.')

        keys = main_outputs[property_name]

        copy = container.freeze()
        mapping = TreeNode.from_edges(copy.edges)
        outputs_copy = node_to_dict(copy.outputs)
        graph_inputs = [mapping[copy.inputs[0]]]

        outputs = []
        for output in main.outputs:
            name = output.name
            if name not in self.cache_names:
                outputs.append(output)

            else:
                local = Node(name)
                # build a graph for each node
                graph = Graph(graph_inputs, mapping[outputs_copy[name]])
                edges.append(CachedColumn(
                    self.disk, self.ram, graph, self.verbose, self.shard_size).bind([output, key, keys], local))
                outputs.append(local)

        return EdgesBag([key], outputs, edges, IdentityContext(), persistent_nodes=main.persistent_nodes)


class CachedColumn(Edge):
    """
    Edge Inputs
    -----------
    entry: the entry at ``key``
    key: a unique key for each entry in the tuple
    keys: all available keys
    """

    def __init__(self, disk: DiskCache, ram: MemoryCache, graph: Graph, verbose: bool,
                 shard_size: Union[int, float, None]):
        super().__init__(arity=3)
        self.graph = graph
        self.disk = disk
        self.ram = ram
        self.verbose = verbose
        self.shard_size = shard_size

    def compute_hash(self) -> Generator[Request, Response, HashOutput]:
        # propagate the first value
        value = yield Command.ParentHash, 0
        return value, None

    def _get_shard(self, key, keys):
        keys = sorted(keys)
        assert key in keys

        size = self.shard_size
        if size is None:
            return keys, 1, 0
        if isinstance(size, float):
            size = ceil(size * len(keys))

        assert size > 0
        idx = keys.index(key) // size
        count = ceil(len(keys) / size)
        start = idx * size
        keys = keys[start:start + size]
        assert key in keys
        return keys, count, idx

    def evaluate(self) -> Generator[Request, Response, Any]:
        output = yield Command.CurrentHash,
        value, exists = self.ram.raw_get(output)
        if exists:
            return value

        key = yield Command.ParentValue, 1
        keys = yield Command.ParentValue, 2
        keys, shards_count, shard_idx = self._get_shard(key, keys)

        hashes, states = [], []
        for k in keys:
            h, state = self.graph.get_hash(k)
            hashes.append(h)
            states.append(state)
            if k == key:
                assert output == h, (output, h)
        # TODO: hash the graph?
        compound = TupleHash(*hashes)

        digest, context = self.disk.prepare(compound)
        values, exists = self.disk.get(digest, context)
        if not exists:
            suffix = '' if shards_count == 1 else f' ({shard_idx}/{shards_count})'
            values = tuple(starmap(self.graph.get_value, tqdm(
                states, desc=f'Generating the columns cache{suffix}', disable=not self.verbose,
            )))
            self.disk.set(digest, values, context)

        for k, h, value in zip(keys, hashes, values):
            self.ram.raw_set(h, value)
            if k == key:
                result = value

        return result  # noqa

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]
