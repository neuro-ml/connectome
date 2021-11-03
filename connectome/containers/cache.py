from itertools import starmap
from typing import Any, Generator

from tqdm import tqdm

from .base import Nodes, Tuple, BoundEdges, EdgesBag, Wrapper, Context
from ..engine import NodeHash
from ..engine.base import Node, TreeNode, Edge, HashOutput, Request, Response, Command
from ..engine.edges import CacheEdge, IdentityEdge, ImpureFunctionEdge
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


class CacheBase(Wrapper):
    pass


class CacheContainer(CacheBase):
    def __init__(self, names, allow_impure):
        self.cache_names = names
        self.allow_impure = allow_impure

    def get_storage(self) -> Cache:
        raise NotImplementedError

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

        return EdgesBag(state.inputs, outputs, edges, IdentityContext())

    @staticmethod
    def _detect_impure(node: TreeNode, name: str):
        if node.is_leaf:
            return

        if isinstance(node.edge, ImpureFunctionEdge):
            raise ValueError(f'Our are trying to cache the field "{name}", '
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
    def __init__(self, names, root, storage, serializer, allow_impure):
        super().__init__(names, allow_impure)
        self.storage = DiskCache(root, storage, serializer)

    def get_storage(self):
        return self.storage


class CacheColumnsContainer(CacheBase):
    """
    CacheRow = Product + CacheToDisk + CacheToRam + Projection
    """

    def __init__(self, names, root, storage, serializer, verbose):
        self.verbose = verbose
        self.cache_names = names
        self.disk = DiskCache(root, storage, serializer)
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
                edges.append(CachedColumn(self.disk, self.ram, graph, self.verbose).bind([output, key, keys], local))
                outputs.append(local)

        return EdgesBag([key], outputs, edges, IdentityContext())


class CachedColumn(Edge):
    """
    Edge Inputs
    -----------
    entry: the entry at ``key``
    key: a unique key for each entry in the tuple
    keys: all available keys
    """

    def __init__(self, disk: DiskCache, ram: MemoryCache, graph: Graph, verbose: bool):
        super().__init__(arity=3)
        self.graph = graph
        self.disk = disk
        self.ram = ram
        self.verbose = verbose

    def compute_hash(self) -> Generator[Request, Response, HashOutput]:
        # propagate the first value
        value = yield Command.ParentHash, 0
        return value, None

    def evaluate(self) -> Generator[Request, Response, Any]:
        output = yield Command.CurrentHash,
        value, exists = self.ram.get(output)
        if exists:
            return value

        key = yield Command.ParentValue, 1
        keys = yield Command.ParentValue, 2
        keys = sorted(keys)
        assert key in keys

        hashes, states = [], []
        for k in keys:
            h, state = self.graph.get_hash(k)
            hashes.append(h)
            states.append(state)
            if k == key:
                assert output == h, (output, h)
        # TODO: hash the graph?
        compound = TupleHash(*hashes)

        values, exists = self.disk.get(compound)
        if not exists:
            values = tuple(starmap(self.graph.get_value, tqdm(
                states, desc='Generating the columns cache', disable=not self.verbose,
            )))
            self.disk.set(compound, values)

        for k, h, value in zip(keys, hashes, values):
            self.ram.set(h, value)
            if k == key:
                result = value

        return result  # noqa

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]
