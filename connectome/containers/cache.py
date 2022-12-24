from abc import ABC, abstractmethod

from .base import EdgesBag, Container
from .context import IdentityContext
from ..cache import Cache, MemoryCache, DiskCache
from ..engine.base import Node, TreeNode
from ..engine.edges import CacheEdge, IdentityEdge, ImpureEdge
from ..engine.graph import Graph
from ..exceptions import DependencyError
from ..layers.columns import CachedColumn
from ..utils import node_to_dict, deprecation_warn


class CacheBase(Container):
    pass


class CacheContainer(CacheBase, ABC):  # pragma: no cover
    def __init__(self, names, allow_impure):
        deprecation_warn()
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

        return EdgesBag(
            state.inputs, outputs, edges, IdentityContext(), persistent_nodes=state.persistent_nodes,
            optional_nodes=None,
        )

    @staticmethod
    def _detect_impure(node: TreeNode, name: str):
        if node.is_leaf:
            return

        if isinstance(node.edge, ImpureEdge):
            raise ValueError(f'You are trying to cache the field "{name}", '
                             f'which has an `impure` dependency - "{node.name}"')

        for parent in node.parents:
            CacheContainer._detect_impure(parent, name)


class MemoryCacheContainer(CacheContainer):  # pragma: no cover
    def __init__(self, names, size, allow_impure, cache_instances):
        super().__init__(names, allow_impure)
        self.cache_instances = cache_instances
        self.size = size

    def get_storage(self):
        cache = MemoryCache(self.size)
        self.cache_instances.add(cache)
        return cache


class DiskCacheContainer(CacheContainer):  # pragma: no cover
    def __init__(self, names, local, remote, allow_impure):
        super().__init__(names, allow_impure)
        self.storage = DiskCache(local, remote, fetch=bool(remote))

    def get_storage(self):
        return self.storage


class CacheColumnsContainer(CacheBase):  # pragma: no cover
    """
    CacheRow = Product + CacheToDisk + CacheToRam + Projection
    """

    def __init__(self, names, local, remote, verbose, shard_size):
        deprecation_warn()
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

        return EdgesBag(
            [key], outputs, edges, IdentityContext(), persistent_nodes=main.persistent_nodes, optional_nodes=None
        )
