from typing import Sequence, Any

from .base import Nodes, Tuple, BoundEdges, EdgesBag, Wrapper, Context
from ..engine import NodeHash
from ..engine.base import BoundEdge, Node, TreeNode, NodesMask, Edge
from ..engine.edges import CacheEdge, IdentityEdge
from ..engine.graph import Graph
from ..engine.node_hash import NodeHashes
from ..utils import node_to_dict
from ..cache import Cache, MemoryCache, DiskCache, RemoteCache


class IdentityContext(Context):
    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        # just propagate everything
        return outputs, edges

    def update(self, mapping: dict) -> 'Context':
        return self


class CacheBase(Wrapper):
    pass


class CacheLayer(CacheBase):
    def __init__(self, names):
        self.cache_names = names

    def get_storage(self) -> Cache:
        raise NotImplementedError

    def wrap(self, layer: 'EdgesBag') -> 'EdgesBag':
        state = layer.freeze()
        forward_outputs = node_to_dict(state.outputs)

        edges = list(state.edges)
        outputs = [Node(name) for name in forward_outputs]

        for node in outputs:
            if self.cache_names is None or node.name in self.cache_names:
                edges.append(CacheEdge(self.get_storage()).bind(forward_outputs[node.name], node))
            else:
                edges.append(IdentityEdge().bind(forward_outputs[node.name], node))

        return EdgesBag(state.inputs, outputs, edges, IdentityContext())


class MemoryCacheLayer(CacheLayer):
    def __init__(self, names, size):
        super().__init__(names)
        self.size = size

    def get_storage(self):
        return MemoryCache(self.size)


class DiskCacheLayer(CacheLayer):
    def __init__(self, names, root, options, serializer, metadata):
        super().__init__(names)
        self.storage = DiskCache(root, options, serializer, metadata)

    def get_storage(self):
        return self.storage


class RemoteStorageLayer(CacheLayer):
    def __init__(self, names, options, serializer):
        super().__init__(names)
        self.storage = RemoteCache(options, serializer)

    def get_storage(self):
        return self.storage


class CacheColumnsLayer(CacheBase):
    """
    CacheRow = Product + CacheToDisk + CacheToRam + Projection
    """

    def __init__(self, names, root, options, serializer, metadata):
        self.cache_names = names
        self.disk = DiskCache(root, options, serializer, metadata)
        self.ram = MemoryCache(None)

    def wrap(self, layer: EdgesBag) -> EdgesBag:
        main = layer.freeze()
        edges = list(main.edges)
        key, = main.inputs
        keys = node_to_dict(main.outputs)['ids']

        copy = layer.freeze()
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
                edges.append(BoundEdge(CachedColumn(self.disk, self.ram, graph), [output, key, keys], local))
                outputs.append(local)

        return EdgesBag([key], outputs, edges, IdentityContext())


class CachedColumn(Edge):
    def __init__(self, disk: DiskCache, ram: MemoryCache, graph: Graph):
        super().__init__(arity=3, uses_hash=True)
        self.graph = graph
        self.disk = disk
        self.ram = ram

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
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

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash) -> Any:
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
