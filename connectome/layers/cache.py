from typing import Sequence

from .base import LayerParams, Attachable, Nodes, Tuple, BoundEdges, EdgesBag, Wrapper
from ..engine.base import BoundEdge, Node, TreeNode
from ..engine.edges import CacheEdge, IdentityEdge, CachedRow
from ..engine.graph import Graph
from ..utils import check_for_duplicates, node_to_dict
from ..cache import Cache, MemoryCache, DiskCache, RemoteCache


class CacheLayer(Attachable):
    def __init__(self, names):
        self.cache_names = names

    def prepare(self) -> LayerParams:
        # TODO: do it better
        return LayerParams([], [], [], [], [], set())

    def get_storage(self) -> Cache:
        raise NotImplementedError

    def _attach_forward(self, forward_outputs: Sequence, params: LayerParams) -> Tuple[Nodes, BoundEdges]:
        check_for_duplicates([x.name for x in forward_outputs])
        forward_outputs = node_to_dict(forward_outputs)

        edges = []
        outputs = [Node(name) for name in forward_outputs]

        for node in outputs:
            if self.cache_names is None or node.name in self.cache_names:
                edges.append(BoundEdge(CacheEdge(self.get_storage()), [forward_outputs[node.name]], node))
            else:
                edges.append(BoundEdge(IdentityEdge(), [forward_outputs[node.name]], node))

        return outputs, edges

    def _attach_backward(self, prev_inputs: Sequence, params: LayerParams) -> Tuple[Nodes, BoundEdges]:
        check_for_duplicates([x.name for x in prev_inputs])
        prev_inputs = node_to_dict(prev_inputs)

        edges = []
        inputs = [Node(name) for name in prev_inputs]
        for node in inputs:
            edges.append(BoundEdge(IdentityEdge(), [node], prev_inputs[node.name]))
        return inputs, edges


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


# TODO: caches need a common parent
class CacheRowsLayer(Wrapper):
    """
    CacheRow = Product + CacheToDisk + CacheToRam + Projection
    """

    def __init__(self, names, root, options, serializer, metadata):
        self.cache_names = names
        self.disk = DiskCache(root, options, serializer, metadata)
        self.ram = MemoryCache(None)

    def wrap(self, layer: EdgesBag) -> EdgesBag:
        outputs, edges = [], []

        main = layer.prepare()
        edges.extend(main.edges)
        key, = main.inputs
        keys = node_to_dict(main.outputs)['ids']

        copy = layer.prepare()
        mapping = TreeNode.from_edges(copy.edges)
        outputs_copy = node_to_dict(copy.outputs)
        graph_inputs = [mapping[copy.inputs[0]]]

        for output in main.outputs:
            name = output.name
            if name not in self.cache_names:
                outputs.append(output)

            else:
                local = Node(name)
                # build a graph for each node
                graph = Graph(graph_inputs, mapping[outputs_copy[name]])
                edges.append(BoundEdge(CachedRow(self.disk, self.ram, graph), [output, key, keys], local))
                outputs.append(local)

        return EdgesBag([key], outputs, edges, [], [])
