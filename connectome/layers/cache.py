from typing import Sequence

from .base import LayerParams, Attachable, Nodes, Tuple, BoundEdges, EdgesBag, Wrapper
from .utils import make_static_product
from ..engine.base import BoundEdge, Node
from ..engine.edges import CacheEdge, IdentityEdge, CachedKeyProjection
from ..storage.remote import RemoteStorage
from ..utils import check_for_duplicates, node_to_dict
from ..storage.base import MemoryStorage, CacheStorage
from ..storage.disk import DiskStorage


class CacheLayer(Attachable):
    def __init__(self, names):
        self.cache_names = names

    def prepare(self) -> LayerParams:
        # TODO: do it better
        return LayerParams([], [], [], [], [], set())

    def get_storage(self) -> CacheStorage:
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
        return MemoryStorage(self.size)


class DiskCacheLayer(CacheLayer):
    def __init__(self, names, root, options, serializer, metadata):
        super().__init__(names)
        self.storage = DiskStorage(root, options, serializer, metadata)

    def get_storage(self):
        return self.storage


class RemoteStorageLayer(CacheLayer):
    def __init__(self, names, options, serializer):
        super().__init__(names)
        self.storage = RemoteStorage(options, serializer)

    def get_storage(self):
        return self.storage


# TODO: caches need a common parent
class CacheRowsLayer(Wrapper):
    """
    CacheRow = Product + CacheToDisk + CacheToRam + Projection
    """

    def __init__(self, names, root, options, serializer, metadata):
        self.cache_names = names
        self.disk = DiskStorage(root, options, serializer, metadata)

    def wrap(self, layer: EdgesBag) -> EdgesBag:
        # TODO: this is bad
        keys = sorted(layer.get_forward_method('ids')())

        outputs, edges = [], []
        main = layer.prepare()
        real_input, = main.inputs
        edges.extend(main.edges)
        main_outputs = {}
        for output in main.outputs:
            if output.name not in self.cache_names:
                outputs.append(output)
            else:
                main_outputs[output.name] = output

        # TODO: this is still long for large datasets
        #  a better solution would be to keep a single copy and run it N times
        #  when the values are not cached.
        #  It would also fix the need to compute the ids inside this function.
        prod_edges, prod_outputs = make_static_product(layer, keys, self.cache_names)
        edges.extend(prod_edges)

        for name, node in prod_outputs.items():
            new, aux = self._combine(
                [node], CacheEdge(self.disk),
                name=name
            )
            edges.extend(new)
            output = Node(name)
            edges.append(BoundEdge(CachedKeyProjection(keys), [real_input, main_outputs[name], aux], output))
            outputs.append(output)

        return EdgesBag([real_input], outputs, edges, [], [])

    @staticmethod
    def _combine(inputs, *edges, name):
        results = []
        for edge in edges:
            out = Node(name)
            results.append(BoundEdge(edge, inputs, out))
            inputs = [out]

        return results, out
