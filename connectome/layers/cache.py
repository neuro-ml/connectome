from typing import Sequence

from .base import LayerParams, Attachable, Nodes, Tuple, Edges
from ..engine.base import BoundEdge, Node
from ..engine.edges import CacheEdge, IdentityEdge
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

    def _attach_forward(self, forward_outputs: Sequence, params: LayerParams) -> Tuple[Nodes, Edges]:
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

    def _attach_backward(self, prev_inputs: Sequence, params: LayerParams) -> Tuple[Nodes, Edges]:
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
    def __init__(self, names, options, serializer):
        super().__init__(names)
        self.serializer = serializer
        self.options = options

    def get_storage(self):
        return DiskStorage(self.options, self.serializer)


class RemoteStorageLayer(CacheLayer):
    def __init__(self, names, options, serializer):
        super().__init__(names)
        self.serializer = serializer
        self.options = options

    def get_storage(self):
        return RemoteStorage(self.options, self.serializer)
