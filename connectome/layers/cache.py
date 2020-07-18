from pathlib import Path
from typing import Sequence

from ..cache import DiskStorage, MemoryStorage
from connectome.engine.edges import CacheEdge, IdentityEdge
from ..old_engine import Node
from .base import AttachableLayer


class CacheLayer(AttachableLayer):
    def __init__(self, names):
        self.names = names

    def get_storage(self):
        raise NotImplementedError

    def get_forward_params(self, other_outputs: Sequence[Node]):
        this_outputs = [Node(o.name) for o in other_outputs]
        edges = []
        for other_output, this_output in zip(other_outputs, this_outputs):
            if self.names is None or other_output.name in self.names:
                edge = CacheEdge(other_output, this_output, storage=self.get_storage())
            else:
                edge = IdentityEdge(other_output, this_output)

            edges.append(edge)
        return this_outputs, edges

    def get_backward_params(self, other_inputs: Sequence[Node]):
        this_inputs = [Node(o.name) for o in other_inputs]
        edges = list(map(IdentityEdge, this_inputs, other_inputs))
        return this_inputs, edges


class MemoryCacheLayer(CacheLayer):
    def get_storage(self):
        return MemoryStorage()


class DiskCacheLayer(CacheLayer):
    def __init__(self, storage, names):
        super().__init__(names)
        self.storage = Path(storage)

    def get_storage(self):
        return DiskStorage(self.storage)
