from pathlib import Path
from typing import Sequence

from ..engine.base import BoundEdge, Node
from ..engine.edges import CacheEdge, IdentityEdge
from ..utils import check_for_duplicates, node_to_dict
from .base import Attachable, Nodes, Tuple, Edges
from ..cache import DiskStorage, MemoryStorage


class CacheLayer(Attachable):
    def __init__(self, names):
        self.cache_names = names

    def get_storage(self):
        raise NotImplementedError

    def attach(self, forward_outputs: Nodes, backward_inputs: Nodes) -> Tuple[Nodes, Nodes, Edges]:
        # TODO: add backward support
        assert not backward_inputs

        check_for_duplicates([x.name for x in forward_outputs])
        forward_outputs = node_to_dict(forward_outputs)

        edges = []
        outputs = [Node(name) for name in forward_outputs]

        for node in outputs:
            if self.cache_names is None or node.name in self.cache_names:
                edges.append(BoundEdge(CacheEdge(self.get_storage()), [forward_outputs[node.name]], node))
            else:
                edges.append(BoundEdge(IdentityEdge(), [forward_outputs[node.name]], node))

        return outputs, [], edges

    def get_backward_params(self, other_inputs: Sequence[Node]):
        this_inputs = [Node(o.name) for o in other_inputs]
        edges = list(map(IdentityEdge, this_inputs, other_inputs))
        return this_inputs, edges


class MemoryCacheLayer(CacheLayer):
    def __init__(self, names, size):
        super().__init__(names)
        self.size = size

    def get_storage(self):
        return MemoryStorage(self.size)


class DiskCacheLayer(CacheLayer):
    def __init__(self, names, storage):
        super().__init__(names)
        self.storage = Path(storage)

    def get_storage(self):
        return DiskStorage(self.storage)
