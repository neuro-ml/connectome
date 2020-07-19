from pathlib import Path
from typing import Sequence

from connectome.engine.base import BoundEdge, Node
from connectome.engine.edges import CacheEdge, IdentityEdge
from connectome.utils import check_for_duplicates, node_to_dict

from .base import Attachable, Nodes, Tuple, Edges
from ..cache import DiskStorage, MemoryStorage


class CacheLayer(Attachable):
    def __init__(self, names):
        self.cache_names = names

    def get_storage(self):
        raise NotImplementedError

    def attach(self, forwards: Nodes, backwards: Nodes) -> Tuple[Nodes, Nodes, Edges]:
        # TODO: add backward support
        assert not backwards

        check_for_duplicates([x.name for x in forwards])
        forwards = node_to_dict(forwards)

        edges = []
        outputs = [Node(name) for name in forwards]

        for node in outputs:
            if node.name in self.cache_names:
                edges.append(BoundEdge(CacheEdge(self.get_storage()), [forwards[node.name]], node))
            else:
                edges.append(BoundEdge(IdentityEdge(), [forwards[node.name]], node))

        return outputs, [], edges

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
