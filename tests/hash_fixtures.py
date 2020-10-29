from typing import Sequence, Tuple

import pytest

from connectome.engine.base import Edge, NodeHash, NodesMask, FULL_MASK, Node, BoundEdge
from connectome.interface.base import FromLayer
from connectome.layers.base import Attachable, LayerParams, Nodes, BoundEdges


class HashEdge(Edge):
    def __init__(self):
        super().__init__(arity=1, uses_hash=True)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        return arguments[0], node_hash

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        return hashes[0], FULL_MASK


class HashLayer(Attachable):
    def prepare(self) -> LayerParams:
        return LayerParams([], [], [], [], [], set())

    def _attach_forward(self, nodes: Sequence, params: LayerParams) -> Tuple[Nodes, BoundEdges]:
        outputs, edges = [], []
        for node in nodes:
            output = Node(node.name)
            outputs.append(output)
            edges.append(BoundEdge(HashEdge(), [node], output))

        return outputs, edges


@pytest.fixture
def hash_layer():
    return FromLayer(HashLayer())
