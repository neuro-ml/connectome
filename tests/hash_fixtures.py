from typing import Sequence, Tuple

import pytest

from connectome.engine.base import Edge, NodeHash, NodesMask, FULL_MASK, Node
from connectome.interface.base import FromLayer
from connectome.layers.base import Wrapper, EdgesBag
from connectome.layers.transform import TransformLayer


class HashEdge(Edge):
    def __init__(self):
        super().__init__(arity=1, uses_hash=True)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        return arguments[0], node_hash

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        return hashes[0], FULL_MASK


class HashLayer(Wrapper):
    def wrap(self, layer: EdgesBag) -> EdgesBag:
        state = layer.freeze()

        outputs, edges = [], list(state.edges)
        for node in state.outputs:
            output = Node(node.name)
            outputs.append(output)
            edges.append(HashEdge().bind(node, output))

        return TransformLayer(state.inputs, outputs, edges)


@pytest.fixture
def hash_layer():
    return FromLayer(HashLayer())
