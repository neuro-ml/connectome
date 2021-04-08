from typing import Sequence, Tuple, Any

import pytest

from connectome.engine.base import Edge, NodeHash, NodesMask, FULL_MASK, Node, NodeHashes
from connectome.engine.edges import FullMask
from connectome.interface.base import BaseBlock
from connectome.layers.base import Wrapper, EdgesBag
from connectome.layers.transform import TransformLayer


class HashEdge(FullMask, Edge):
    def __init__(self):
        super().__init__(arity=1, uses_hash=True)

    def _evaluate(self, arguments: Sequence, output: NodeHash, payload: Any):
        return arguments[0], output

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]


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
    return BaseBlock(HashLayer())
