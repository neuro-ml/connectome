from collections import defaultdict
from typing import Sequence

from ..engine.edges import ConstantEdge
from ..engine.base import Node, NodeHash, Edge, NodesMask, FULL_MASK, NodeHashes, TreeNode
from ..engine.graph import Graph
from ..engine.node_hash import HashType, CompoundHash, LeafHash
from ..utils import node_to_dict
from .base import EdgesBag


class SwitchLayer(EdgesBag):
    def __init__(self, id_to_index: dict, layers: Sequence[EdgesBag]):
        self.layers = layers
        self.id_to_index = id_to_index
        super().__init__(*self.create_graph(), context=None)

    def create_graph(self):
        # find outputs
        common_outputs = {x.name for x in self.layers[0].outputs} - {'ids'}
        for layer in self.layers[1:]:
            common_outputs &= {x.name for x in layer.outputs}

        # compile graphs
        graphs = defaultdict(list)
        for layer in self.layers:
            layer_params = layer.freeze()
            inp, = layer_params.inputs
            out = node_to_dict(layer_params.outputs)
            mapping = TreeNode.from_edges(layer_params.edges)

            for name in common_outputs:
                graphs[name].append(Graph([mapping[inp]], mapping[out[name]]))

        # make outputs
        inp = Node('id')
        outputs, edges = [], []
        for name, graph in graphs.items():
            out = Node(name)
            outputs.append(out)
            edges.append(SwitchEdge(self.id_to_index, graph).bind(inp, out))

        # and ids
        ids = Node('ids')
        outputs.append(ids)
        edges.append(ConstantEdge(tuple(sorted(self.id_to_index))).bind([], ids))

        return [inp], outputs, edges


class SwitchEdge(Edge):
    def __init__(self, id_to_index: dict, graphs: Sequence[Graph]):
        super().__init__(arity=1, uses_hash=True)
        self.graphs = graphs
        self.id_to_index = id_to_index

    def _select_graph(self, key):
        try:
            return self.graphs[self.id_to_index[key]]
        except KeyError:
            raise ValueError(f'Identifier {key} not found.') from None

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        node_hash, = inputs
        assert isinstance(node_hash, LeafHash)
        graph = self._select_graph(node_hash.data)
        return graph.eval_hash(*inputs)

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return FULL_MASK

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        key, = arguments
        graph = self._select_graph(key)
        return graph.eval(key)

    def _hash_graph(self, inputs: Sequence[NodeHash]) -> NodeHash:
        return CompoundHash(
            *inputs, *(graph.hash() for graph in self.graphs),
            kind=HashType.MERGE
        )
