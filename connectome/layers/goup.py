from collections import defaultdict
from typing import Sequence, Any

from .base import EdgesBag, Wrapper, NoContext
from ..engine import NodeHash
from ..engine.base import Node, TreeNode, NodeHashes, NodesMask, FULL_MASK, Edge
from ..engine.edges import FunctionEdge
from ..engine.graph import Graph
from ..engine.node_hash import HashType


class GroupLayer(Wrapper):
    def __init__(self, name: str):
        self.name = name

    @staticmethod
    def _find(nodes, name):
        for node in nodes:
            if node.name == name:
                return node

        raise ValueError(f'The previous layer must contain the attribute "{name}"')

    def wrap(self, layer: EdgesBag) -> EdgesBag:
        main = layer.freeze()

        inp, = main.inputs
        edges = list(main.edges)
        outputs = []
        mapping = TreeNode.from_edges(edges)
        changed_input = Node('id')
        mapping_node = Node('$mapping')
        ids_node = self._find(main.outputs, 'ids')
        outputs.append(changed_input)

        # create a mapping: {new_id: [old_ids]}
        edges.append(MappingEdge(Graph([mapping[inp]], mapping[self._find(main.outputs, self.name)])).bind(
            [ids_node], mapping_node))

        # evaluate each output
        for node in main.outputs:
            if node.name in [self.name, 'ids', 'id']:
                continue

            output = Node(node.name)
            outputs.append(output)
            edges.append(GroupEdge(Graph([mapping[inp]], mapping[node])).bind(
                [changed_input, mapping_node], output))

        # update ids
        output_ids = Node('ids')
        outputs.append(output_ids)
        edges.append(FunctionEdge(extract_keys, arity=1).bind(mapping_node, output_ids))

        return EdgesBag([changed_input], outputs, edges, NoContext())


class MappingEdge(Edge):
    def __init__(self, graph):
        super().__init__(arity=1, uses_hash=True)
        self.graph = graph
        self._mapping = None

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return NodeHash.from_hash_nodes(
            *inputs, self.graph.hash(),
            kind=HashType.MAPPING,
        )

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        if self._mapping is not None:
            return []
        return FULL_MASK

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash) -> Any:
        if self._mapping is not None:
            return self._mapping

        mapping = defaultdict(list)
        for i in arguments[0]:
            mapping[self.graph.eval(i)].append(i)

        self._mapping = mapping = {k: tuple(sorted(v)) for k, v in mapping.items()}
        return mapping

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return self._propagate_hash(inputs)


class GroupEdge(Edge):
    def __init__(self, graph):
        super().__init__(arity=2, uses_hash=True)
        self.graph = graph

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return NodeHash.from_hash_nodes(
            *inputs, self.graph.hash(),
            kind=HashType.GROUPING,
        )

    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> NodesMask:
        return FULL_MASK

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash) -> Any:
        # get the required ids
        ids = arguments[1][arguments[0]]

        result = {}
        for i in ids:
            result[i] = self.graph.eval(i)

        return result

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return self._propagate_hash(inputs)


def extract_keys(d):
    return tuple(sorted(d))
