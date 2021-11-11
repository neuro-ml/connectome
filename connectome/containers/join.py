from collections import defaultdict
from typing import Callable, Generator, Any

from ..containers.base import EdgesBag
from ..engine import NodeHash
from ..engine.base import Node, Edge, Command, Request, Response, HashOutput, TreeNode
from ..engine.edges import StaticGraph, StaticHash, FunctionEdge, ProductEdge
from ..engine.graph import Graph
from ..engine.node_hash import LeafHash, NodeHashes, JoinMappingHash
from ..utils import Strings, node_to_dict


class JoinContainer(EdgesBag):
    def __init__(self, left: EdgesBag, right: EdgesBag, on: Strings, id_maker: Callable):
        assert len(set(on)) == len(on), on
        left, right = left.freeze(), right.freeze()

        edges = [*left.edges, *right.edges]
        outputs = []

        if len(left.inputs) != 1 or len(right.inputs) != 1:
            raise ValueError('Both layers should have exactly one input')

        # build the core mapping
        outputs_left, outputs_right = node_to_dict(left.outputs), node_to_dict(right.outputs)
        # TODO: parametrize these names
        keys_left, keys_right = outputs_left.pop('ids'), outputs_right.pop('ids')
        outputs_left.pop('id')
        outputs_right.pop('id')

        intersection = set(outputs_left) & set(outputs_right)
        missing = set(on) - intersection
        if missing:
            raise ValueError(f'Fields {missing} are missing')
        conflict = intersection - set(on)
        if conflict:
            raise ValueError(f'Field conflicts resolution not supported yet. Conflicts: {conflict}')

        mapping = Node('$mapping')
        edges.append(JoinMappingEdge(
            self._make_graph(left.inputs, outputs_left, left.edges, on),
            self._make_graph(right.inputs, outputs_right, right.edges, on),
            id_maker,
        ).bind([keys_left, keys_right], mapping))

        # add the new keys
        keys = Node('ids')
        outputs.append(keys)
        edges.append(FunctionEdge(lambda x: tuple(sorted(x)), 1).bind(mapping, keys))

        # add the new input
        inp = Node('id')
        edges.append(KeyProjection(0).bind([inp, mapping], left.inputs[0]))
        edges.append(KeyProjection(1).bind([inp, mapping], right.inputs[0]))

        # no_conflict = set(outputs_left) | set(outputs_right) - intersection
        outputs.extend(outputs_left.values())
        outputs.extend(outputs_right[x] for x in set(outputs_right) - set(outputs_left))
        super().__init__([inp], outputs, edges, None, persistent_nodes=left.persistent_nodes & right.persistent_nodes)

    @staticmethod
    def _make_graph(inputs, outputs, edges, on):
        edges = list(edges)
        output = Node('$output')
        edges.append(ProductEdge(len(on)).bind([outputs[x] for x in on], output))
        mapping = TreeNode.from_edges(edges)
        return Graph([mapping[x] for x in inputs], mapping[output])


class JoinMappingEdge(StaticGraph, StaticHash):
    def __init__(self, left: Graph, right: Graph, id_maker: Callable):
        super().__init__(arity=2)
        self.left = left
        self.right = right
        self.id_maker = id_maker
        self._hashes = left.hash(), right.hash()
        # TODO: this is potentially dangerous. should use a composition of Mapping and MemoryCache
        self._mapping = None

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return JoinMappingHash(*self._hashes, *inputs, id_maker=self.id_maker)

    def evaluate(self) -> Generator[Request, Response, Any]:
        if self._mapping is not None:
            return self._mapping

        left_keys, right_keys = yield Command.Await, (Command.ParentValue, 0), (Command.ParentValue, 1)
        precomputed = defaultdict(list)
        for i in left_keys:
            left = self.left.call(i)
            precomputed[left].append(i)

        mapping = {}
        for j in right_keys:
            right = self.right.call(j)
            for i in precomputed[right]:
                key = self.id_maker((i, j))
                if key in mapping:
                    raise ValueError(f'The provided key function is not reversible: value {key} already present')
                mapping[key] = i, j

        self._mapping = mapping
        return mapping


class KeyProjection(Edge):
    def __init__(self, index):
        super().__init__(2)
        self.index = index

    def compute_hash(self) -> Generator[Request, Response, HashOutput]:
        key, mapping = yield Command.Await, (Command.ParentValue, 0), (Command.ParentValue, 1)
        value = mapping[key][self.index]
        return LeafHash(value), value

    def evaluate(self) -> Generator[Request, Response, Any]:
        value = yield Command.Payload,
        return value

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        raise RuntimeError('Join cannot be currently a part of a subgraph')
