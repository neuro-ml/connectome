from enum import unique, IntEnum
from typing import Sequence, Tuple, Dict, Union, NamedTuple


@unique
class HashType(IntEnum):
    LEAF = 0
    COMPOUND = 1


class NodeHash:
    def __init__(self, *data, kind: HashType, prev_edge=None):
        if kind == HashType.LEAF:
            data, = data
            children = ()
            assert not isinstance(data, NodeHash)
        else:
            for entry in data:
                assert isinstance(entry, NodeHash), type(entry)
            children, data = data, None

        self.prev_edge = prev_edge
        self._kind = kind
        self._data = data
        self.children: Sequence[NodeHash] = children

    def __hash__(self):
        return hash(self.value)

    @classmethod
    def from_leaf(cls, data):
        return NodeHash(data, kind=HashType.LEAF)

    @classmethod
    def from_hash_nodes(cls, *hashes: 'NodeHash', prev_edge=None):
        return NodeHash(*hashes, kind=HashType.COMPOUND, prev_edge=prev_edge)

    @property
    def data(self):
        if self._kind == HashType.LEAF:
            return self._data
        else:
            return tuple(h.value for h in self.children)

    @property
    def value(self):
        return self._kind.value, self.data


FULL_MASK = None
NodesMask = Union[Sequence[int], FULL_MASK]


class Edge:
    def __init__(self, arity: int):
        self.arity = arity

    def evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        assert len(arguments) == len(mask)
        return self._evaluate(arguments, mask, node_hash)

    def process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        assert len(hashes) == self.arity
        node_hash, mask = self._process_hashes(hashes)
        if mask == FULL_MASK:
            mask = range(self.arity)
        assert all(0 <= x < self.arity for x in mask)
        assert len(set(mask)) == len(mask)
        return node_hash, mask

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        raise NotImplementedError

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        raise NotImplementedError


class TreeNode:
    def __init__(self, name: str, edges: Dict[Edge, Sequence['TreeNode']]):
        # TODO: need an object that encapsulates this relation
        self.edges = edges
        self.name = name

    def add(self, edge, inputs):
        assert not self.edges, self.edges
        self.edges[edge] = inputs

    @staticmethod
    def from_edges(edges: Sequence['BoundEdge']) -> dict:
        def update(*nodes):
            for node in nodes:
                if node not in mapping:
                    mapping[node] = TreeNode(node.name, {})

        mapping = {}
        for edge in edges:
            update(*edge.inputs, edge.output)
            mapping[edge.output].add(edge.edge, [mapping[x] for x in edge.inputs])

        return mapping

    def __str__(self):
        return f'<Node: {self.name}>'

    def __repr__(self):
        return str(self)


class Node:
    def __init__(self, name: str):
        self.name = name

    def __str__(self):
        return f'<Node: {self.name}>'

    def __repr__(self):
        return str(self)


class BoundEdge(NamedTuple):
    edge: Edge
    inputs: Sequence[Node]
    output: Node
