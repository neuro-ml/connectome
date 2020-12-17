from enum import unique, IntEnum
from typing import Sequence, Tuple, Union, NamedTuple, Optional


@unique
class HashType(IntEnum):
    LEAF = 0
    COMPOUND = 1
    FILTER = 2


class NodeHash:
    __slots__ = 'kind', 'data', 'value', 'children', '_hash'

    def __init__(self, data, children, kind: HashType):
        # TODO: self.prev_edge = prev_edge
        self.children: Sequence[NodeHash] = children
        self.kind = kind
        self.data = data
        self.value = kind.value, data
        # TODO: reuse children?
        self._hash = hash(self.value)

    # TODO: at this point it looks like 2 different objects
    @classmethod
    def from_leaf(cls, data):
        assert not isinstance(data, NodeHash)
        return cls(data, (), kind=HashType.LEAF)

    @classmethod
    def from_hash_nodes(cls, *hashes: 'NodeHash', prev_edge=None, kind=HashType.COMPOUND):
        data = tuple(h.value for h in hashes)
        return cls(data, hashes, kind=kind)

    def __hash__(self):
        return self._hash

    def __repr__(self):
        if self.kind == HashType.LEAF:
            # FIXME
            from connectome.engine.edges import Nothing

            if self.data is Nothing:
                name = 'Nothing'
            else:
                name = f'Leaf'
        else:
            name = f'Compound'

        return f'<NodeHash: {name}>'

    def __eq__(self, other):
        return isinstance(other, NodeHash) and self.value == other.value


FULL_MASK = None
NodesMask = Union[Sequence[int], FULL_MASK]


class Edge:
    def __init__(self, arity: int, uses_hash: bool):
        self.arity = arity
        self._uses_hash = uses_hash

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

    def hash_graph(self, hashes: Sequence[NodeHash]) -> NodeHash:
        assert len(hashes) == self.arity
        return self._hash_graph(hashes)

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        raise NotImplementedError

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        raise NotImplementedError

    def _hash_graph(self, hashes: Sequence[NodeHash]) -> NodeHash:
        raise NotImplementedError

    @property
    def uses_hash(self):
        return self._uses_hash


class TreeNode:
    __slots__ = 'name', 'edge'

    def __init__(self, name: str, edge: Optional[Tuple[Edge, Sequence['TreeNode']]]):
        self.name, self.edge = name, edge

    @classmethod
    def from_edges(cls, edges: Sequence['BoundEdge']) -> dict:
        def update(node: Node):
            if node in mapping:
                return mapping[node]

            bridge = bridges.get(node)
            if bridge is not None:
                bridge = bridge.edge, tuple(update(x) for x in bridge.inputs)
            mapping[node] = new = cls(node.name, bridge)
            return new

        nodes = set()
        bridges = {}
        # each edge is represented by its output
        for edge in edges:
            assert edge.output not in bridges, edge
            bridges[edge.output] = edge
            nodes.add(edge.output)
            nodes.update(edge.inputs)

        mapping = {}
        for n in nodes:
            update(n)
        return mapping

    def __str__(self):
        return f'<TreeNode: {self.name}>'

    def __repr__(self):
        return str(self)

    def visualize(self, path, cache=()):
        """
        Useful for visualization during debug. Requires `graphviz` (not the python package) to be installed.
        """
        from anytree.exporter import UniqueDotExporter
        from anytree import Node

        def convert(node):
            return Node(node.name, original=node, edge=f'label={type(node.edge[0]).__name__}' if node.edge else '',
                        children=list(map(convert, node.edge[1] if node.edge else [])))

        UniqueDotExporter(
            convert(self),
            edgeattrfunc=lambda parent, child: parent.edge,
            nodeattrfunc=lambda
                node: f"shape={'box' if node.original not in cache else 'ellipse'}, label=\"{node.name}\"",
            nodenamefunc=lambda node: hex(id(node.original))
        ).to_picture(path)


TreeNodes = Sequence[TreeNode]


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
