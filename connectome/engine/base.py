from abc import ABC, abstractmethod
from typing import Sequence, Tuple, Union, NamedTuple, Optional, Any

from .node_hash import NodeHash, NodeHashes

FULL_MASK = None
NodesMask = Union[Sequence[int], FULL_MASK]
MaskOutput = Tuple[NodesMask, Any]


class Edge(ABC):
    def __init__(self, arity: int, uses_hash: bool):
        self.arity = arity
        self._uses_hash = uses_hash

    def propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        """ Computes the hash of the output given the input hashes. """
        assert len(inputs) == self.arity
        return self._propagate_hash(inputs)

    @abstractmethod
    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        pass

    def compute_mask(self, inputs: NodeHashes, output: NodeHash) -> MaskOutput:
        """ Computes the mask of the essential inputs. """
        assert len(inputs) == self.arity
        mask, payload = self._compute_mask(inputs, output)
        if mask == FULL_MASK:
            mask = range(self.arity)
        assert all(0 <= x < self.arity for x in mask)
        assert len(set(mask)) == len(mask)
        return mask, payload

    @abstractmethod
    def _compute_mask(self, inputs: NodeHashes, output: NodeHash) -> MaskOutput:
        pass

    def evaluate(self, inputs: Sequence, output: NodeHash, payload: Any) -> Any:
        """ Computes the output value. """
        assert len(inputs) <= self.arity
        return self._evaluate(inputs, output, payload)

    @abstractmethod
    def _evaluate(self, arguments: Sequence, output: NodeHash, payload: Any) -> Any:
        pass

    def handle_exception(self, output: NodeHash, payload: Any):
        pass

    def hash_graph(self, inputs: NodeHashes) -> NodeHash:
        """ Propagates the graph's hash without any control flow. """
        assert len(inputs) == self.arity
        return self._hash_graph(inputs)

    @abstractmethod
    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        pass

    @property
    def uses_hash(self) -> bool:
        return self._uses_hash

    def bind(self, inputs: Union['Node', 'Nodes'], output: 'Node') -> 'BoundEdge':
        if isinstance(inputs, Node):
            inputs = [inputs]
        assert len(inputs) == self.arity
        return BoundEdge(self, inputs, output)


class TreeNode:
    __slots__ = 'name', '_edge'

    def __init__(self, name: str, edge: Optional[Tuple[Edge, Sequence['TreeNode']]]):
        self.name, self._edge = name, edge

    @property
    def is_leaf(self):
        return self._edge is None

    @property
    def edge(self):
        return self._edge[0]

    @property
    def parents(self):
        return self._edge[1]

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
            return Node(node.name, original=node, edge=f'label={type(node.edge).__name__}' if not node.is_leaf else '',
                        children=list(map(convert, node.parents if not node.is_leaf else [])))

        UniqueDotExporter(
            convert(self),
            edgeattrfunc=lambda parent, child: (parent.edge,),
            nodeattrfunc=lambda
                node: f"shape={'box' if node.original not in cache else 'ellipse'}, label=\"{node.name}\"",
            nodenamefunc=lambda node: hex(id(node.original))
        ).to_picture(path)


class Node:
    def __init__(self, name: str):
        self.name = name

    def __str__(self):
        return f'<Node: {self.name}>'

    def __repr__(self):
        return str(self)


class BoundEdge(NamedTuple):
    edge: Edge
    inputs: 'Nodes'
    output: Node

    __iter__ = None


TreeNodes = Sequence[TreeNode]
Nodes = Sequence[Node]
BoundEdges = Sequence[BoundEdge]
Edges = Sequence[Edge]
