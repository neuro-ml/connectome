from abc import ABC, abstractmethod
from enum import Enum
from typing import Sequence, Tuple, Union, NamedTuple, Optional, Any, Generator

from .node_hash import NodeHash, NodeHashes


class Command(Enum):
    ParentHash, CurrentHash, ParentValue, Payload, Await, Call = range(6)
    Send, Store, Item, ComputeHash, Evaluate, AwaitThunk, Tuple, Return = range(-8, 0)


HashOutput = Tuple[NodeHash, Any]
Request = Tuple  # [RequestType, Any, ...]
Response = Union[NodeHash, Any, Tuple[NodeHash, Any]]


class Edge(ABC):
    def __init__(self, arity: int):
        self.arity = arity

    @abstractmethod
    def compute_hash(self) -> Generator[Request, Response, HashOutput]:
        """ Computes the hash of the output given the input hashes. """

    @abstractmethod
    def evaluate(self) -> Generator[Request, Response, Any]:
        """ Computes the output value. """

    def hash_graph(self, inputs: NodeHashes) -> NodeHash:
        """ Propagates the graph's hash without any control flow. """
        assert len(inputs) == self.arity
        return self._hash_graph(inputs)

    @abstractmethod
    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        pass

    def bind(self, inputs: Union['Node', 'Nodes'], output: 'Node') -> 'BoundEdge':
        if isinstance(inputs, Node):
            inputs = [inputs]
        assert len(inputs) == self.arity, (len(inputs), self.arity)
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
            # TODO: replace by exception
            assert edge.output not in bridges, edge
            bridges[edge.output] = edge
            nodes.add(edge.output)
            nodes.update(edge.inputs)

        mapping = {}
        for n in nodes:
            update(n)
        return mapping

    def __repr__(self):
        return f'<TreeNode: {self.name}>'


class Node:
    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f'<Node: {self.name}>'


class BoundEdge(NamedTuple):
    edge: Edge
    inputs: 'Nodes'
    output: Node

    __iter__ = None


TreeNodes = Sequence[TreeNode]
Nodes = Sequence[Node]
BoundEdges = Sequence[BoundEdge]
Edges = Sequence[Edge]


class HashError(RuntimeError):
    pass
