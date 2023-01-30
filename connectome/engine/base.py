from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from types import FunctionType
from typing import Sequence, Tuple, Union, NamedTuple, Optional, Any, Generator, Collection, Iterable, Set, Dict, Type

from ..exceptions import GraphError
from .node_hash import NodeHash, NodeHashes

__all__ = (
    'Command', 'HashOutput', 'Request', 'Response', 'HashError',
    'Edge', 'Node', 'Nodes', 'NodeSet', 'BoundEdge', 'BoundEdges', 'TreeNode', 'TreeNodes', 'Details',
)


class Command(Enum):
    ParentHash, CurrentHash, ParentValue, Payload, Await, Call = range(6)
    Send, Store, Item, ComputeHash, Evaluate, AwaitFuture, Tuple, Return = range(-8, 0)


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
        """ Propagates the graph's hash without any control flow. """

    def bind(self, inputs: Union['Node', 'Nodes'], output: 'Node') -> 'BoundEdge':
        if isinstance(inputs, Node):
            inputs = [inputs]
        assert len(inputs) == self.arity, (len(inputs), self.arity)
        return BoundEdge(self, inputs, output)


@dataclass(unsafe_hash=True)
class Details:
    layer: Union[Type, FunctionType] = field(hash=True)
    parent: Optional['Details'] = field(default=None, hash=True)

    def update(self, mapping: Dict['Details', 'Details'], parent: Union['Details', None]):
        """ Update the whole tree with a sentinel """
        assert isinstance(parent, Details) or parent is None, parent
        if self.parent is not None:
            if self.parent in mapping:
                parent = mapping[self.parent]
            else:
                parent = mapping[self.parent] = self.parent.update(mapping, parent)

        return Details(self.layer, parent)

    def __str__(self):
        result = f'{self.layer} ({hex(id(self))})'
        if self.parent is not None:
            result += ' -> ' + str(self.parent)
        return result


class TreeNode:
    __slots__ = 'name', '_edge', 'details'

    def __init__(self, name: str, edge: Optional[Tuple[Edge, Sequence['TreeNode']]],
                 details: Union[Details, None] = None):
        self.name, self._edge, self.details = name, edge, details

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
    def from_edges(cls, edges: Iterable['BoundEdge']) -> Dict['Node', 'TreeNode']:
        def update(node: Node):
            if node in mapping:
                return mapping[node]

            bridge = bridges.get(node)
            if bridge is not None:
                bridge = bridge.edge, tuple(map(update, bridge.inputs))
            mapping[node] = new = cls(node.name, bridge, node.details)
            return new

        nodes = set()
        bridges = {}
        # each edge is represented by its output
        for edge in edges:
            if edge.output in bridges:
                raise GraphError(f'The node "{edge.output.name}" has multiple incoming edges')

            bridges[edge.output] = edge
            nodes.add(edge.output)
            nodes.update(edge.inputs)

        mapping = {}
        for n in nodes:
            update(n)
        return mapping

    @staticmethod
    def to_edges(nodes: Iterable['TreeNode']) -> Sequence['BoundEdge']:
        def reverse(node) -> Node:
            if node not in _reversed:
                _reversed[node] = Node(node.name, node.details)
            return _reversed[node]

        def visit(node: 'TreeNode'):
            if node in visited or node.is_leaf:
                return
            visited.add(node)
            for parent in node.parents:
                visit(parent)

            result.append(BoundEdge(node.edge, list(map(reverse, node.parents)), reverse(node)))

        result = []
        visited = set()
        _reversed = {}
        for n in nodes:
            visit(n)

        return tuple(result)

    def __repr__(self):
        return f'<TreeNode: {self.name}>'


class Node:
    __slots__ = 'name', 'details'

    def __init__(self, name: str, details: Union[Details, None] = None):
        assert isinstance(details, Details) or details is None, details
        self.name = name
        self.details = details

    def clone(self):
        return type(self)(self.name, self.details)

    def __repr__(self):
        return f'<Node: {self.name}>'


class BoundEdge(NamedTuple):
    edge: Edge
    inputs: 'Nodes'
    output: Node


TreeNodes = Collection[TreeNode]
Nodes = Collection[Node]
NodeSet = Set[Node]
BoundEdges = Collection[BoundEdge]
Edges = Collection[Edge]


class HashError(RuntimeError):
    pass
