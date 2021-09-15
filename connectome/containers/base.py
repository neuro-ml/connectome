import logging
from abc import ABC, abstractmethod
from operator import itemgetter
from typing import Tuple, Optional, Union, Sequence

from ..engine.edges import FunctionEdge, ProductEdge
from ..engine.graph import Graph
from ..engine.base import TreeNode, BoundEdge, Node, Nodes, BoundEdges
from ..engine import Backend, DefaultBackend
from ..utils import node_to_dict

logger = logging.getLogger(__name__)


class Container:
    pass


class Wrapper(Container):
    def wrap(self, container: 'EdgesBag') -> 'EdgesBag':
        raise NotImplementedError


class Context(ABC):
    @abstractmethod
    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        pass

    @abstractmethod
    def update(self, mapping: dict) -> 'Context':
        pass


class NoContext(Context):
    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        raise ValueError('The layer is not reversible')

    def update(self, mapping: dict) -> 'Context':
        return self


class EdgesBag(Wrapper):
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, context: Optional[Context],
                 virtual_nodes: Union[bool, Sequence[str]] = (), persistent_nodes: Sequence[str] = ()):
        self.inputs = tuple(inputs)
        self.outputs = tuple(outputs)
        self.edges = tuple(edges)
        self.virtual_nodes = virtual_nodes
        self.persistent_nodes = persistent_nodes
        self.context = context if context is not None else NoContext()
        self.backend = DefaultBackend

    def freeze(self) -> 'EdgesBag':
        # TODO: layer inputs and outputs may not be among the edges
        node_map = {}
        edges_copy = []

        for edge in self.edges:
            inputs = update_map(edge.inputs, node_map)
            output = update_map([edge.output], node_map)[0]
            edges_copy.append(BoundEdge(edge.edge, inputs, output))

        return EdgesBag(
            update_map(self.inputs, node_map),
            update_map(self.outputs, node_map),
            edges_copy,
            self.context.update(node_map),
            virtual_nodes=self.virtual_nodes, persistent_nodes=self.persistent_nodes,
        )

    def compile(self) -> 'GraphContainer':
        return GraphContainer(self.inputs, self.outputs, self.edges, self.backend)

    def loopback(self, func, inputs, output):
        state = self.freeze()
        edges = list(state.edges)
        current = node_to_dict(state.outputs)

        # connect forward outputs with bridge
        outputs = []
        if isinstance(inputs, str):
            inputs = inputs,
        inputs = tuple(inputs)

        if len(set(inputs)) != len(inputs):
            raise ValueError(f'The inputs contain duplicates: {inputs}')

        inputs = [current[name] for name in inputs]
        edge = FunctionEdge(func, len(inputs))

        # single output
        if isinstance(output, str):
            output = Node(output)

            edges.append(edge.bind(inputs, output))
            outputs.append(output)

        # multiple outputs
        else:
            assert len(set(outputs)) == len(outputs)

            aux = Node('$aux')
            edges.append(edge.bind(inputs, aux))
            for idx, out in enumerate(output):
                out = Node(out)
                edges.append(FunctionEdge(itemgetter(idx), 1).bind(aux, out))
                outputs.append(out)

        outputs, edges = state.context.reverse(state.inputs, outputs, edges)
        return GraphContainer(state.inputs, outputs, edges, self.backend)


def update_map(nodes, node_map):
    for node in nodes:
        if node not in node_map:
            node_map[node] = Node(node.name)
    return [node_map[x] for x in nodes]


class GraphContainer:
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, backend: Backend):
        tree_node_map = TreeNode.from_edges(edges)
        self._edges = edges
        self.inputs = [tree_node_map[x] for x in inputs]
        self.outputs = node_to_dict(tree_node_map[x] for x in outputs)
        self.backend = backend
        self.methods = {node.name: self._compile(node) for node in self.outputs.values()}

    def __getitem__(self, item):
        if item not in self.methods:
            if not isinstance(item, tuple):
                raise AttributeError(item)

            outputs = []
            for name in item:
                if name not in self.outputs:
                    raise ValueError(f'"{name}" is not an available output: {tuple(self.outputs)}')
                outputs.append(self.outputs[name])

            product = TreeNode('$product', (ProductEdge(len(item)), outputs))
            self.methods[item] = self._compile(product)

        return self.methods[item]

    def _compile(self, node):
        return Graph(self.inputs, node, self.backend).call
