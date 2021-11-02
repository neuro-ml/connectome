import logging
from abc import ABC, abstractmethod
from operator import itemgetter
from typing import Tuple, Optional, Set

from ..engine.edges import FunctionEdge, ProductEdge, IdentityEdge
from ..engine.graph import Graph
from ..engine.base import TreeNode, BoundEdge, Node, Nodes, BoundEdges
from ..engine import Backend, DefaultBackend
from ..exceptions import GraphError
from ..utils import node_to_dict

logger = logging.getLogger(__name__)

NameSet = Set[str]


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
                 virtual_nodes: NameSet = None, persistent_nodes: NameSet = None):
        if virtual_nodes is None:
            virtual_nodes = set()
        if persistent_nodes is None:
            persistent_nodes = set()
        if context is None:
            context = NoContext()

        self.inputs, self.outputs, self.edges, self.virtual_nodes = normalize_bag(
            inputs, outputs, edges, virtual_nodes, persistent_nodes)

        self.persistent_nodes = persistent_nodes
        self.context = context
        self.backend = DefaultBackend

    def freeze(self) -> 'EdgesBag':
        # TODO: layer inputs and outputs may not be among the edges
        node_map = {}
        edges_copy = []

        for edge in self.edges:
            inputs = update_map(edge.inputs, node_map)
            output, = update_map([edge.output], node_map)
            edges_copy.append(BoundEdge(edge.edge, inputs, output))

        return EdgesBag(
            update_map(self.inputs, node_map),
            update_map(self.outputs, node_map),
            edges_copy,
            self.context.update(node_map),
            virtual_nodes=self.virtual_nodes, persistent_nodes=self.persistent_nodes
        )

    def compile(self) -> 'GraphContainer':
        return GraphContainer(self.inputs, self.outputs, self.edges, self.virtual_nodes, self.backend)

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

        input_nodes, all_inputs = [], list(state.inputs)
        for name in inputs:
            if name not in current:
                if name not in state.virtual_nodes:
                    raise GraphError(f'Node "{name}" is not defined')
                node = Node(name)
                all_inputs.append(node)
            else:
                node = current[name]

            input_nodes.append(node)

        edge = FunctionEdge(func, len(input_nodes))

        # single output
        if isinstance(output, str):
            output = Node(output)

            edges.append(edge.bind(input_nodes, output))
            outputs.append(output)

        # multiple outputs
        else:
            assert len(set(outputs)) == len(outputs)

            aux = Node('$aux')
            edges.append(edge.bind(input_nodes, aux))
            for idx, out in enumerate(output):
                out = Node(out)
                edges.append(FunctionEdge(itemgetter(idx), 1).bind(aux, out))
                outputs.append(out)

        outputs, edges = state.context.reverse(all_inputs, outputs, edges)
        return GraphContainer(all_inputs, outputs, edges, set(), self.backend)


def update_map(nodes, node_map):
    for node in nodes:
        if node not in node_map:
            node_map[node] = Node(node.name)
    return [node_map[x] for x in nodes]


def identity(x):
    return x


class GraphContainer:
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, virtuals: NameSet, backend: Backend):
        tree_node_map = TreeNode.from_edges(edges)
        self._edges = edges
        self.inputs = [tree_node_map[x] for x in inputs]
        self.outputs = node_to_dict(tree_node_map[x] for x in outputs)
        self.backend = backend
        self.virtuals = virtuals
        self.methods = {node.name: self._compile(node) for node in self.outputs.values()}

    def __getitem__(self, item):
        if item not in self.methods:
            if isinstance(item, str) and item in self.virtuals:
                # TODO: signature
                value = identity

            elif isinstance(item, tuple):
                outputs = []
                for name in item:
                    if name not in self.outputs:
                        raise ValueError(f'"{name}" is not an available output: {tuple(self.outputs)}')
                    outputs.append(self.outputs[name])

                product = TreeNode('$product', (ProductEdge(len(item)), outputs))
                value = self._compile(product)

            else:
                raise AttributeError(item)

            self.methods[item] = value

        return self.methods[item]

    def _compile(self, node):
        return Graph(self.inputs, node, self.backend).call


def get_parents(node: TreeNode):
    if node.is_leaf:
        return

    for parent in node.parents:
        yield from get_parents(parent)


def normalize_bag(inputs: Nodes, outputs: Nodes, edges: BoundEdges, virtual_nodes: Set[str], persistent_nodes):
    # 1. outputs must only depend on inputs
    # 1a. inputs must have no dependencies
    # 2. each node can only have a single incoming edge
    # 2a. the intersection between outputs and virtual nodes must be empty
    # 3. virtual edges with a present input node become non-virtual
    inputs, outputs = node_to_dict(inputs), node_to_dict(outputs)
    edges = list(edges)

    # 2a:
    intersection = virtual_nodes & set(outputs)
    if intersection:
        raise GraphError(f'The nodes {intersection} are both inherited and have defined edges')

    # 3:
    add = (virtual_nodes | persistent_nodes) & set(inputs) - set(outputs)
    virtual_nodes = virtual_nodes - add
    for name in add:
        outputs[name] = Node(name)
        edges.append(IdentityEdge().bind(inputs[name], outputs[name]))

    # 2:
    product = Node('$product')
    # this call already has the check
    mapping = TreeNode.from_edges(edges + [ProductEdge(len(outputs)).bind(tuple(outputs.values()), product)])
    # 1a:
    tree_inputs = {mapping[node] for node in inputs.values()}
    not_leaves = {node.output.name for node in tree_inputs if not node.is_leaf}
    if not_leaves:
        raise GraphError(f'The inputs {not_leaves} are not actual inputs - they have dependencies')
    # 1:
    missing = {node.output.name for node in set(get_parents(mapping[product])) - tree_inputs}
    if missing:
        raise GraphError(f'The nodes {missing} are missing from the inputs')

    return tuple(inputs.values()), tuple(outputs.values()), tuple(edges), virtual_nodes
