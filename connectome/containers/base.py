import logging
import warnings
from operator import itemgetter
from typing import Optional, AbstractSet, Union

from ..engine import (
    GraphCompiler, TreeNode, Node, Nodes, BoundEdges, NodeSet, FunctionEdge, ProductEdge, IdentityEdge, Details
)
from ..exceptions import GraphError
from ..utils import node_to_dict, NameSet
from .context import Context, NoContext, update_map

__all__ = 'Container', 'EdgesBag'

logger = logging.getLogger(__name__)


class Container:
    def __init__(self):
        warnings.warn(
            'The container interface is deprecated and will be merged with `EdgesBag` soon',
            UserWarning
        )
        warnings.warn(
            'The container interface is deprecated and will be merged with `EdgesBag` soon',
            DeprecationWarning
        )

    def wrap(self, container: 'EdgesBag') -> 'EdgesBag':
        raise NotImplementedError


class EdgesBag:
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, context: Optional[Context], *,
                 virtual_nodes: Optional[NameSet] = None, persistent_nodes: Optional[NameSet],
                 optional_nodes: Optional[NodeSet] = None):
        if virtual_nodes is None:
            virtual_nodes = set()
        if persistent_nodes is None:
            persistent_nodes = set()
        if optional_nodes is None:
            optional_nodes = set()
        if not isinstance(optional_nodes, AbstractSet):
            optional_nodes = set(optional_nodes)
        if context is None:
            context = NoContext()

        self.inputs, self.outputs, self.edges, self.virtual_nodes = normalize_bag(
            inputs, outputs, edges, virtual_nodes, optional_nodes, persistent_nodes)

        self.persistent_nodes: NameSet = persistent_nodes
        self.optional_nodes: NodeSet = optional_nodes
        self.context = context
        self.backend = None

    def freeze(self, parent: Union[Details, None] = None) -> 'EdgesBag':
        """
        Creates a copy of the nodes and edges.
        If `parent` is not None, increases the nesting of the layers hierarchy by assigning
        `parent` to top layers and nodes
        """
        node_map = {}
        layers_map = {} if parent is not None else None

        edges = []
        for edge in self.edges:
            inputs = update_map(edge.inputs, node_map, parent, layers_map)
            output, = update_map([edge.output], node_map, parent, layers_map)
            edges.append(edge.edge.bind(inputs, output))

        return EdgesBag(
            update_map(self.inputs, node_map, parent, layers_map),
            update_map(self.outputs, node_map, parent, layers_map),
            edges,
            # TODO: should the context also update the nesting?
            self.context.update(node_map),
            virtual_nodes=self.virtual_nodes, persistent_nodes=self.persistent_nodes,
            optional_nodes=update_map(self.optional_nodes, node_map, parent, layers_map),
        )

    def compile(self) -> GraphCompiler:
        return GraphCompiler(
            self.inputs, self.outputs, self.edges, self.virtual_nodes, self.optional_nodes, self.backend
        )

    # TODO: this should return a container without compilation
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
        # the function is the parent of these new nodes
        parent = Details(func)
        for name in inputs:
            if name not in current:
                if name not in state.virtual_nodes:
                    raise GraphError(f'Node "{name}" is not defined')
                node = Node(name, parent)
                all_inputs.append(node)
            else:
                node = current[name]

            input_nodes.append(node)

        edge = FunctionEdge(func, len(input_nodes))

        # single output
        if isinstance(output, str):
            output = Node(output, parent)

            edges.append(edge.bind(input_nodes, output))
            outputs.append(output)

        # multiple outputs
        else:
            assert len(set(outputs)) == len(outputs)

            aux = Node('tuple', parent)
            edges.append(edge.bind(input_nodes, aux))
            for idx, out in enumerate(output):
                out = Node(out, parent)
                edges.append(FunctionEdge(itemgetter(idx), 1).bind(aux, out))
                outputs.append(out)

        outputs, edges = state.context.reverse(all_inputs, outputs, edges)
        return GraphCompiler(all_inputs, outputs, edges, set(), self.optional_nodes, self.backend)


def normalize_bag(inputs: Nodes, outputs: Nodes, edges: BoundEdges, virtuals: NameSet, optionals: NodeSet,
                  persistent_nodes: NameSet):
    # 1. outputs must only depend on inputs
    # 1a. inputs must have no dependencies
    # 2. each node can only have a single incoming edge
    # 2a. the intersection between outputs and virtual nodes must be empty
    # 3. virtual edges with a present input node become non-virtual
    # 4. the graph must be acyclic
    # 5. all the optional nodes must be present among the edges' inputs/outputs
    inputs, outputs = node_to_dict(inputs), node_to_dict(outputs)
    edges = list(edges)

    # 2a:
    intersection = virtuals & set(outputs)
    if intersection:
        raise GraphError(f'The nodes {intersection} are both inherited and have defined edges')

    # 3:
    add = (virtuals | persistent_nodes) & set(inputs) - set(outputs)
    virtuals = virtuals - add
    for name in add:
        outputs[name] = inputs[name].clone()
        edges.append(IdentityEdge().bind(inputs[name], outputs[name]))

    # 2:
    adjacency = {}
    for edge in edges:
        if edge.output in adjacency:
            raise GraphError(f'The node "{edge.output.name}" has multiple incoming edges')
        adjacency[edge.output] = edge.inputs
    # 4:
    cycles = detect_cycles(adjacency)
    if cycles:
        raise GraphError(
            'The computational graph contains cycles:\n  ' +
            '\n  '.join(' -> '.join(node.name for node in nodes) for nodes in cycles)
        )

    # 1a:
    product = Node('$product', None)
    mapping = TreeNode.from_edges(edges + [ProductEdge(len(outputs)).bind(tuple(outputs.values()), product)])
    tree_inputs = {mapping[node] for node in inputs.values()}
    not_leaves = {node.output.name for node in tree_inputs if not node.is_leaf}
    if not_leaves:
        raise GraphError(f'The inputs {not_leaves} are not actual inputs - they have dependencies')
    # 1:
    # TODO:
    # missing = {node.name for node in set(get_parents(mapping[product])) - tree_inputs}
    # if missing:
    #     raise GraphError(f'The nodes {missing} are missing from the inputs')

    # 5:
    missing_optionals = optionals - set(mapping)
    if missing_optionals:
        missing_optionals = {x.name for x in missing_optionals}
        raise GraphError(f'The nodes {missing_optionals} are marked as optional, but are not present in the graph')

    return tuple(inputs.values()), tuple(outputs.values()), tuple(edges), virtuals


def detect_cycles(adjacency):
    def visit(node):
        if node in visited:
            # the node was not completely visited
            if not visited[node]:
                cycles.append(tuple(stack) + (node,))

            return

        visited[node] = False
        stack.append(node)

        for parent in adjacency.get(node, ()):
            visit(parent)

        stack.pop()
        visited[node] = True

    stack, visited, cycles = [], {}, []
    for n in adjacency:
        visit(n)

    return cycles
