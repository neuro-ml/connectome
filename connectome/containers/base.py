import logging
import warnings
from operator import itemgetter
from typing import AbstractSet, Callable, Optional, Union

from ..engine import (
    BoundEdges, Details, FunctionEdge, GraphCompiler, IdentityEdge, Node, Nodes, NodeSet, ProductEdge, TreeNode
)
from ..engine.compiler import find_dependencies
from ..exceptions import GraphError
from ..utils import NameSet, StringsLike, check_for_duplicates, node_to_dict
from .context import ChainContext, Context, NoContext, update_map, BagContext

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
        layers_map = {}
        # the context must be updated first, because it shouldn't inherit the parent anyway
        context = self.context.update(node_map, layers_map)

        edges = []
        for edge in self.edges:
            inputs = update_map(edge.inputs, node_map, parent, layers_map)
            output, = update_map([edge.output], node_map, parent, layers_map)
            edges.append(edge.edge.bind(inputs, output))

        return EdgesBag(
            update_map(self.inputs, node_map, parent, layers_map),
            update_map(self.outputs, node_map, parent, layers_map),
            edges, context,
            virtual_nodes=self.virtual_nodes, persistent_nodes=self.persistent_nodes,
            optional_nodes=update_map(self.optional_nodes, node_map, parent, layers_map),
        )

    def compile(self) -> GraphCompiler:
        return GraphCompiler(
            self.inputs, self.outputs, self.edges, self.virtual_nodes, self.optional_nodes, self.backend
        )

    def loopback(self, func: Callable, inputs: StringsLike, output: StringsLike) -> 'EdgesBag':
        state = connect_bags(self, function_to_bag(func, inputs, output))
        outputs, new_edges, new_optionals = state.context.reverse(state.outputs)
        return EdgesBag(
            state.inputs, outputs, list(state.edges) + list(new_edges), None,
            virtual_nodes=None, persistent_nodes=None, optional_nodes=state.optional_nodes | new_optionals,
        )


def normalize_bag(inputs: Nodes, outputs: Nodes, edges: BoundEdges, virtuals: NameSet, optionals: NodeSet,
                  persistent_nodes: NameSet, allow_missing_inputs: bool = True):
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
    if not allow_missing_inputs:
        product_tree = mapping[product]
        missing = {node.name for node in find_dependencies([product_tree])[product_tree] - tree_inputs}
        if missing:
            raise GraphError(f'The nodes {missing} are missing from the inputs')

    # 5:
    missing_optionals = optionals - set(mapping)
    if missing_optionals:
        missing_optionals = {x.name for x in missing_optionals}
        raise GraphError(f'The nodes {missing_optionals} are marked as optional, but are not present in the graph')

    return tuple(inputs.values()), tuple(outputs.values()), tuple(edges), virtuals


def function_to_bag(func: Callable, inputs: StringsLike, output: StringsLike) -> EdgesBag:
    # the function is the parent of the nodes
    parent = Details(getattr(func, '__name__', str(func)))

    if isinstance(inputs, str):
        inputs = inputs,
    if len(set(inputs)) != len(inputs):
        raise ValueError(f'The inputs contain duplicates: {inputs}')
    inputs = [Node(name, parent) for name in inputs]

    edges, outputs = [], []
    edge = FunctionEdge(func, len(inputs))
    # single output
    if isinstance(output, str):
        output = Node(output, parent)
        outputs.append(output)
        edges.append(edge.bind(inputs, output))

    # multiple outputs
    else:
        if len(set(output)) != len(output):
            raise ValueError(f'The outputs contain duplicates: {outputs}')

        aux = Node('tuple', parent)
        edges.append(edge.bind(inputs, aux))
        for idx, out in enumerate(output):
            out = Node(out, parent)
            edges.append(FunctionEdge(itemgetter(idx), 1).bind(aux, out))
            outputs.append(out)

    return EdgesBag(
        inputs, outputs, edges, BagContext((), (), set(node_to_dict(outputs))),
        virtual_nodes=None, persistent_nodes=None, optional_nodes=None,
    )


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


def connect_bags(left: EdgesBag, right: EdgesBag) -> EdgesBag:
    left = left.freeze()
    right = right.freeze()

    inputs, outputs = set(left.inputs), set(right.outputs)
    edges = list(left.edges) + list(right.edges)
    optionals = left.optional_nodes | right.optional_nodes

    right_inputs = node_to_dict(right.inputs)
    left_outputs = node_to_dict(left.outputs)

    # common
    for name in set(left_outputs) & set(right_inputs):
        edges.append(IdentityEdge().bind(left_outputs[name], right_inputs[name]))

    # left virtuals
    for name in left.virtual_nodes & set(right_inputs):
        out = right_inputs[name]
        inp = out.clone()
        edges.append(IdentityEdge().bind(inp, out))
        inputs.add(inp)
        if out in optionals:
            optionals.add(inp)

    # right virtuals or persistent but unused
    for name in set(left_outputs) & (right.virtual_nodes | (left.persistent_nodes - {x.name for x in outputs})):
        inp = left_outputs[name]
        out = inp.clone()
        edges.append(IdentityEdge().bind(inp, out))
        outputs.add(out)
        if inp in optionals:
            optionals.add(out)

    check_for_duplicates(outputs)
    return EdgesBag(
        inputs, outputs, edges,
        ChainContext(left.context, right.context),
        virtual_nodes=left.virtual_nodes & right.virtual_nodes,
        optional_nodes=optionals,
        persistent_nodes=left.persistent_nodes | right.persistent_nodes,
    )
