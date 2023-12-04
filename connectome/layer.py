from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Optional

from connectome.containers import Context, NoContext, ChainContext
from connectome.containers.base import function_to_bag
from connectome.containers.context import update_map
# from connectome.containers import EdgesBag
from connectome.engine import Nodes, BoundEdges, FunctionEdge, GraphCompiler, IdentityEdge
from connectome.engine.base import Edges, Node, NodeSet, TreeNode
from connectome.utils import StringsLike, NameSet, node_to_dict

Slice = tuple[Nodes, Nodes, BoundEdges, Context]


class Layer(ABC):
    def _compile(self, names):
        _names = names
        if isinstance(names, str):
            _names = names,
        if isinstance(names, list):
            names = tuple(names)
        inputs, outputs, edges, _ = self._slice(_names)
        # TODO: remove this class
        return GraphCompiler(inputs, outputs, edges, set(), []).compile(names)

    def _loopback(self, func: Callable, inputs: StringsLike, outputs: StringsLike) -> Layer:
        # current layer
        inps, outs, edges, context = self._slice(inputs)
        # the function is a new layer
        bag = function_to_bag(func, inputs, outputs)
        # use the context to add edges
        final_outs, new_edges, _ = context.reverse(bag.outputs)
        # combine all the edges
        edges = [*edges, *bag.edges, *new_edges, *build_bridge(outs, bag.inputs)]
        return EdgeContainer(inps, final_outs, edges, None)

    def _connect(self, previous: Layer) -> Layer:
        return Link(previous, self)

    @abstractmethod
    def _slice(self, names) -> Slice:
        pass


class Link(Layer):
    def __init__(self, left: Layer, right: Layer):
        self._left = left
        self._right = right

    def _slice(self, names):
        r_inputs, r_outputs, r_edges, r_context = freeze(*self._right._slice(names))
        l_inputs, l_outputs, l_edges, l_context = freeze(*self._left._slice([x.name for x in r_inputs]))
        r_edges = [*r_edges, *l_edges, *build_bridge(l_outputs, r_inputs)]
        return l_inputs, r_outputs, r_edges, ChainContext(l_context, r_context)


def build_bridge(left: Nodes, right: Nodes) -> BoundEdges:
    left, right = node_to_dict(left), node_to_dict(right)
    assert set(left) == set(right)
    return [IdentityEdge().bind(left[x], right[x]) for x in left]


def freeze(inputs_, outputs_, edges_, context) -> Slice:
    node_map = {}
    layers_map = {}
    # the context must be updated first, because it shouldn't inherit the parent anyway
    context = context.update(node_map, layers_map)

    edges = []
    parent = None
    for edge in edges_:
        inputs = update_map(edge.inputs, node_map, parent, layers_map)
        output, = update_map([edge.output], node_map, parent, layers_map)
        edges.append(edge.edge.bind(inputs, output))

    return (
        update_map(inputs_, node_map, parent, layers_map),
        update_map(outputs_, node_map, parent, layers_map),
        edges, context,
    )


class EdgeContainer(Layer):
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, context: Optional[Context], *,
                 virtual: Optional[NameSet] = None, persistent: Optional[NameSet] = None,
                 optional: Optional[NodeSet] = None):
        if virtual is None:
            virtual = set()
        if persistent is None:
            persistent = set()
        if optional is None:
            optional = set()
        if context is None:
            context = NoContext()

        self._inputs = inputs
        self._outputs = node_to_dict(outputs)
        self._edges = edges
        self._context = context
        self._mapping = TreeNode.from_edges(self._edges)
        self._reverse = dict(zip(self._mapping.values(), self._mapping))

        # self.inputs, self.outputs, self.edges, self.virtual = normalize_bag(
        #     inputs, outputs, edges, virtual, optional, persistent)

        # self.persistent: NameSet = persistent
        # self.optional: NodeSet = optional

    def _slice(self, names) -> Slice:
        def visit(node: TreeNode):
            if node.is_leaf:
                inputs.add(self._reverse[node])
            else:
                for parent in node.parents:
                    visit(parent)

        inputs, outputs = set(), []
        for name in names:
            # TODO: virtuals
            output = self._outputs[name]
            outputs.append(output)
            visit(self._mapping[output])

        for inp in inputs:
            assert inp in self._inputs

        # TODO: make copies
        return inputs, outputs, self._edges, self._context


class Chain(Layer):
    def __init__(self, *layers: Layer):
        head, *tail = layers
        for layer in tail:
            head = layer._connect(head)

        self._connected = head
        self._layers = layers

    def _slice(self, names) -> Slice:
        return self._connected._slice(names)


class Album(Layer):
    def _slice(self, names) -> Slice:
        inputs = outputs = f(names)
        edges = [AlbumEdge(pipeline, names), *getitem]
        return inputs, outputs, edges, NoContext()
