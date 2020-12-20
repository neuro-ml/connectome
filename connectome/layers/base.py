from operator import itemgetter
from typing import Tuple

from ..engine.edges import FunctionEdge
from ..engine.graph import compile_graph
from ..engine.base import TreeNode, BoundEdge, Node, Nodes, BoundEdges
from ..utils import node_to_dict


class Layer:
    pass


class Wrapper(Layer):
    def wrap(self, layer: 'EdgesBag') -> 'EdgesBag':
        raise NotImplementedError


class Context:
    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        raise NotImplementedError

    def update(self, mapping: dict) -> 'Context':
        raise NotImplementedError


class NoContext(Context):
    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        raise ValueError('The layer is not reversible')

    def update(self, mapping: dict) -> 'Context':
        pass


class EdgesBag(Wrapper):
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, context: Context = None):
        self.inputs = tuple(inputs)
        self.outputs = tuple(outputs)
        self.edges = tuple(edges)
        self.context = context if context is not None else NoContext()

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
            self.context.update(node_map)
        )

    def compile(self):
        return bake_methods(self.inputs, self.outputs, self.edges)

    def loopback(self, bridges):
        if self.context is None:
            raise ValueError

        # TODO: freeze
        edges = list(self.edges)
        current = node_to_dict(self.outputs)

        # TODO: check uniqueness
        # connect forward outputs with bridges
        outputs = []
        for func, inputs, output in bridges:
            if isinstance(inputs, str):
                inputs = [inputs]
            inputs = [current[name] for name in inputs]
            edge = FunctionEdge(func, len(inputs))

            # single output
            if isinstance(output, str):
                output = Node(output)

                edges.append(edge.bind(inputs, output))
                outputs.append(output)

            # multiple outputs
            else:
                aux = Node('$aux')
                edges.append(edge.bind(inputs, aux))
                for idx, out in enumerate(output):
                    out = Node(out)
                    edges.append(FunctionEdge(itemgetter(idx), 1).bind(aux, out))
                    outputs.append(out)

        outputs, edges = self.context.reverse(self.inputs, outputs, edges)
        return bake_methods(self.inputs, outputs, edges)


def update_map(nodes, node_map):
    for node in nodes:
        if node not in node_map:
            node_map[node] = Node(node.name)
    return [node_map[x] for x in nodes]


def bake_methods(inputs: Nodes, outputs: Nodes, edges: BoundEdges):
    tree_node_map = TreeNode.from_edges(edges)
    inputs = [tree_node_map[x] for x in inputs]
    outputs = [tree_node_map[x] for x in outputs]
    return {node.name: compile_graph(inputs, node) for node in outputs}
