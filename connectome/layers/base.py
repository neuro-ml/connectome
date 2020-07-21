from typing import Sequence, Callable, Tuple

from connectome.engine.edges import IdentityEdge, FunctionEdge
from ..engine.graph import compile_graph
from ..utils import check_for_duplicates, extract_signature, node_to_dict
from ..engine import TreeNode, Edge, BoundEdge, Node

Nodes = Sequence[Node]
Edges = Sequence[BoundEdge]


class Layer:
    pass


class Attachable(Layer):
    def attach(self, forward_outputs: Nodes, backward_inputs: Nodes) -> Tuple[Nodes, Nodes, Edges]:
        """
        Returns new forward and backward nodes, as well as additional edges.
        """
        edges, node_map = self.prepare()
        forward_outputs, new = self._attach_forward(forward_outputs, node_map)
        edges.extend(new)

        backward_inputs, new = self._attach_backward(backward_inputs, node_map)
        edges.extend(new)
        return forward_outputs, backward_inputs, edges

    def prepare(self) -> Tuple[list, dict]:
        return [], {}

    def _attach_forward(self, nodes: Nodes, node_map: dict) -> Tuple[Nodes, Edges]:
        raise NotImplementedError

    def _attach_backward(self, nodes: Nodes, node_map: dict) -> Tuple[Nodes, Edges]:
        raise NotImplementedError


class EdgesBag(Attachable):
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: Edges, backward_inputs: Nodes = None,
                 backward_outputs: Nodes = None):

        self.inputs = inputs
        self.outputs = outputs
        self.edges = edges
        self.backward_inputs = backward_inputs = backward_inputs or []
        self.backward_outputs = backward_outputs = backward_outputs or []

        node_to_tree_node = TreeNode.from_edges(edges)
        inputs = [node_to_tree_node[x] for x in inputs]
        outputs = [node_to_tree_node[x] for x in outputs]

        backward_inputs = [node_to_tree_node[x] for x in backward_inputs]
        backward_outputs = [node_to_tree_node[x] for x in backward_outputs]

        self._forward_methods = {}
        for node in outputs:
            self._forward_methods[node.name] = compile_graph(inputs, node)

        self._backward_methods = {}
        for node in backward_outputs:
            self._backward_methods[node.name] = compile_graph(backward_inputs + inputs, node)

    def get_forward_method(self, name):
        return self._forward_methods[name]

    def get_backward_method(self, name):
        return self._backward_methods[name]

    def prepare(self):
        """
        Prepares a copy of edges and nodes for connection.
        """
        node_map = {}

        # TODO: layer inputs and outputs may not be among the edges
        edges_copy = []
        for edge in self.edges:
            inputs = self.update_map(edge.inputs, node_map)
            output = self.update_map([edge.output], node_map)[0]
            edges_copy.append(BoundEdge(edge.edge, inputs, output))

        return edges_copy, node_map

    # TODO remove duplicated code
    def _attach_forward(self, forwards: Nodes, node_map: dict) -> Tuple[Nodes, Edges]:
        check_for_duplicates([x.name for x in forwards])

        new_edges = []
        inputs = self.update_map(self.inputs, node_map)
        outputs = self.update_map(self.outputs, node_map)

        forwards = node_to_dict(forwards)
        for i in inputs:
            new_edges.append(BoundEdge(IdentityEdge(), [forwards[i.name]], i))
        return outputs, new_edges

    # TODO remove duplicated code
    def _attach_backward(self, backwards: Nodes, node_map: dict) -> Tuple[Nodes, Edges]:
        check_for_duplicates([x.name for x in backwards])

        new_edges = []
        backward_inputs = self.update_map(self.backward_inputs, node_map)
        backward_outputs = self.update_map(self.backward_outputs, node_map)

        backwards = node_to_dict(backwards)
        for o in backward_outputs:
            new_edges.append(BoundEdge(IdentityEdge(), [o], backwards[o.name]))
        return backward_inputs, new_edges

    def get_loopback(self, function: Callable, backward_input_name: str):
        """
        Creates a graph by closing forward outputs and backward input using the given function.
        """

        attr_names = extract_signature(function)

        forward_outputs = node_to_dict(self.outputs)
        backward_inputs = node_to_dict(self.backward_inputs)
        backward_outputs = node_to_dict(self.backward_outputs)

        required_outputs = {}
        for name in attr_names:
            assert name in forward_outputs
            required_outputs[name] = forward_outputs[name]

        assert backward_input_name in backward_inputs
        loopback_output = backward_inputs[backward_input_name]
        loopback_inputs = list(required_outputs.values())

        loopback_edge = BoundEdge(
            FunctionEdge(function, len(attr_names)),
            loopback_inputs, loopback_output
        )

        edges = list(self.edges)
        edges.append(loopback_edge)

        mapping = TreeNode.from_edges(edges)
        graph_inputs = [mapping[x] for x in self.inputs]
        graph_output = mapping[backward_outputs[backward_input_name]]
        return compile_graph(graph_inputs, graph_output)

    @staticmethod
    def update_map(nodes, node_map):
        for node in nodes:
            if node not in node_map:
                node_map[node] = Node(node.name)
        return [node_map[x] for x in nodes]


class _Layer:
    def get_forward_params(self, other_outputs: Sequence[Node]):
        raise NotImplementedError

    def get_backward_params(self, other_backwards: Sequence[Node]):
        raise NotImplementedError

    def get_all_forward_methods(self):
        raise NotImplementedError

    def get_backward_inputs(self):
        raise NotImplementedError

    def get_backward_outputs(self):
        raise NotImplementedError

    def get_inputs(self):
        raise NotImplementedError

    def get_outputs(self):
        raise NotImplementedError

    def get_edges(self):
        raise NotImplementedError


# TODO come up with a good name
class NodeInterface:
    """
    Keeps forward and backward methods.
    """

    def __init__(self, name, forward_method, backward_method=None):
        # self.edges = None
        self.name = name

        self._forward_method = forward_method
        self._backward_method = backward_method

    @property
    def forward(self):
        return self._forward_method

    @property
    def backward(self):
        return self._backward_method


class FreeLayer(Layer):
    def __init__(self):
        self._edges = []
        self._inputs = []
        self._outputs = []
        self._backward_inputs = []
        self._backward_outputs = []

        self.graph = Graph()
        self._forward_methods = {}
        self._backward_methods = {}

    def create_methods(self, outputs, inputs, edges):
        methods = {}
        for output in outputs:
            methods[output.name] = self.graph.compile_graph([output], inputs, edges)
        return methods

    # just for now
    def get_backward_params(self, other_backwards: Sequence[TreeNode]):
        check_for_duplicates([x.name for x in other_backwards])

        other_backwards = node_to_dict(other_backwards)
        new_edges = []

        for i in self._backward_inputs:
            new_edges.append(IdentityEdge(other_backwards[i.name], i))
        return self._backward_outputs, new_edges

    def set_graph_forwards_from_layer(self, layer: Layer):
        self._edges = list(layer.get_edges())
        self._inputs = list(layer.get_inputs())
        self._outputs = list(layer.get_outputs())
        self._forward_methods = self.create_methods(self._outputs, self._inputs, self._edges)

    def set_graph_backwards_from_layer(self, layer: Layer):
        self._backward_inputs = list(layer.get_backward_inputs())
        self._backward_outputs = list(layer.get_backward_outputs())

        self.check_backwards()
        self._backward_methods = self.create_methods(self._backward_outputs, self._backward_inputs, self._edges)

    def check_backwards(self):
        backward_inputs_dict = node_to_dict(self._backward_inputs)
        backward_outputs_dict = node_to_dict(self._backward_outputs)
        outputs_dict = node_to_dict(self._outputs)

        for name in backward_outputs_dict.keys():
            assert name in backward_inputs_dict
            assert name in outputs_dict

    def get_forward_method(self, name):
        return self._forward_methods[name]

    def get_all_forward_methods(self):
        return self._forward_methods

    def get_backward_method(self, name):
        return self._backward_methods[name]

    def get_all_backward_methods(self):
        return self._backward_methods

    def get_node_interface(self, name):
        # TODO replace by exception
        # TODO refactor
        assert name in self._forward_methods

        if name in self._backward_methods:
            return NodeInterface(name, self._forward_methods[name], self._backward_methods[name])

        return NodeInterface(name, self._forward_methods[name])

    # TODO replace by properties
    def get_backward_inputs(self):
        return self._backward_inputs

    def get_backward_outputs(self):
        return self._backward_outputs

    def get_inputs(self):
        return self._inputs

    def get_outputs(self):
        return self._outputs

    def get_edges(self):
        return self._edges

    def get_forward_params(self, other_outputs: Sequence[TreeNode]):
        raise NotImplementedError


class AttachableLayer(Layer):
    # TODO just for now
    def get_backward_params(self, other_backwards: Sequence[TreeNode]):
        return other_backwards, []

    def get_forward_params(self, *args, **kwargs):
        raise NotImplementedError

    def get_all_forward_methods(self):
        return {}

    def get_backward_inputs(self):
        return []

    def get_backward_outputs(self):
        return []

    def get_inputs(self):
        raise AttributeError

    def get_outputs(self):
        raise AttributeError

    def get_edges(self):
        raise AttributeError
