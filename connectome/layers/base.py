from typing import Sequence

from connectome.engine.edges import IdentityEdge
from ..utils import check_for_duplicates, node_to_dict
from ..engine import TreeNode, Edge, BoundEdge, Node


class Layer:
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


class EdgesBag(Layer):
    def __init__(self, inputs: Sequence[Node], outputs: Sequence[Node], edges: Sequence[BoundEdge]):
        # backward_inputs: Sequence[Node], backward_outputs: Sequence[Node]):
        self.inputs = inputs
        self.outputs = outputs
        self.edges = edges
        # self.backward_inputs = backward_inputs
        # self.backward_outputs = backward_outputs


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


class CustomLayer(FreeLayer):
    def __init__(self, inputs: Sequence[TreeNode], outputs: Sequence[TreeNode], edges: Sequence[Edge],
                 backward_inputs: Sequence[TreeNode] = None, backward_outputs: Sequence[TreeNode] = None):
        super().__init__()

        self._edges = edges
        self._inputs = inputs
        self._outputs = outputs
        self._forward_methods = self.create_methods(self._outputs, self._inputs, self._edges)

        if backward_inputs is None:
            self._backward_inputs = []
        else:
            self._backward_inputs = backward_inputs

        if backward_outputs is None:
            self._backwards_outputs = []
        else:
            self._backward_outputs = backward_outputs

        self.check_backwards()
        self._backward_methods = self.create_methods(self._backward_outputs, self._backward_inputs, self._edges)

    def get_forward_params(self, other_outputs: Sequence[TreeNode]):
        check_for_duplicates([x.name for x in other_outputs])
        outputs = node_to_dict(other_outputs)

        forward_edges = []
        for i in self._inputs:
            forward_edges.append(IdentityEdge(outputs[i.name], i))

        forward_edges.extend(self._edges)
        return self._outputs, forward_edges
