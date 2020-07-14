from pathlib import Path
from typing import Sequence, Callable

from .cache import DiskStorage, MemoryStorage
from .edges import CacheEdge, IdentityEdge, MuxEdge
from .utils import count_duplicates, check_for_duplicates, node_to_dict
from .engine import Graph, Layer, Node, Edge


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
    def get_backward_params(self, other_backwards: Sequence[Node]):
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

    def get_forward_params(self, other_outputs: Sequence[Node]):
        raise NotImplementedError


class AttachableLayer(Layer):
    # TODO just for now
    def get_backward_params(self, other_backwards: Sequence[Node]):
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


class MemoryCacheLayer(AttachableLayer):
    def get_forward_params(self, other_outputs: Sequence[Node]):
        this_outputs = [Node(o.name) for o in other_outputs]
        edges = [
            CacheEdge(other_output, this_output, storage=MemoryStorage())
            for other_output, this_output in zip(other_outputs, this_outputs)
        ]
        return this_outputs, edges


class DiskCacheLayer(AttachableLayer):
    def __init__(self, storage):
        self.path = Path(storage)
        # TODO: pass a list of names

    def get_forward_params(self, other_outputs: Sequence[Node]):
        # TODO: make sure that the names are unique
        # TODO: use the same storage for all?
        this_outputs = [Node(o.name) for o in other_outputs]
        edges = [
            CacheEdge(other_output, this_output, storage=DiskStorage(self.path))
            for other_output, this_output in zip(other_outputs, this_outputs)
        ]
        return this_outputs, edges


class PipelineLayer(FreeLayer):
    def __init__(self, *layers: Layer):
        assert len(layers) > 0
        super().__init__()

        self.set_graph_forwards_from_layer(layers[0])
        self.create_forward_connections(layers)

        self.set_graph_backwards_from_layer(layers[-1])
        self.create_backward_connections(layers[:-1])
        self.layers = layers

    def create_forward_connections(self, layers):
        for layer in layers[1:]:
            self._outputs, new_edges = layer.get_forward_params(self._outputs)

            for e in new_edges:
                assert e not in self._edges
                self._edges.append(e)

        self._forward_methods = self.create_methods(self._outputs, self._inputs, self._edges)

    def create_backward_connections(self, new_layers):
        for layer in reversed(new_layers):
            self._backward_outputs, new_edges = layer.get_backward_params(self._backward_outputs)

            for e in new_edges:
                assert e not in self._edges
                self._edges.append(e)

        self.check_backwards()
        self._backward_methods = self.create_methods(self._backward_outputs, self._backward_inputs, self._edges)

    def get_forward_params(self, outputs: Sequence[Node]):
        all_edges = []
        for layer in self.layers:
            outputs, edges = layer.get_forward_params(outputs)
            all_edges.extend(edges)

        return outputs, all_edges

    # TODO add operators?
    def index(self, index):
        return self.slice(index, index + 1)

    def slice(self, start, stop):
        assert start >= 0, start > stop

        if issubclass(type(self.layers[start]), FreeLayer):
            return PipelineLayer(*self.layers[start:stop])
        else:
            raise IndexError('First layer must be a Free layer')


class CustomLayer(FreeLayer):
    def __init__(self, inputs: Sequence[Node], outputs: Sequence[Node], edges: Sequence[Edge],
                 backward_inputs: Sequence[Node] = None, backward_outputs: Sequence[Node] = None):
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

    def get_forward_params(self, other_outputs: Sequence[Node]):
        check_for_duplicates([x.name for x in other_outputs])
        outputs = node_to_dict(other_outputs)

        forward_edges = []
        for i in self._inputs:
            forward_edges.append(IdentityEdge(outputs[i.name], i))

        forward_edges.extend(self._edges)
        return self._outputs, forward_edges


# TODO add backwards
class MuxLayer(FreeLayer):
    def __init__(self, branch_selector: Callable, *layers: FreeLayer):
        super().__init__()

        self._outputs, self._inputs, self._edges = self.create_graph(branch_selector, layers)
        self._forward_methods = self.create_methods(self._outputs, self._inputs, self._edges)

    def get_forward_params(self, *args, **kwargs):
        raise RuntimeError("Mux layer can't be attached")

    def create_graph(self, branch_selector: Callable, layers: Sequence[FreeLayer]):
        inputs = []
        shared_output_names = set()

        for layer in layers:
            inputs.extend(layer.get_inputs())
            # check for outputs with the same name
            output_names = [o.name for o in layer.get_outputs()]
            assert any(v < 2 for _, v in count_duplicates(output_names).items())
            shared_output_names.update(output_names)

        layers_outputs_map = []
        layers_input_names = []

        for layer in layers:
            cur_outputs_map = {o.name: o for o in layer.get_outputs() if o.name in shared_output_names}
            cur_essential_inputs_map, _, _ = self.graph.get_graph_structure(cur_outputs_map.values(),
                                                                            layer.get_inputs(), layer.get_edges())
            # check for essential inputs with the same name
            assert any(v < 2 for _, v in count_duplicates(cur_essential_inputs_map.keys()).items())

            layers_outputs_map.append(cur_outputs_map)
            layers_input_names.append(cur_essential_inputs_map.keys())

        # all layers must contain the same set of input nodes
        input_names_union = set.union(*[set(names) for names in layers_input_names])
        input_names_intersection = set.intersection(*[set(names) for names in layers_input_names])
        assert input_names_intersection == input_names_union

        outputs = []
        mux_edges = []
        for name in shared_output_names:
            output_node = Node(name)
            edge = MuxEdge(branch_selector, [layer_map[name] for layer_map in layers_outputs_map], output_node)
            mux_edges.append(edge)
            outputs.append(output_node)

        all_edges = []
        for layer in layers:
            all_edges.extend(layer.get_edges())

        all_edges.extend(mux_edges)
        return outputs, inputs, all_edges
