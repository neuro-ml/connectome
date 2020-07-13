from pathlib import Path
from typing import Sequence, Callable

from .cache import DiskStorage, MemoryStorage
from .edges import CacheEdge, IdentityEdge, MuxEdge
from .utils import count_duplicates, node_to_dict
from .engine import Graph, Layer, Node, Edge


class FreeLayer(Layer):
    def __init__(self, use_backward):
        self.use_backward = use_backward

        self._edges = []
        self._inputs = []
        self._outputs = []
        self._backwards = []

        self.graph = Graph()
        self._forward_methods = {}

    # TODO: do we need this?
    # TODO: We have a test where it is used)
    def __call__(self, *args, **kwargs):
        if len(self._inputs) == 0:
            raise RuntimeError('Layer must contain at least 1 input node')

        caller = self.graph.compile_graph(self._outputs, self._inputs, self._edges)
        return caller(*args, **kwargs)

    def create_methods(self, outputs, inputs, edges):
        methods = {}
        for output in outputs:
            methods[output.name] = self.graph.compile_graph([output], inputs, edges)
        return methods

    def get_backward_params(self, other_backwards: Sequence[Node]):
        self.check_for_duplicates([x.name for x in other_backwards])

        other_backwards = node_to_dict(other_backwards)
        new_edges = []

        for i in self._backwards:
            new_edges.append(IdentityEdge(other_backwards[i.name], i))
        return self._backwards, new_edges

    def get_method(self, name):
        return self._forward_methods[name]

    def get_all_methods(self):
        return self._forward_methods

    def get_backwards(self):
        return self._backwards

    def get_inputs(self):
        return self._inputs

    def get_outputs(self):
        return self._outputs

    def get_edges(self):
        return self._edges

    def get_forward_params(self, other_outputs: Sequence[Node]):
        raise NotImplementedError

    # TODO add error message
    @staticmethod
    def check_for_duplicates(collection):
        counts: dict = count_duplicates([x for x in collection])
        assert not any(v > 1 for k, v in counts.items())


class AttachableLayer(Layer):
    def get_forward_params(self, *args, **kwargs):
        raise NotImplementedError

    def get_all_methods(self):
        return {}

    def get_backwards(self):
        raise AttributeError

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
    def __init__(self, *layers: Layer, use_backward=False):
        assert len(layers) > 0
        super().__init__(use_backward)

        first_layer = layers[0]
        self.layers = [first_layer]

        self._edges = list(first_layer.get_edges())
        self._inputs = list(first_layer.get_inputs())
        self._outputs = list(first_layer.get_outputs())
        # self._backwards = (first_layer.get_backwards())

        self._forward_methods = self.create_methods(self._outputs, self._inputs, self._edges)

        for layer in layers[1:]:
            self.add_layer(layer)

    def add_layer(self, layer):
        new_outputs, new_edges = layer.get_forward_params(self._outputs)
        self._outputs = new_outputs

        for e in new_edges:
            assert e not in self._edges
            self._edges.append(e)

        self._forward_methods = self.create_methods(self._outputs, self._inputs, self._edges)

        # if self.use_backward:
        #   for layer in reversed(self.layers):
        #       self._backwards = layer.get_backward_params(self._backwards)

        self.layers.append(layer)

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
                 use_backward: bool = False, backwards: Sequence[Node] = None):
        super().__init__(use_backward=use_backward)

        self._edges = edges
        self._inputs = inputs
        self._outputs = outputs
        self._backwards = backwards

        self._forward_methods = self.create_methods(self._outputs, self._inputs, self._edges)

    def get_forward_params(self, other_outputs: Sequence[Node]):
        self.check_for_duplicates([x.name for x in other_outputs])
        outputs = node_to_dict(other_outputs)

        forward_edges = []
        for i in self._inputs:
            forward_edges.append(IdentityEdge(outputs[i.name], i))

        forward_edges.extend(self._edges)
        return self._outputs, forward_edges


class MuxLayer(FreeLayer):
    def __init__(self, branch_selector: Callable, *layers: FreeLayer):
        super().__init__(use_backward=False)

        self._outputs, self._inputs, self._edges = self.create_graph(branch_selector, layers)
        self._forward_methods = self.create_methods(self._outputs, self._inputs, self._edges)

    def get_forward_params(self, *args, **kwargs):
        raise RuntimeError("Mux layer can't be attached")

    def get_backward_params(self, other_backwards: Sequence[Node]):
        pass

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
