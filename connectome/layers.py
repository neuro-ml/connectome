from pathlib import Path
from typing import Sequence, Callable

from .cache import DiskStorage, MemoryStorage
from .edges import CacheEdge, IdentityEdge, MuxEdge
from .utils import count_duplicates
from .engine import Graph, Layer, Node, Edge


class FreeLayer(Layer):
    """
    Layer that supports 'run' method
    """

    def __init__(self, *args, **kwargs):
        self.graph = self.create_graph(*args, **kwargs)
        self._methods = self.create_output_node_methods(self.outputs)

    # TODO: do we need this?
    def __call__(self, *args, node_names=None, **kwargs):
        if len(self.inputs) == 0:
            raise RuntimeError('Layer must contain at least 1 input node')

        caller = self.graph.compile_graph(node_names=node_names)
        return caller(*args, **kwargs)

    def __getattr__(self, item):
        # to stop recursion in bad cases
        # TODO: maybe use get_method instead?
        if '_methods' in self.__dict__ and item in self._methods:
            return self._methods[item]

        # TODO add more details
        raise AttributeError

    def get_connection_params(self, other_outputs: Sequence[Node]):
        raise NotImplementedError

    def create_graph(self, *args, **kwargs):
        raise NotImplementedError

    def create_output_node_methods(self, nodes):
        methods = {}
        for node in nodes:
            methods[node.name] = self.graph.compile_graph(node_names=[node.name])
        return methods

    def get_output_node_methods(self):
        return self._methods

    @property
    def inputs(self):
        return self.graph.inputs

    @inputs.setter
    def inputs(self, value):
        self.graph.inputs = value

    @property
    def outputs(self):
        return self.graph.outputs

    @property
    def edges(self):
        return self.graph.edges


class AttachableLayer(Layer):
    def get_connection_params(self, *args, **kwargs):
        raise NotImplementedError

    def get_output_node_methods(self):
        return {}


class MemoryCacheLayer(AttachableLayer):
    def get_connection_params(self, other_outputs: Sequence[Node]):
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

    def get_connection_params(self, other_outputs: Sequence[Node]):
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
        super().__init__(layers[0])

        self.layers = layers
        for layer in layers[1:]:
            self.add_layer(layer)

    def add_layer(self, layer):
        new_outputs, new_edges = layer.get_connection_params(self.outputs)

        self.graph.update(new_outputs, new_edges)
        self._methods = self.create_output_node_methods(self.outputs)

    def create_graph(self, first_layer):
        return Graph(first_layer.inputs, first_layer.outputs, first_layer.edges)

    def get_connection_params(self, outputs: Sequence[Node]):
        all_edges = []
        for layer in self.layers:
            outputs, edges = layer.get_connection_params(outputs)
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
    def __init__(self, inputs, outputs, edges: Sequence[Edge]):
        super().__init__(inputs, outputs, edges)

    def create_graph(self, inputs, outputs, edges: Sequence[Edge]):
        return Graph(inputs, outputs, edges)

    def get_connection_params(self, other_outputs: Sequence[Node]):
        self.check_for_duplicates([x.name for x in other_outputs])

        outputs = {}
        for o in other_outputs:
            outputs[o.name] = o

        new_edges = []
        for i in self.inputs:
            new_edges.append(IdentityEdge(outputs[i.name], i))
        return self.outputs, self.edges + new_edges

    @staticmethod
    def check_for_duplicates(collection):
        counts: dict = count_duplicates([x for x in collection])
        if any(v > 1 for k, v in counts.items()):
            raise RuntimeError('Input nodes must have different names')


class MuxLayer(FreeLayer):
    def __init__(self, branch_selector: Callable, *layers: FreeLayer):
        super().__init__(branch_selector, *layers)

    def get_connection_params(self, *args, **kwargs):
        raise RuntimeError("Mux layer can't be attached")

    def create_graph(self, branch_selector: Callable, *layers: FreeLayer):
        input_nodes = []
        shared_output_names = set()

        for layer in layers:
            input_nodes.extend(layer.inputs)
            # check for outputs with the same name
            output_names = [o.name for o in layer.outputs]
            assert any(v < 2 for _, v in count_duplicates(output_names).items())
            shared_output_names.update(output_names)

        layers_outputs_map = []
        layers_input_names = []

        for layer in layers:
            cur_outputs_map = {o.name: o for o in layer.outputs if o.name in shared_output_names}
            cur_essential_inputs_map, _, _ = layer.graph.get_graph_structure(cur_outputs_map.values())
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
            all_edges.extend(layer.edges)

        all_edges.extend(mux_edges)
        return Graph(input_nodes, outputs, all_edges)