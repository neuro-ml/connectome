from pathlib import Path
from typing import Sequence

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


# TODO sequence of layers
class MuxLayer(FreeLayer):
    def __init__(self, func, first_layer: FreeLayer, second_layer: FreeLayer):
        super().__init__(func, first_layer, second_layer)

    def create_graph(self, func, first_layer: FreeLayer, second_layer: FreeLayer):
        shared_output_names = set(
            [o.name for o in first_layer.outputs]).intersection(
            [o.name for o in second_layer.outputs])

        assert len(first_layer.inputs) == len(second_layer.inputs) == 1
        input_nodes = (first_layer.inputs[0], second_layer.inputs[0])

        # TODO check for duplicated names
        first_name_map = {o.name: o for o in first_layer.outputs if o.name in shared_output_names}
        second_name_map = {o.name: o for o in second_layer.outputs if o.name in shared_output_names}

        inputs = []
        outputs = []
        hub_edges = []

        for name in shared_output_names:
            output_node = Node(name)
            first_node, second_node = first_name_map[name], second_name_map[name]
            edge = MuxEdge(func, [first_node, second_node], output_node)

            hub_edges.append(edge)
            inputs.extend([first_node, second_node])
            outputs.append(output_node)

        all_edges = first_layer.edges + second_layer.edges + hub_edges
        return Graph(input_nodes, outputs, all_edges)

    def get_connection_params(self, *args, **kwargs):
        raise RuntimeError("Can't attach Mux layer")
