from engine import Graph, Node, FunctionEdge, IdentityEdge, MemoryCacheEdge

from functools import reduce
from typing import Sequence


class Layer:
    def __init__(self, *args, **kwargs):
        self.graph = self.create_graph(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        if len(self.inputs) == 0:
            raise RuntimeError('Layer must contain at least 1 input node')

        return self.graph.run(*args, **kwargs)

    def get_connection_params(self, other_outputs: Sequence[Node]):
        raise NotImplementedError

    def create_graph(self, *args, **kwargs):
        return Graph([], [], [])

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


class MemoryCacheLayer(Layer):
    def __init__(self):
        super().__init__()

    def get_connection_params(self, other_outputs: Sequence[Node]):
        this_outputs = [Node(f'cache_output{i}') for i in range(len(other_outputs))]
        edges = [
            MemoryCacheEdge(other_output, this_output)
            for other_output, this_output in zip(other_outputs, this_outputs)
        ]
        return this_outputs, edges


class InputLayer(Layer):
    def __init__(self, size):
        super().__init__(size)

    def create_graph(self, size):
        inputs = [Node(f'input_input_{i}') for i in range(size)]
        outputs = [Node(f'input_output_{i}') for i in range(size)]
        edges = [IdentityEdge(i, o) for i, o in zip(inputs, outputs)]
        return Graph(inputs, outputs, edges)

    def get_connection_params(self, other_outputs: Sequence[Node]):
        return self.outputs, self.edges


class IdentityLayer(Layer):
    def __init__(self):
        super().__init__()

    def get_connection_params(self, other_outputs: Sequence[Node]):
        this_outputs = [Node(f'lambda_output{i}') for i in range(len(other_outputs))]
        edges = [
            IdentityEdge(other_output, this_output)
            for other_output, this_output in zip(other_outputs, this_outputs)
        ]
        return this_outputs, edges


class Lambda(Layer):
    def __init__(self, func):
        self.func = func
        super().__init__(func)

    def get_connection_params(self, other_outputs: Sequence[Node]):
        this_outputs = [Node(f'lambda_output{i}') for i in range(len(other_outputs))]
        edges = [
            FunctionEdge(self.func, [other_output], this_output)
            for other_output, this_output in zip(other_outputs, this_outputs)
        ]
        return this_outputs, edges


class Reducer(Layer):
    def __init__(self, func):
        self.func = func
        super().__init__(func)

    def get_connection_params(self, other_outputs: Sequence[Node]):
        output = Node(f'reduce_output')

        def reduce_decorator(func):
            def wrapper(*sequence):
                result = reduce(func, sequence)
                return result

            return wrapper

        wrapped = reduce_decorator(self.func)
        edge = FunctionEdge(wrapped, other_outputs, output)
        return [output], [edge]


class Pipeline(Layer):
    def __init__(self, *layers):
        assert len(layers) > 0
        super().__init__(layers[0])

        self.layers = layers
        for layer in layers[1:]:
            self.add_layer(layer)

    def add_layer(self, layer):
        new_outputs, new_edges = layer.get_connection_params(self.outputs)
        self.graph.update(new_outputs, new_edges)

    def create_graph(self, first_layer):
        return Graph(first_layer.inputs, first_layer.outputs, first_layer.edges)

    def get_connection_params(self, outputs: Sequence[Node]):
        all_edges = []
        for layer in self.layers:
            outputs, edges = layer.get_connection_params(outputs)
            all_edges.extend(edges)

        return outputs, all_edges
