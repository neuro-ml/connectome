from functools import reduce
from typing import Sequence, Tuple, Any

from utils import count_duplicates
from engine import Graph, GraphParameter, Layer, Node, Edge, MemoryStorage, CacheStorage


class IdentityEdge(Edge):
    def __init__(self, incoming: Node, output: Node):
        super().__init__([incoming], output)

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: GraphParameter) -> Tuple[Any]:
        return arguments[0]


class CacheEdge(Edge):
    def __init__(self, incoming: Node, output: Node, *, storage: CacheStorage = None):
        super().__init__([incoming], output)

        if storage is None:
            self.storage = MemoryStorage()
        else:
            self.storage = storage

    def process_parameters(self, parameters: Sequence[GraphParameter]):
        parameter = self._merge_parameters(parameters)
        if self.storage.contains(parameter):
            inputs = []
        else:
            inputs = self.inputs

        return inputs, parameter

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: GraphParameter):
        if len(arguments) == 0:
            return self.storage.get(parameter)
        else:
            self.storage.set(parameter, arguments[0])
            return arguments[0]


class FunctionEdge(Edge):
    def __init__(self, function, inputs: Sequence[Node], output: Node):
        super().__init__(inputs, output)
        self.function = function

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: GraphParameter) -> Tuple[Any]:
        return self.function(*arguments)


class ValueEdge(Edge):
    def __init__(self, target: Node, value):
        super().__init__([], target)
        self.value = value

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: GraphParameter) -> Tuple[Any]:
        return self.value


class CacheToDisk(Edge):
    # TODO: path
    def __init__(self, incoming: Node, output: Node):
        super().__init__([incoming], output)

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: GraphParameter) -> Tuple[Any]:
        return arguments[0]


class MemoryCacheLayer(Layer):
    def __init__(self):
        super().__init__()

    def get_connection_params(self, other_outputs: Sequence[Node]):
        this_outputs = [Node(f'cache_output{i}') for i in range(len(other_outputs))]
        edges = [
            CacheEdge(other_output, this_output, storage=MemoryStorage())
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


class CustomLayer(Layer):
    def __init__(self, edges: Sequence[Edge]):
        # TODO is it necessary? move it to pipeline?
        inputs = self.get_all_inputs(edges)
        counts: dict = count_duplicates([x.name for x in inputs])

        if any(v > 1 for k, v in counts.items()):
            raise RuntimeError('Input nodes must have different names')

        super().__init__()
        # TODO match with graph edges
        self._edges = edges

    def get_connection_params(self, other_outputs: Sequence[Node]):

        inputs = self.get_all_inputs(self._edges)
        assert len(inputs) == len(other_outputs)

        other_outputs = iter(other_outputs)
        for e in self._edges:
            new_inputs = [next(other_outputs) for i in range(len(e.inputs))]
            e.inputs = new_inputs

        outputs = [e.output for e in self._edges]
        return outputs, self._edges

    @staticmethod
    def get_all_inputs(edges: Sequence[Edge]):
        inputs = []
        for e in edges:
            inputs.extend(e.inputs)
        return inputs


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
