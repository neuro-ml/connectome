from functools import reduce
from typing import Sequence, Tuple, Any

from .utils import count_duplicates
from .engine import Graph, GraphParameter, AttachableLayer, FreeLayer, Node, Edge, MemoryStorage, CacheStorage


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
        if self.storage.contains(parameter):
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


class MemoryCacheLayer(AttachableLayer):
    def get_connection_params(self, other_outputs: Sequence[Node]):
        this_outputs = [Node(o.name) for o in other_outputs]
        edges = [
            CacheEdge(other_output, this_output, storage=MemoryStorage())
            for other_output, this_output in zip(other_outputs, this_outputs)
        ]
        return this_outputs, edges


class IdentityLayer(AttachableLayer):
    def get_connection_params(self, other_outputs: Sequence[Node]):
        this_outputs = [Node(o.name) for o in other_outputs]
        edges = [
            IdentityEdge(other_output, this_output)
            for other_output, this_output in zip(other_outputs, this_outputs)
        ]
        return this_outputs, edges


class Lambda(AttachableLayer):
    def __init__(self, func, required_node_names: Sequence = None):
        self.required_node_names = required_node_names
        self.func = func

    def get_connection_params(self, other_outputs: Sequence[Node]):
        if self.required_node_names is None:
            required_node_names = [o.name for o in other_outputs]
        else:
            required_node_names = self.required_node_names

        edges = []
        this_outputs = [Node(o.name) for o in other_outputs]

        for other_output, this_output in zip(other_outputs, this_outputs):
            if this_output.name in required_node_names:
                new_edge = FunctionEdge(self.func, [other_output], this_output)
            else:
                new_edge = IdentityEdge(other_output, this_output)
            edges.append(new_edge)

        return this_outputs, edges


class Reducer(AttachableLayer):
    def __init__(self, func, output_name: str):
        self.output_name = output_name
        self.func = func

    def get_connection_params(self, other_outputs: Sequence[Node]):
        output = Node(self.output_name)

        def reduce_decorator(func):
            def wrapper(*sequence):
                result = reduce(func, sequence)
                return result

            return wrapper

        wrapped = reduce_decorator(self.func)
        edge = FunctionEdge(wrapped, other_outputs, output)
        return [output], [edge]


class InputLayer(FreeLayer):
    def __init__(self, output_names: Sequence[str]):
        super().__init__(output_names)

    def create_graph(self, output_names: Sequence[str], input_names: Sequence[str] = None):
        if input_names is not None:
            assert len(output_names) == len(input_names)
        else:
            input_names = output_names

        inputs = [Node(name) for name in input_names]
        outputs = [Node(name) for name in output_names]

        edges = [IdentityEdge(i, o) for i, o in zip(inputs, outputs)]
        return Graph(inputs, outputs, edges)

    def get_connection_params(self, other_outputs: Sequence[Node]):
        return self.outputs, self.edges


class Pipeline(FreeLayer):
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


class CustomLayer(FreeLayer):
    def __init__(self, inputs, outputs, edges: Sequence[Edge]):
        super().__init__(inputs, outputs, edges)

    def create_graph(self, inputs, outputs, edges: Sequence[Edge]):
        return Graph(inputs, outputs, edges)

    def get_connection_params(self, other_outputs: Sequence[Node]):
        self.check_for_duplicates([x.name for x in other_outputs])

        # inputs = self.get_all_inputs(self.edges)
        # TODO is it necessary? no (:
        # assert len(inputs) == len(other_outputs), (inputs, other_outputs)

        outputs = {}
        for o in other_outputs:
            outputs[o.name] = o

        for e in self.edges:
            inputs = []
            for i in e.inputs:
                inputs.append(outputs[i.name])
            e.inputs = inputs

        return self.outputs, self.edges

    @staticmethod
    def check_for_duplicates(collection):
        counts: dict = count_duplicates([x for x in collection])
        if any(v > 1 for k, v in counts.items()):
            raise RuntimeError('Input nodes must have different names')

    @staticmethod
    def get_all_inputs(edges: Sequence[Edge]):
        inputs = set()
        for e in edges:
            inputs.update(e.inputs)
        return sorted(inputs, key=lambda n: n.name)
