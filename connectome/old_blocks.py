from functools import reduce
from typing import Sequence

from connectome.engine.edges import IdentityEdge, FunctionEdge
from .old_engine import Graph, Node
from .layers import AttachableLayer, FreeLayer


class IdentityLayer(AttachableLayer):
    def get_forward_params(self, other_outputs: Sequence[Node]):
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

    def get_forward_params(self, other_outputs: Sequence[Node]):
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

    def get_forward_params(self, other_outputs: Sequence[Node]):
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

    def get_forward_params(self, other_outputs: Sequence[Node]):
        return self.outputs, self.edges
