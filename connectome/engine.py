import inspect
from collections import defaultdict
from functools import reduce
from typing import Sequence, Any, Tuple


class Node:
    def __init__(self, name: str, count: int = 0):
        self._entry_count = count
        self.name = name

    def __str__(self):
        return f'<Node: {self.name}>'

    def __repr__(self):
        return str(self)

    def reset_count(self):
        self._entry_count = 0

    def inc_count(self):
        self._entry_count += 1

    def dec_count(self):
        self._entry_count -= 1

    def is_used(self):
        return self._entry_count > 0


class Edge:
    def __init__(self, inputs: Sequence[Node], output: Node):
        self.inputs = tuple(inputs)
        self.output = output

    def reset(self):
        for x in self.inputs:
            x.reset_count()

        self.output.reset_count()

    def evaluate(self, arguments: Sequence, parameters: Sequence) -> Tuple[Any, Any]:
        assert len(arguments) == len(parameters) == len(self.inputs)
        return self._evaluate(arguments, parameters)

    def _evaluate(self, arguments: Sequence, parameters: Sequence) -> Tuple[Any, Any]:
        raise NotImplementedError


class Graph:
    def __init__(self, inputs: Sequence[Node], outputs: Sequence[Node], edges: Sequence[Edge]):
        self.inputs = inputs
        self.outputs = outputs
        self.edges = edges
        self.essential_inputs = inputs

        self.parents = {}
        self.cache = {}
        self.build()

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def reset(self):
        # TODO add checks
        self.parents = {}
        self.cache = {}

        for e in self.edges:
            e.reset()

    def run(self, *args, **kwargs):
        # TODO
        signature = inspect.Signature([
            inspect.Parameter(node.name, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            for node in self.essential_inputs
        ])

        scope = signature.bind(*args, **kwargs)
        for x in self.essential_inputs:
            self.cache[x] = (scope.arguments[x.name],) * 2

        result = [self.render(node)[0] for node in self.outputs]
        # TODO: is this bad?
        if len(result) == 1:
            result = result[0]
        return result

    def render(self, node: Node):
        if node not in self.cache:
            edge: Edge = self.parents[node]
            arguments, parameters = [], []
            for x in edge.inputs:
                arg, param = self.render(x)
                arguments.append(arg)
                parameters.append(param)

            self.cache[node] = edge.evaluate(arguments, parameters)

        # extract
        node.dec_count()
        value = self.cache[node]
        # expire
        if not node.is_used():
            self.cache.pop(node)
        return value

    def build(self):
        self._find_parents(self.outputs, self.edges)
        self._count_entries(self.outputs)
        self.essential_inputs = [node for node in self.inputs if node.is_used()]

    def update(self, new_outputs, new_edges: Sequence[Edge]):
        for new_edge in new_edges:
            assert new_edge not in self.edges

        self.outputs = new_outputs
        self.edges.extend(new_edges)
        self.reset()
        self.build()

    def _find_parents(self, nodes: Sequence[Node], edges: Sequence[Edge]):
        for node in nodes:
            # input has no parents
            if node in self.inputs:
                continue

            incoming = []
            for edge in edges:
                if edge.output == node:
                    incoming.append(edge)

            assert len(incoming) == 1, incoming
            edge = self.parents[node] = incoming[0]
            self._find_parents(edge.inputs, edges)

    def _count_entries(self, nodes: Sequence[Node]):
        for node in nodes:
            node.inc_count()
            if node in self.parents:
                self._count_entries(self.parents[node].inputs)


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


# TODO remove dependencies
class IdentityLayer(Layer):
    def __init__(self, size):
        super().__init__(size)

    def create_graph(self, size):
        inputs = [Node(f'identity_input_{i}') for i in range(size)]
        outputs = [Node(f'identity_output_{i}') for i in range(size)]
        edges = [IdentityEdge(i, o) for i, o in zip(inputs, outputs)]
        return Graph(inputs, outputs, edges)

    def get_connection_params(self, other_outputs: Sequence[Node]):
        return self.outputs, self.edges


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


class Repeater(Layer):
    pass


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


class FunctionEdge(Edge):
    def __init__(self, function, inputs: Sequence[Node], output: Node):
        super().__init__(inputs, output)
        self.function = function

    def _evaluate(self, arguments: Sequence, parameters: Sequence) -> Tuple[Any, Any]:
        # TODO: pickle the function
        return self.function(*arguments), (self.function, *parameters)


class IdentityEdge(Edge):
    def __init__(self, incoming: Node, output: Node):
        super().__init__([incoming], output)

    def _evaluate(self, arguments: Sequence, parameters: Sequence) -> Tuple[Any, Any]:
        return arguments[0], parameters[0]


class ValueEdge(Edge):
    def __init__(self, target: Node, value):
        super().__init__([], target)
        self.value = value

    def _evaluate(self, arguments: Sequence, parameters: Sequence) -> Tuple[Any, Any]:
        return self.value, self.value


class CacheToDisk(Edge):
    # TODO: path
    def __init__(self, incoming: Node, output: Node):
        super().__init__([incoming], output)

    def _evaluate(self, arguments: Sequence, parameters: Sequence) -> Tuple[Any, Any]:
        print(parameters)
        return arguments[0], parameters[0]


# class Layer:
#   def _prepare(self, output: Sequence[Node]):
#      return self._inputs, self._outputs, self._edges

#  def _combine(self, other: 'Layer'):
#     def match(n: Node):
#           for out in self._outputs:
#             if n.name == out.name:
#                  return out
#           raise RuntimeError(n)

#    inputs, outputs, edges = other._prepare(self._outputs)
#   edges = set(edges) | set(self._edges)
#  for node in inputs:
#     edges.add(IdentityEdge(match(node), node))

#  return Layer(self._inputs, outputs, list(edges))


# class Pipeline:
#   def __init__(self, *layers):
#      self._layers = layers
#
#       for node in outputs:
#          setattr(self, node.name, compile_graph(inputs, [node], edges))


def compile_graph(inputs: Sequence[Node], outputs: Sequence[Node], edges: Sequence[Edge]):
    # TODO: detect cycles

    def find_parents(nodes: Sequence[Node]):
        for node in nodes:
            # input has no parents
            if node in inputs:
                continue

            incoming = []
            for edge in edges:
                if edge.output == node:
                    incoming.append(edge)

            assert len(incoming) == 1, incoming
            edge = parents[node] = incoming[0]
            find_parents(edge.inputs)

    def count_parents(nodes: Sequence[Node]):
        for node in nodes:
            counts[node] += 1
            if node in parents:
                count_parents(parents[node].inputs)

    parents = {}
    counts = defaultdict(int)

    find_parents(outputs)
    count_parents(outputs)

    essential_inputs = [node for node in inputs if counts[node] > 0]
    signature = inspect.Signature([
        inspect.Parameter(node.name, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        for node in essential_inputs
    ])

    def caller(*args, **kwargs):
        def render(node: Node):
            if node not in cache:
                edge: Edge = parents[node]
                arguments, parameters = [], []
                for x in edge.inputs:
                    arg, param = render(x)
                    arguments.append(arg)
                    parameters.append(param)

                cache[node] = edge.evaluate(arguments, parameters)

            # extract
            counts[node] -= 1
            value = cache[node]
            # expire
            if counts[node] <= 0:
                cache.pop(node)
            return value

        cache = {}
        scope = signature.bind(*args, **kwargs)
        for x in essential_inputs:
            cache[x] = (scope.arguments[x.name],) * 2

        result = [render(node)[0] for node in outputs]
        # TODO: is this bad?
        if len(result) == 1:
            result = result[0]
        return result

    caller.__signature__ = signature
    return caller
