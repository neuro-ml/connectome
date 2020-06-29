import inspect
from collections import defaultdict
from typing import Sequence, Any, Tuple


class Node:
    def __init__(self, name: str):
        self.name = name

    def __str__(self):
        return f'<Node: {self.name}>'

    def __repr__(self):
        return str(self)


class Edge:
    def __init__(self, inputs: Sequence[Node], output: Node):
        self.inputs = tuple(inputs)
        self.output = output

    def evaluate(self, arguments: Sequence, parameters: Sequence) -> Tuple[Any, Any]:
        assert len(arguments) == len(parameters) == len(self.inputs)
        return self._evaluate(arguments, parameters)

    def _evaluate(self, arguments: Sequence, parameters: Sequence) -> Tuple[Any, Any]:
        raise NotImplementedError


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
                count_parents(parents[node]._inputs)

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


class Layer:
    def _prepare(self, output: Sequence[Node]):
        return self._inputs, self._outputs, self._edges

    def _combine(self, other: 'Layer'):
        def match(n: Node):
            for out in self._outputs:
                if n.name == out.name:
                    return out
            raise RuntimeError(n)

        inputs, outputs, edges = other._prepare(self._outputs)
        edges = set(edges) | set(self._edges)
        for node in inputs:
            edges.add(IdentityEdge(match(node), node))

        return Layer(self._inputs, outputs, list(edges))


class Pipeline:
    def __init__(self, *layers):
        self._layers = layers

        for node in outputs:
            setattr(self, node.name, compile_graph(inputs, [node], edges))
