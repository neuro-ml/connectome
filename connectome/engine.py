import inspect
from threading import Lock
from collections import defaultdict
from typing import Sequence, Any, Tuple

from utils import atomize


class GraphParameter:
    def __init__(self, parameters, prev_edge=None, is_root: bool = False):
        self.prev_edge = prev_edge
        self.data = parameters
        self.is_root = is_root

    def __hash__(self):
        return hash(self.data)


class Node:
    def __init__(self, name: str):
        self.name = name

    def __str__(self):
        return f'<Node: {self.name}>'

    def __repr__(self):
        return str(self)


class Edge:
    def __init__(self, inputs: Sequence[Node], output: Node):
        self._inputs = tuple(inputs)
        self.output = output

    def evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: GraphParameter) -> Tuple[Any]:
        assert len(arguments) == len(essential_inputs)
        return self._evaluate(arguments, essential_inputs, parameter)

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter) -> Tuple[Any]:
        raise NotImplementedError

    def process_parameters(self, parameters: Sequence[GraphParameter]):
        return self.inputs, self._merge_parameters(parameters)

    def _merge_parameters(self, parameters: Sequence):
        for param in parameters:
            assert isinstance(param, GraphParameter)

        merged = (*[p.data for p in parameters],)
        return GraphParameter(merged, prev_edge=self)

    @property
    def inputs(self):
        return self._inputs

    @inputs.setter
    def inputs(self, value):
        assert len(value) == len(self._inputs)
        self._inputs = value


class StateHolder:
    def __init__(self, *, parents: dict = None):
        self.parents = parents
        self.essential_inputs = None

        self.entry_counts = defaultdict(int)
        self.edge_inputs = defaultdict(tuple)
        self.edge_parameters = {}
        self.cache = {}


class Graph:
    def __init__(self, inputs: Sequence[Node], outputs: Sequence[Node], edges: Sequence[Edge]):
        self.inputs = inputs
        self.outputs = []
        self.edges = []

        self.update(outputs, edges)

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def run(self, *args, **kwargs):
        parents = self.find_parents(self.outputs, self.edges)

        state = StateHolder(parents=parents)
        self.count_entries(self.outputs, state)

        signature = inspect.Signature([
            inspect.Parameter(node.name, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            for node in state.essential_inputs
        ])

        scope = signature.bind(*args, **kwargs)
        for x in state.essential_inputs:
            state.cache[x] = scope.arguments[x.name]

        self.set_parameters(state)
        result = [self.render(node, state) for node in self.outputs]
        # TODO: is this bad?
        if len(result) == 1:
            result = result[0]

        return result

    def render(self, node: Node, state: StateHolder):
        if node not in state.cache:
            edge: Edge = state.parents[node]
            arguments = []
            for x in state.edge_inputs[edge]:
                arg = self.render(x, state)
                arguments.append(arg)

            state.cache[node] = edge.evaluate(arguments, state.edge_inputs[edge], state.edge_parameters[edge])

        # extract
        state.entry_counts[node] -= 1
        value = state.cache[node]
        # expire
        if state.entry_counts[node] == 0:
            state.cache.pop(node)
        return value

    def update(self, new_outputs, new_edges: Sequence[Edge]):
        for new_edge in new_edges:
            assert new_edge not in self.edges

        self.outputs = new_outputs
        self.edges.extend(new_edges)

    def set_parameters(self, state: StateHolder):
        for node in self.outputs:
            self._set_parameters_rec(state.parents[node], state)

    def _set_parameters_rec(self, edge: Edge, state: StateHolder):
        parameters = []
        for node in edge.inputs:
            if node not in state.essential_inputs:
                parent_edge: Edge = state.parents[node]
                param = self._set_parameters_rec(parent_edge, state)
            else:
                param = GraphParameter(state.cache[node], is_root=True)

            parameters.append(param)

        inputs, param = edge.process_parameters(parameters)
        state.edge_inputs[edge] = inputs
        state.edge_parameters[edge] = param
        return param

    def count_entries(self, nodes: Sequence[Node], state: StateHolder):
        self._count_entries_rec(nodes, state)
        state.essential_inputs = [x for x in self.inputs if state.entry_counts[x] > 0]

    def _count_entries_rec(self, nodes: Sequence[Node], state: StateHolder):
        for node in nodes:
            state.entry_counts[node] += 1
            if node in state.parents:
                self._count_entries_rec(state.parents[node].inputs, state)

    def find_parents(self, nodes: Sequence[Node], edges: Sequence[Edge]):
        parents = {}
        self._find_parents_rec(nodes, edges, parents)
        return parents

    def _find_parents_rec(self, nodes: Sequence[Node], edges: Sequence[Edge], parents: dict):
        for node in nodes:
            # input has no parents
            if node in self.inputs:
                continue

            incoming = []
            for edge in edges:
                if edge.output == node:
                    incoming.append(edge)

            assert len(incoming) == 1, incoming
            edge = parents[node] = incoming[0]
            self._find_parents_rec(edge.inputs, edges, parents)


class Layer:
    def get_connection_params(self, other_outputs: Sequence[Node]):
        raise NotImplementedError


class FreeLayer(Layer):
    """
    Layer that supports 'run' method
    """

    def __init__(self, *args, **kwargs):
        self.graph = self.create_graph(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        if len(self.inputs) == 0:
            raise RuntimeError('Layer must contain at least 1 input node')

        return self.graph.run(*args, **kwargs)

    def get_connection_params(self, other_outputs: Sequence[Node]):
        raise NotImplementedError

    def create_graph(self, *args, **kwargs):
        raise NotImplementedError

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


# TODO redefine operators
class CacheStorage(object):
    def __init__(self, atomized=True):
        self._atomized = atomized
        self.mutex = Lock()

    def contains(self, param: GraphParameter) -> bool:
        raise NotImplementedError

    def set(self, param: GraphParameter, value):
        raise NotImplementedError

    def get(self, param: GraphParameter) -> Any:
        raise NotImplementedError

    def __getattribute__(self, name):
        attr = object.__getattribute__(self, name)
        if callable(attr):
            if self.atomized:
                return atomize(self.mutex)(attr)
        else:
            return attr

    @property
    def atomized(self):
        return self._atomized

    @atomized.setter
    def atomized(self, value: bool):
        self._atomized = value


class MemoryStorage(CacheStorage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache = {}

    def contains(self, param: GraphParameter) -> bool:
        return param.data in self._cache

    def set(self, param: GraphParameter, value):
        self._cache[param.data] = value

    def get(self, param: GraphParameter) -> Any:
        return self._cache[param.data]
