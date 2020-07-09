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


class NodeHash:
    def __init__(self, *, prev_edge=None, data=None, children=None):
        self.prev_edge = prev_edge
        self.children = children
        self._data = data

    def __hash__(self):
        return hash(self.data)

    @classmethod
    def from_hash_nodes(cls, hashes: Sequence, prev_edge=None):
        for h in hashes:
            assert isinstance(h, NodeHash)
        return NodeHash(children=hashes, prev_edge=prev_edge)

    @property
    def data(self):
        if self.children is None:
            return self._data
        else:
            merged = (*[h.data for h in self.children],)
            return merged


class Edge:
    def __init__(self, inputs: Sequence[Node], output: Node):
        self._inputs = tuple(inputs)
        self.output = output

    def evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter: NodeHash):
        assert len(arguments) == len(essential_inputs)
        return self._evaluate(arguments, essential_inputs, parameter)

    def _evaluate(self, arguments: Sequence, essential_inputs: Sequence[Node], parameter):
        raise NotImplementedError

    def process_parameters(self, parameters: Sequence[NodeHash]):
        raise NotImplementedError

    @property
    def inputs(self):
        return self._inputs

    @inputs.setter
    def inputs(self, value):
        assert len(value) == len(self._inputs)
        self._inputs = value


# TODO looks unnecessary
class StateHolder:
    def __init__(self, *, parents: dict, inputs_map: dict, required_outputs: list, entry_counts: defaultdict,
                 scope: inspect.BoundArguments):
        self.scope = scope
        self.inputs_map = inputs_map
        self.required_outputs = required_outputs
        self.entry_counts = entry_counts
        self.parents = parents

        self.edge_inputs = defaultdict(tuple)
        self.used_input_names = defaultdict(list)
        self.edge_parameters = {}
        self.cache = {}


class Graph:
    def __init__(self, inputs: Sequence[Node], outputs: Sequence[Node], edges: Sequence[Edge]):
        self.inputs = inputs
        self.outputs = []
        self.edges = []

        self.update(outputs, edges)

    def compile_graph(self, node_names=None):
        name_node_dict = {}
        for o in self.outputs:
            name_node_dict[o.name] = o

        if node_names is None:
            required_outputs = self.outputs
        else:
            required_outputs = []
            for name in node_names:
                # TODO replace by exception
                assert name in name_node_dict
                required_outputs.append(name_node_dict[name])

        inputs_map, parents, entry_counts = self.get_graph_structure(required_outputs)

        signature = inspect.Signature([
            inspect.Parameter(node_name, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            for node_name in inputs_map.keys()
        ])

        def caller(*args, **kwargs):
            scope = signature.bind(*args, **kwargs)
            state = StateHolder(parents=parents,
                                required_outputs=required_outputs,
                                entry_counts=entry_counts,
                                inputs_map=inputs_map,
                                scope=scope)

            self.set_parameters(state)
            result = tuple(self.render(node, state) for node in state.required_outputs)

            # TODO: is this bad?
            if len(result) == 1:
                result = result[0]

            return result

        caller.__signature__ = signature
        return caller

    def set_parameters(self, state: StateHolder):
        for name in state.inputs_map:
            for x in state.inputs_map[name]:
                state.cache[x] = state.scope.arguments[x.name]

        for node in state.required_outputs:
            self._set_parameters_rec(state.parents[node], state)

    # TODO check if essential input nodes have different names

    def _set_parameters_rec(self, edge: Edge, state: StateHolder):
        parameters = []
        for node in edge.inputs:
            name = node.name
            if name in state.inputs_map and node in state.inputs_map[name]:
                param = NodeHash(data=state.cache[node], children=None)
            else:
                parent_edge: Edge = state.parents[node]
                param = self._set_parameters_rec(parent_edge, state)

            parameters.append(param)

        inputs, param = edge.process_parameters(parameters)

        state.edge_inputs[edge] = inputs
        state.edge_parameters[edge] = param
        return param

    def get_graph_structure(self, required_outputs):
        parents = self.find_parents(required_outputs, self.edges)
        entry_counts = self.count_entries(parents, required_outputs)

        inputs_map = defaultdict(list)
        for x in self.inputs:
            if entry_counts[x] > 0:
                inputs_map[x.name].append(x)

        return inputs_map, parents, entry_counts

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

    def count_entries(self, parents: dict, outputs: Sequence[Node]):
        entry_counts = defaultdict(int)
        self._count_entries_rec(nodes=outputs, entry_counts=entry_counts, parents=parents)
        return entry_counts

    def _count_entries_rec(self, *, nodes: Sequence[Node], entry_counts: dict, parents: dict):
        for node in nodes:
            entry_counts[node] += 1
            if node in parents:
                self._count_entries_rec(nodes=parents[node].inputs, entry_counts=entry_counts, parents=parents)

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

    def update(self, new_outputs, new_edges: Sequence[Edge]):
        for new_edge in new_edges:
            assert new_edge not in self.edges

        self.outputs = new_outputs
        self.edges.extend(new_edges)


class Layer:
    def get_connection_params(self, other_outputs: Sequence[Node]):
        raise NotImplementedError

    def get_output_node_methods(self):
        raise NotImplementedError
