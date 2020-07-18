import inspect

from collections import defaultdict
from typing import Sequence, Any, Tuple, Dict, Union


# TODO it looks unnecessary
class StateHolder:
    def __init__(self, *, parents: dict, inputs_map: dict, required_outputs: Sequence[Node], entry_counts: defaultdict,
                 scope: inspect.BoundArguments):
        self.parents = parents
        self.inputs_map = inputs_map
        self.required_outputs = required_outputs
        self.entry_counts = entry_counts
        self.scope = scope

        self.edge_inputs = defaultdict(tuple)
        self.used_inputs = {}
        self.edge_parameters = {}
        self.cache = {}


class Graph:
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

        inputs, param = edge.process_hashes(parameters)

        state.edge_inputs[edge] = inputs
        state.edge_parameters[edge] = param
        return param

    def get_graph_structure(self, required_outputs, required_inputs, edges):
        parents = self.find_parents(required_outputs, required_inputs, edges)
        entry_counts = self.count_entries(parents, required_outputs)

        inputs_map = defaultdict(list)

        for x in required_inputs:
            if entry_counts[x] > 0:
                inputs_map[x.name].append(x)

        return inputs_map, parents, entry_counts

    def find_parents(self, output_nodes: Sequence[Node], input_nodes: Sequence[Node], edges: Sequence[Edge]):
        parents = {}
        self._find_parents_rec(output_nodes, input_nodes, edges, parents)
        return parents

    def _find_parents_rec(self, outputs: Sequence[Node], inputs: Sequence[Node], edges: Sequence[Edge], parents: dict):
        for node in outputs:
            # input has no parents
            if node in inputs:
                continue

            incoming = []
            for edge in edges:
                if edge.output == node:
                    incoming.append(edge)

            assert len(incoming) == 1, incoming
            edge = parents[node] = incoming[0]
            self._find_parents_rec(edge.inputs, inputs, edges, parents)
