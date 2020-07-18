import inspect

from collections import defaultdict
from typing import Sequence, Any, Tuple, Dict, Union


# TODO: hashes should also store a type
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


NodesMask = Sequence[int]


class Edge:
    def __init__(self, arity):
        self.arity = arity

    def evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        assert len(arguments) == len(mask)
        return self._evaluate(arguments, mask, node_hash)

    def process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        assert len(hashes) == self.arity
        node_hash, mask = self._process_hashes(hashes)
        assert all(0 <= x < self.arity for x in mask)
        assert len(set(mask)) == len(mask)
        return node_hash, mask

    def _evaluate(self, arguments: Sequence, mask: NodesMask, node_hash: NodeHash):
        raise NotImplementedError

    def _process_hashes(self, hashes: Sequence[NodeHash]) -> Tuple[NodeHash, NodesMask]:
        raise NotImplementedError


class Node:
    def __init__(self, name: str, edges: Dict[Edge, Sequence['Node']]):
        # TODO: need an object that encapsulates this relation
        self.edges = edges
        self.name = name

    def __str__(self):
        return f'<Node: {self.name}>'

    def __repr__(self):
        return str(self)


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


class ExpirationCache:
    def __init__(self, counts):
        self.counts = counts
        self.cache = {}

    def __setitem__(self, key, value):
        assert key in self.counts
        assert key not in self.cache
        self.cache[key] = value

    def __getitem__(self, key):
        assert self.counts[key]
        value = self.cache[key]
        self.counts[key] -= 1
        if self.counts[key] <= 0:
            del self.cache[key]
            del self.counts[key]
        return value

    def __contains__(self, key):
        return key in self.cache


class Graph:
    def compile(self, inputs: Sequence[Node], outputs: Union[Node, Sequence[Node]]):
        squeeze = isinstance(outputs, Node)
        if squeeze:
            outputs = [outputs]

        self.validate_graph(inputs, outputs)
        counts = self.count_entries(inputs, outputs)
        inputs = [x for x in inputs if counts[x]]
        inputs_map = {x.name: x for x in inputs}

        signature = inspect.Signature([
            inspect.Parameter(name, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            for name in inputs_map
        ])

        def caller(*args, **kwargs):
            scope = signature.bind(*args, **kwargs)
            # drop unnecessary branches
            masks = self.get_masks(inputs_map, outputs, scope.arguments, counts)
            # prepare
            cache = ExpirationCache(self.count_entries(inputs, outputs, masks))
            for name, n in inputs_map.items():
                cache[n] = scope.arguments[name]

            result = tuple(self.render(node, cache, masks) for node in outputs)
            if squeeze:
                result = result[0]
            return result

        caller.__signature__ = signature
        return caller

    @staticmethod
    def validate_graph(inputs, outputs):
        def visitor(nodes):
            for node in nodes:
                # no edges - must be an input
                if not node.edges:
                    assert node in inputs

                assert len(node.edges) == 1
                # input doesn't need parents
                if node not in inputs:
                    for group in node.edges.values():
                        visitor(group)

        visitor(outputs)

    @staticmethod
    def count_entries(inputs: Sequence[Node], outputs: Sequence[Node], masks=None):
        def visitor(node: Node):
            entry_counts[node] += 1
            # input doesn't need parents
            if node in inputs:
                return

            group, = node.edges.values()
            if masks is not None:
                group = [group[idx] for idx in masks[node]]

            visitor(group)

        entry_counts = defaultdict(int)
        for x in outputs:
            visitor(x)
        return dict(entry_counts)

    @staticmethod
    def get_masks(inputs_map, outputs, arguments, counts):
        def visitor(node: Node):
            if node in cache:
                return

            (edge, group), = node.edges.items()
            result, mask = edge.process_hashes([visitor(x) for x in group])
            masks[node] = mask
            cache[node] = result
            return result

        masks = {}
        cache = ExpirationCache(counts.copy())
        for name, n in inputs_map.items():
            cache[n] = arguments[name]
        for n in outputs:
            visitor(n)

        return masks

    def render(self, node, cache, masks, hashes):
        if node not in cache:
            (edge, inputs), = node.edges.items()
            mask = masks[node]
            inputs = [inputs[idx] for idx in masks]
            cache[node] = edge.evaluate([self.render(x, cache, masks, hashes) for x in inputs], mask, hashes[node])

        return cache[node]

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


class Layer:
    def get_forward_params(self, other_outputs: Sequence[Node]):
        raise NotImplementedError

    def get_backward_params(self, other_backwards: Sequence[Node]):
        raise NotImplementedError

    def get_all_forward_methods(self):
        raise NotImplementedError

    def get_backward_inputs(self):
        raise NotImplementedError

    def get_backward_outputs(self):
        raise NotImplementedError

    def get_inputs(self):
        raise NotImplementedError

    def get_outputs(self):
        raise NotImplementedError

    def get_edges(self):
        raise NotImplementedError
