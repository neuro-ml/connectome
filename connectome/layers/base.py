from typing import Sequence, Callable, Tuple, NamedTuple, List, Set

from ..engine.edges import IdentityEdge, FunctionEdge
from ..engine.graph import compile_graph, count_entries
from ..engine.base import TreeNode, BoundEdge, Node
from ..utils import check_for_duplicates, node_to_dict

Nodes = Sequence[Node]
BoundEdges = Sequence[BoundEdge]

INHERIT_ALL = object()


class Layer:
    pass


class LayerParams(NamedTuple):
    edges: List[BoundEdge]
    inputs: List[Node]
    outputs: List[Node]

    backward_inputs: List[Node]
    backward_outputs: List[Node]

    persistent_nodes: Set[str]


class Attachable(Layer):
    def attach(self, forward_outputs: Nodes, backward_inputs: Nodes,
               persistent_nodes: Sequence[str] = None) -> Tuple[Nodes, Nodes, BoundEdges]:
        """
        Returns new forward and backward nodes, as well as additional edges.
        """
        graph_params = self.prepare()
        persistent_nodes = persistent_nodes or []
        graph_params.persistent_nodes.update(persistent_nodes)

        forward_outputs, new = self._attach_forward(forward_outputs, graph_params)
        graph_params.edges.extend(new)

        backward_inputs, new = self._attach_backward(backward_inputs, graph_params)
        graph_params.edges.extend(new)
        return forward_outputs, backward_inputs, graph_params.edges

    # TODO set defaults somewhere else
    def prepare(self) -> LayerParams:
        raise NotImplementedError

    def _attach_forward(self, nodes: Sequence, params: LayerParams) -> Tuple[Nodes, BoundEdges]:
        raise NotImplementedError

    def _attach_backward(self, nodes: Sequence, params: LayerParams) -> Tuple[Nodes, BoundEdges]:
        raise NotImplementedError


class EdgesBag(Attachable):
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, backward_inputs: Nodes = None,
                 backward_outputs: Nodes = None, optional_nodes: Sequence[str] = None,
                 inherit_nodes: Sequence[str] = None, persistent_nodes: Sequence[str] = None):

        check_for_duplicates(node_to_dict(inputs).keys())

        self.inputs = inputs
        self.outputs = outputs
        self.edges = edges

        self.backward_inputs = backward_inputs = backward_inputs or []
        self.backward_outputs = backward_outputs = backward_outputs or []
        check_for_duplicates(node_to_dict(backward_inputs))

        # TODO check for duplicates
        self.inherit_nodes = inherit_nodes or []
        self.optional_nodes = optional_nodes or []
        self.persistent_nodes = set(persistent_nodes or [])

        tree_node_map = TreeNode.from_edges(edges)
        inputs = [tree_node_map[x] for x in inputs]
        outputs = [tree_node_map[x] for x in outputs]

        backward_inputs = [tree_node_map[x] for x in backward_inputs]
        backward_outputs = [tree_node_map[x] for x in backward_outputs]

        self._forward_methods = {}
        for node in outputs:
            self._forward_methods[node.name] = compile_graph(inputs, node, self.uses_hash())

        self._backward_methods = {}
        for node in backward_outputs:
            self._backward_methods[node.name] = compile_graph(backward_inputs + inputs, node, self.uses_hash())

    def get_forward_method(self, name):
        return self._forward_methods[name]

    def get_backward_method(self, name):
        return self._backward_methods[name]

    def prepare(self):
        """
        Prepares a copy of edges and nodes for connection.
        """
        # TODO: layer inputs and outputs may not be among the edges
        node_map = {}
        edges_copy = []

        for edge in self.edges:
            inputs = self.update_map(edge.inputs, node_map)
            output = self.update_map([edge.output], node_map)[0]
            edges_copy.append(BoundEdge(edge.edge, inputs, output))

        params = LayerParams(
            edges_copy,
            self.update_map(self.inputs, node_map),
            self.update_map(self.outputs, node_map),
            self.update_map(self.backward_inputs, node_map),
            self.update_map(self.backward_outputs, node_map),
            self.persistent_nodes
        )
        return params

    # TODO: it would be much simpler if edges would get overwritten
    def _attach_forward(self, prev_outputs: Nodes, params: LayerParams) -> Tuple[Nodes, BoundEdges]:
        check_for_duplicates([x.name for x in prev_outputs])

        prev_outputs = node_to_dict(prev_outputs)
        cur_inputs = node_to_dict(params.inputs)

        if self.inherit_nodes == INHERIT_ALL:
            inherit_nodes = list(prev_outputs.keys())
        else:
            inherit_nodes = self.inherit_nodes + list(params.persistent_nodes)

        outputs = []
        new_edges = []
        active_input_names = []

        # connect common nodes
        for i in params.inputs:
            if i.name in prev_outputs:
                active_input_names.append(i.name)
                new_edges.append(BoundEdge(IdentityEdge(), [prev_outputs[i.name]], i))

        # check for inherited nodes
        defined_outputs = [o.name for o in params.outputs]
        for name, prev_output in prev_outputs.items():
            if name in inherit_nodes and (
                    name not in active_input_names or
                    name not in defined_outputs):
                output = Node(name)
                outputs.append(output)
                active_input_names.append(name)
                new_edges.append(BoundEdge(IdentityEdge(), [prev_output], output))

        # check that unused nodes are @optional
        unused_names = set(cur_inputs.keys()).difference(set(active_input_names))
        for name in unused_names:
            if name not in self.optional_nodes:
                raise RuntimeError(f"Previous layer must contain '{name}' node.")

        essential_input_names = self.get_essential_input_names(params.inputs, params.outputs, params.edges)
        for o in params.outputs:
            # drop nodes that depend on inactive inputs
            if all(name in active_input_names for name in essential_input_names[o]):
                outputs.append(o)

        return outputs, new_edges

    def _attach_backward(self, prev_inputs: Nodes, params: LayerParams) -> Tuple[Nodes, BoundEdges]:
        # means that this is the last backward layer
        if len(prev_inputs) == 0:
            return params.backward_inputs, []

        check_for_duplicates([x.name for x in prev_inputs])
        prev_inputs = node_to_dict(prev_inputs)

        if self.inherit_nodes == INHERIT_ALL:
            inherit_node_names = list(prev_inputs.keys())
        else:
            inherit_node_names = self.inherit_nodes

        inputs = []
        new_edges = []
        active_output_names = []
        cur_outputs = node_to_dict(params.backward_outputs)

        for o in params.backward_outputs:
            if o.name in prev_inputs:
                active_output_names.append(o.name)
                new_edges.append(BoundEdge(IdentityEdge(), [o], prev_inputs[o.name]))

        for name, prev_input in prev_inputs.items():
            if name not in active_output_names and name in inherit_node_names:
                inp = Node(name)
                inputs.append(inp)
                active_output_names.append(name)
                new_edges.append(BoundEdge(IdentityEdge(), [inp], prev_input))

        # check that unused nodes are @optional
        unused_names = set(cur_outputs.keys()).difference(set(active_output_names))
        for name in unused_names:
            if name not in self.optional_nodes:
                raise RuntimeError(f"Previous layer must contain '{name}' node.")

        # drop inactive inputs
        for name, node in node_to_dict(params.backward_inputs).items():
            if name in active_output_names:
                inputs.append(node)

        return inputs, new_edges

    def get_loopback(self, function: Callable, forward_names: Sequence[str], backward_name: str):
        """
        Creates a graph by closing forward outputs and backward input using the given function.
        """

        forward_outputs = node_to_dict(self.outputs)
        backward_inputs = node_to_dict(self.backward_inputs)
        backward_outputs = node_to_dict(self.backward_outputs)

        required_outputs = {}
        for name in forward_names:
            assert name in forward_outputs
            required_outputs[name] = forward_outputs[name]

        assert backward_name in backward_inputs
        loopback_output = backward_inputs[backward_name]
        loopback_inputs = list(required_outputs.values())

        loopback_edge = BoundEdge(
            FunctionEdge(function, len(forward_names)),
            loopback_inputs, loopback_output
        )

        edges = list(self.edges)
        edges.append(loopback_edge)

        mapping = TreeNode.from_edges(edges)
        graph_inputs = [mapping[x] for x in self.inputs]
        graph_output = mapping[backward_outputs[backward_name]]
        return compile_graph(graph_inputs, graph_output)

    def uses_hash(self):
        return any(e.edge.uses_hash for e in self.edges)

    @staticmethod
    def update_map(nodes, node_map):
        for node in nodes:
            if node not in node_map:
                node_map[node] = Node(node.name)
        return [node_map[x] for x in nodes]

    @staticmethod
    def get_essential_input_names(inputs: Sequence[Node], outputs: Sequence[Node], edges: BoundEdges):
        check_for_duplicates(node_to_dict(inputs).keys())

        tree_node_map = TreeNode.from_edges(edges)
        inputs = [tree_node_map[x] for x in inputs]

        essential_input_names = {}
        for o in outputs:
            output = tree_node_map[o]
            counts = count_entries(inputs, [output])
            input_names = [x.name for x in inputs if counts.get(x, 0)]
            essential_input_names[o] = input_names
        return essential_input_names
