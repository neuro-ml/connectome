from typing import Tuple, Sequence, NamedTuple, List, Dict, Set

from .base import Context, EdgesBag, update_map, NameSet
from ..engine.base import BoundEdge, Node, Nodes, BoundEdges, TreeNode, TreeNodes
from ..engine.edges import IdentityEdge
from ..engine.graph import count_entries
from ..exceptions import DependencyError
from ..utils import AntiSet, check_for_duplicates, node_to_dict


class LayerConnectionState(NamedTuple):
    # elements of composed layer
    inputs: List
    outputs: List
    edges: List[BoundEdge]
    # names of already used nodes
    used_output_names: set
    used_input_names: set
    cur_used_virtual: set
    prev_used_virtual: set
    # elements of current/previous layer
    cur_virtual: Set[str]
    cur_inputs: Dict[str, Node]
    cur_optional: Set[str]
    prev_virtual: set
    prev_persistent: Set[str]
    prev_outputs: Dict[str, Node]


class TransformContainer(EdgesBag):
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, backward_inputs: Nodes = (),
                 backward_outputs: Nodes = (), *, optional_nodes: NameSet = None,
                 forward_virtual: NameSet, backward_virtual: NameSet, persistent_nodes: NameSet = None):

        forward_virtual, valid = normalize_inherit(forward_virtual, node_to_dict(outputs))
        assert valid
        backward_virtual, valid = normalize_inherit(backward_virtual, node_to_dict(backward_outputs))
        assert valid

        check_for_duplicates(inputs)
        super().__init__(
            inputs, outputs, edges,
            BagContext(backward_inputs, backward_outputs, backward_virtual),
            virtual_nodes=forward_virtual, persistent_nodes=persistent_nodes
        )
        self.optional_nodes = optional_nodes if optional_nodes is not None else set()

    def wrap(self, container: 'EdgesBag') -> 'EdgesBag':
        current = self.freeze()
        previous = container.freeze()
        all_edges = list(previous.edges) + list(current.edges)
        persistent_nodes = set.union(set(previous.persistent_nodes), set(self.persistent_nodes))
        state = LayerConnectionState(
            edges=all_edges,
            cur_used_virtual=set(),
            used_input_names=set(),
            prev_used_virtual=set(),
            used_output_names=set(),
            inputs=list(previous.inputs),
            outputs=list(current.outputs),
            cur_virtual=self.virtual_nodes,
            cur_optional=self.optional_nodes,
            cur_inputs=node_to_dict(current.inputs),
            prev_virtual=container.virtual_nodes,
            prev_outputs=node_to_dict(previous.outputs),
            prev_persistent=previous.persistent_nodes,
        )

        self._connect_common_nodes(state)
        self._connect_cur_virtual(state)
        self._connect_prev_virtual(state)

        essential_input_names = self.get_essential_input_names(current.inputs, current.outputs, current.edges)
        for o in current.outputs:
            # drop nodes that depend on inactive inputs
            if any(name not in state.used_input_names for name in essential_input_names[o]):
                state.outputs.remove(o)

        check_for_duplicates(state.outputs)
        new_virtual_nodes = self._merge_virtual_nodes(state)
        return EdgesBag(
            state.inputs, state.outputs, state.edges,
            PipelineContext(previous.context, current.context),
            virtual_nodes=new_virtual_nodes, persistent_nodes=persistent_nodes
        )

    @staticmethod
    def _connect_common_nodes(state: LayerConnectionState):
        cur_inputs = state.cur_inputs
        prev_outputs = state.prev_outputs
        common_node_names = set(cur_inputs.keys()).intersection(set(prev_outputs.keys()))
        for name in common_node_names:
            state.used_input_names.add(name)
            state.used_output_names.add(name)
            state.edges.append(IdentityEdge().bind(prev_outputs[name], cur_inputs[name]))

    @staticmethod
    def _connect_cur_virtual(state: LayerConnectionState):
        prev_outputs = state.prev_outputs
        unused_prev_outputs = set(prev_outputs.keys()).difference(state.used_output_names)
        for name in unused_prev_outputs:
            if name in state.cur_virtual or name in state.prev_persistent:
                output = Node(name)
                state.outputs.append(output)
                state.cur_used_virtual.add(name)
                state.used_output_names.add(name)
                state.edges.append(BoundEdge(IdentityEdge(), [prev_outputs[name]], output))

    @staticmethod
    def _connect_prev_virtual(state: LayerConnectionState):
        cur_inputs = state.cur_inputs
        unused_input_names = set(cur_inputs.keys()).difference(state.used_input_names)
        for name in unused_input_names:
            if name in state.prev_virtual:
                input_node = Node(name)
                output = cur_inputs[name]
                state.inputs.append(input_node)
                state.used_input_names.add(name)
                state.prev_used_virtual.add(name)
                state.edges.append(BoundEdge(IdentityEdge(), [input_node], output))

            elif name not in state.cur_optional:
                raise DependencyError(f"Previous layer must contain '{name}' node.")

    @staticmethod
    def _merge_virtual_nodes(state: LayerConnectionState):
        unused_cur_virtual = state.cur_virtual - state.cur_used_virtual
        unused_prev_virtual = state.prev_virtual - state.prev_used_virtual
        return unused_cur_virtual & unused_prev_virtual

    @staticmethod
    def get_essential_input_names(inputs: Sequence[Node], outputs: Sequence[Node], edges: BoundEdges):
        check_for_duplicates(inputs)
        tree_node_map = TreeNode.from_edges(edges)
        inputs = [tree_node_map[x] for x in inputs]

        essential_input_names = {}
        for o in outputs:
            output = tree_node_map[o]
            counts = count_entries(inputs, output)
            input_names = [x.name for x in inputs if counts.get(x, 0)]
            essential_input_names[o] = input_names
        return essential_input_names


def is_reachable(inputs: TreeNodes, output: TreeNode):
    def reachable(x: TreeNode):
        if x.is_leaf:
            return x in inputs

        return all(map(reachable, x.parents))

    inputs = set(inputs)
    return reachable(output)


def normalize_inherit(value, outputs) -> Tuple[Set[str], bool]:
    if isinstance(value, str):
        value = [value]

    if isinstance(value, bool):
        valid = value
        value = AntiSet(set(outputs))
    elif isinstance(value, AntiSet):
        valid = True
    else:
        value = set(value)
        valid = all(isinstance(node_name, str) for node_name in value)

    return value, valid


class PipelineContext(Context):
    def __init__(self, previous: Context, current: Context):
        self.previous = previous
        self.current = current

    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        outputs, edges = self.current.reverse(inputs, outputs, edges)
        outputs, edges = self.previous.reverse(inputs, outputs, edges)
        return outputs, edges

    def update(self, mapping: dict) -> 'Context':
        return PipelineContext(self.previous.update(mapping), self.current.update(mapping))


class BagContext(Context):
    def __init__(self, inputs: Nodes, outputs: Nodes, inherit: Set[str]):
        self.inputs = inputs
        self.outputs = outputs
        self.inherit = inherit

    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        edges = list(edges)
        outputs = node_to_dict(outputs)
        # backward transforms
        for node in self.inputs:
            name = node.name
            if name in outputs:
                edges.append(IdentityEdge().bind(outputs[name], node))

        # collect the actual outputs
        actual = []
        mapping = TreeNode.from_edges(edges)
        leaves = [mapping[node] for node in inputs]
        for node in self.outputs:
            if is_reachable(leaves, mapping[node]):
                actual.append(node)

        # add inheritance
        add = self.inherit - set(node_to_dict(actual))
        for name, node in outputs.items():
            name = node.name
            if name in add:
                out = Node(name)
                edges.append(IdentityEdge().bind(node, out))
                actual.append(out)

        return actual, edges

    def update(self, mapping: dict) -> 'Context':
        return BagContext(
            update_map(self.inputs, mapping),
            update_map(self.outputs, mapping),
            self.inherit,
        )
