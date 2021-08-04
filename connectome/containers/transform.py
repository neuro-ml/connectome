from typing import Tuple, Sequence, Union, Iterable, NamedTuple, List, Set

from .base import Context, EdgesBag, update_map
from ..engine.base import BoundEdge, Node, Nodes, BoundEdges, TreeNode, TreeNodes
from ..engine.edges import IdentityEdge
from ..engine.graph import count_entries
from ..exceptions import DependencyError
from ..utils import check_for_duplicates, node_to_dict

INHERIT_ALL = True
InheritType = Union[str, Iterable[str], bool]


class LayerConnectionState(NamedTuple):
    # elements of composed layer
    inputs: List
    outputs: List
    edges: List[BoundEdge]
    # names of already used prev layer inputs
    used_names: Set
    # elements of current/previous layer
    cur_virtual: List
    cur_inputs: Nodes
    cur_outputs: Nodes
    cur_optional: Sequence
    cur_used_virtual: Set
    prev_virtual: List
    prev_inputs: Nodes
    prev_outputs: Nodes
    prev_used_virtual: Set


class TransformContainer(EdgesBag):
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, backward_inputs: Nodes = (),
                 backward_outputs: Nodes = (), *, optional_nodes: Sequence[str] = (),
                 forward_virtual: InheritType, backward_virtual: InheritType,
                 persistent_nodes: Sequence[str] = ()):

        forward_virtual, valid = normalize_inherit(forward_virtual)
        assert valid
        # if it's a tuple - it must be empty
        if isinstance(forward_virtual, tuple):
            assert not forward_virtual

        backward_virtual, valid = normalize_inherit(backward_virtual)
        assert valid

        check_for_duplicates(inputs)
        super().__init__(
            inputs, outputs, edges,
            BagContext(backward_inputs, backward_outputs, backward_virtual),
            virtual_nodes=forward_virtual, persistent_nodes=tuple(persistent_nodes)
        )
        self.optional_nodes = tuple(optional_nodes)

    def wrap(self, container: 'EdgesBag') -> 'EdgesBag':
        current = self.freeze()
        previous = container.freeze()
        inherit_nodes = self.virtual_nodes
        all_edges = list(previous.edges) + list(current.edges)
        persistent_nodes = tuple(set(previous.persistent_nodes) | set(self.persistent_nodes))

        if inherit_nodes != INHERIT_ALL:
            inherit_nodes = list(inherit_nodes) + list(persistent_nodes)

        state = LayerConnectionState(
            cur_inputs=current.inputs,
            cur_outputs=current.outputs,
            cur_virtual=inherit_nodes,
            cur_optional=self.optional_nodes,
            prev_inputs=previous.inputs,
            prev_outputs=previous.outputs,
            prev_virtual=previous.virtual_nodes,
            inputs=list(previous.inputs),
            outputs=list(current.outputs),
            used_names=set(),
            cur_used_virtual=set(),
            prev_used_virtual=set(),
            edges=all_edges,
        )

        self._connect_common_nodes(state)
        self._connect_cur_virtual(state)
        self._connect_prev_virtual(state)

        essential_input_names = self.get_essential_input_names(current.inputs, current.outputs, current.edges)
        for o in current.outputs:
            # drop nodes that depend on inactive inputs
            if any(name not in state.used_names for name in essential_input_names[o]):
                state.outputs.remove(o)

        check_for_duplicates(state.outputs)
        new_virtual_nodes = self._merge_virtual_nodes(state)
        return EdgesBag(
            state.inputs, state.outputs, state.edges,
            PipelineContext(previous.context, current.context),
            virtual_nodes=new_virtual_nodes, persistent_nodes=persistent_nodes,
        )

    @staticmethod
    def _connect_common_nodes(state: LayerConnectionState):
        prev_outputs = node_to_dict(state.prev_outputs)
        for i in state.cur_inputs:
            if i.name in prev_outputs:
                state.used_names.add(i.name)
                state.edges.append(IdentityEdge().bind(prev_outputs[i.name], i))

    @staticmethod
    def _connect_cur_virtual(state: LayerConnectionState):
        cur_outputs = node_to_dict(state.cur_outputs)
        prev_outputs = node_to_dict(state.prev_outputs)

        for name, prev_output in prev_outputs.items():
            if name in cur_outputs:
                continue
            # propagate identity transformation
            if state.cur_virtual == INHERIT_ALL or name in state.cur_virtual:
                output = Node(name)
                state.outputs.append(output)
                state.used_names.add(name)
                state.cur_used_virtual.add(name)
                state.edges.append(BoundEdge(IdentityEdge(), [prev_output], output))

    @staticmethod
    def _connect_prev_virtual(state: LayerConnectionState):
        cur_inputs = node_to_dict(state.cur_inputs)
        cur_outputs = node_to_dict(state.cur_outputs)
        prev_inputs = node_to_dict(state.prev_inputs)
        unused_names = set(cur_inputs.keys()).difference(set(state.used_names))

        for name in unused_names:
            if name not in state.cur_optional:
                # propagate identity transformation
                if state.prev_virtual == INHERIT_ALL or name in state.prev_virtual:
                    if name in prev_inputs:
                        input_node = prev_inputs[name]
                    else:
                        input_node = Node(name)
                        state.inputs.append(input_node)

                    if name not in cur_inputs:
                        output = Node(name)
                    else:
                        output = cur_inputs[name]

                    if name not in cur_outputs:
                        state.outputs.append(output)

                    state.used_names.add(name)
                    state.prev_used_virtual.add(name)
                    state.edges.append(BoundEdge(IdentityEdge(), [input_node], output))
                else:
                    raise DependencyError(f"Previous layer must contain '{name}' node.")

    @staticmethod
    def _merge_virtual_nodes(state: LayerConnectionState):
        # remove created outputs from new propagated nodes
        if isinstance(state.prev_virtual, bool):
            unused_prev_virtual = state.prev_virtual
        else:
            unused_prev_virtual = []
            for node_name in state.prev_virtual:
                if node_name not in state.prev_used_virtual:
                    unused_prev_virtual.append(node_name)

        if isinstance(state.cur_virtual, bool):
            unused_cur_virtual = state.cur_virtual
        else:
            unused_cur_virtual = []
            for node_name in state.cur_virtual:
                if node_name not in state.cur_used_virtual:
                    unused_cur_virtual.append(node_name)

        if isinstance(unused_cur_virtual, bool):
            return unused_prev_virtual

        if isinstance(unused_prev_virtual, bool):
            return unused_cur_virtual

        else:
            return list(set.intersection(set(unused_prev_virtual), set(unused_cur_virtual)))

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


def normalize_inherit(value):
    if isinstance(value, str):
        value = [value]

    if isinstance(value, bool):
        valid = value
    else:
        value = tuple(value)
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
    def __init__(self, inputs: Nodes, outputs: Nodes, inherit: InheritType):
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
        output_names = node_to_dict(actual)
        for name, node in outputs.items():
            name = node.name
            if name not in output_names and (self.inherit == INHERIT_ALL or name in self.inherit):
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
