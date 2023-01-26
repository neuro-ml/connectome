from typing import Tuple, NamedTuple, List, Dict

from ..containers import Context, EdgesBag
from ..engine import BoundEdge, Node, Nodes, BoundEdges, IdentityEdge
from ..utils import check_for_duplicates, node_to_dict, NameSet


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
    cur_virtual: NameSet
    cur_inputs: Dict[str, Node]
    cur_optional: NameSet
    prev_virtual: set
    prev_persistent: NameSet
    prev_outputs: Dict[str, Node]


class ChainContext(Context):
    def __init__(self, previous: Context, current: Context):
        self.previous = previous
        self.current = current

    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        outputs, edges = self.current.reverse(inputs, outputs, edges)
        outputs, edges = self.previous.reverse(inputs, outputs, edges)
        return outputs, edges

    def update(self, mapping: dict) -> 'Context':
        return ChainContext(self.previous.update(mapping), self.current.update(mapping))


def connect(head: EdgesBag, *tail: EdgesBag) -> EdgesBag:
    for container in tail:
        head = _connect(head, container)
    return head


def _connect(left: EdgesBag, right: EdgesBag) -> EdgesBag:
    left = left.freeze()
    right = right.freeze()

    inputs, outputs = set(left.inputs), set(right.outputs)
    edges = list(left.edges) + list(right.edges)
    optionals = left.optional_nodes | right.optional_nodes

    # state = LayerConnectionState(
    #     edges=all_edges,
    #     cur_used_virtual=set(),
    #     used_input_names=set(),
    #     prev_used_virtual=set(),
    #     used_output_names=set(),
    #     inputs=list(left.inputs),
    #     outputs=list(right.outputs),
    #     cur_virtual=right.virtual_nodes,
    #     cur_optional=right.optional_nodes,
    #     cur_inputs=node_to_dict(right.inputs),
    #     prev_virtual=left.virtual_nodes,
    #     prev_outputs=node_to_dict(left.outputs),
    #     prev_persistent=left.persistent_nodes,
    # )

    right_inputs = node_to_dict(right.inputs)
    left_outputs = node_to_dict(left.outputs)

    # common
    for name in set(left_outputs) & set(right_inputs):
        edges.append(IdentityEdge().bind(left_outputs[name], right_inputs[name]))

    # left virtuals
    for name in left.virtual_nodes & set(right_inputs):
        inp = Node(name)
        out = right_inputs[name]
        edges.append(IdentityEdge().bind(inp, out))
        inputs.add(inp)
        if out in optionals:
            optionals.add(inp)

    # right virtuals
    for name in set(left_outputs) & (right.virtual_nodes | left.persistent_nodes):
        inp = left_outputs[name]
        out = Node(name)
        edges.append(IdentityEdge().bind(inp, out))
        outputs.add(out)
        if inp in optionals:
            optionals.add(out)

    # essential_input_names = _get_essential_input_names(right.inputs, right.outputs, right.edges)
    # for o in right.outputs:
    #     # drop nodes that depend on inactive inputs
    #     if any(name not in state.used_input_names for name in essential_input_names[o]):
    #         state.outputs.remove(o)

    check_for_duplicates(outputs)
    return EdgesBag(
        inputs, outputs, edges,
        ChainContext(left.context, right.context),
        virtual_nodes=left.virtual_nodes & right.virtual_nodes,
        optional_nodes=optionals,
        persistent_nodes=left.persistent_nodes | right.persistent_nodes,
    )

# def _connect_common_nodes(state: LayerConnectionState):
#     cur_inputs = state.cur_inputs
#     prev_outputs = state.prev_outputs
#     common_node_names = set(cur_inputs).intersection(set(prev_outputs))
#     for name in common_node_names:
#         state.used_input_names.add(name)
#         state.used_output_names.add(name)
#         state.edges.append(IdentityEdge().bind(prev_outputs[name], cur_inputs[name]))

#
# def _connect_cur_virtual(state: LayerConnectionState):
#     prev_outputs = state.prev_outputs
#     unused_prev_outputs = set(prev_outputs) - state.used_output_names
#     for name in unused_prev_outputs:
#         if name in state.cur_virtual or name in state.prev_persistent:
#             output = Node(name)
#             state.outputs.append(output)
#             state.cur_used_virtual.add(name)
#             state.used_output_names.add(name)
#             state.edges.append(IdentityEdge().bind(prev_outputs[name], output))
#
#
# def _connect_prev_virtual(state: LayerConnectionState):
#     cur_inputs = state.cur_inputs
#     unused_input_names = set(cur_inputs).difference(state.used_input_names)
#     for name in unused_input_names:
#         if name in state.prev_virtual:
#             input_node = Node(name)
#             output = cur_inputs[name]
#             state.inputs.append(input_node)
#             state.used_input_names.add(name)
#             state.prev_used_virtual.add(name)
#             state.edges.append(BoundEdge(IdentityEdge(), [input_node], output))
#
#         elif name not in state.cur_optional:
#             raise DependencyError(f"Previous layer must contain '{name}' node.")

# def _merge_virtual_nodes(state: LayerConnectionState):
#     unused_cur_virtual = state.cur_virtual - state.cur_used_virtual
#     unused_prev_virtual = state.prev_virtual - state.prev_used_virtual
#     return unused_cur_virtual & unused_prev_virtual
#
#
# def _get_essential_input_names(inputs: Sequence[Node], outputs: Sequence[Node], edges: BoundEdges):
#     check_for_duplicates(inputs)
#     tree_node_map = TreeNode.from_edges(edges)
#     inputs = [tree_node_map[x] for x in inputs]
#
#     essential_input_names = {}
#     for o in outputs:
#         output = tree_node_map[o]
#         counts = count_entries(inputs, output)
#         input_names = [x.name for x in inputs if counts.get(x, 0)]
#         essential_input_names[o] = input_names
#     return essential_input_names
