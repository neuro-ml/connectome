from typing import Tuple

from ..containers import Context, EdgesBag
from ..engine import Nodes, BoundEdges, IdentityEdge
from ..utils import check_for_duplicates, node_to_dict


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

    right_inputs = node_to_dict(right.inputs)
    left_outputs = node_to_dict(left.outputs)

    # common
    for name in set(left_outputs) & set(right_inputs):
        edges.append(IdentityEdge().bind(left_outputs[name], right_inputs[name]))

    # left virtuals
    for name in left.virtual_nodes & set(right_inputs):
        out = right_inputs[name]
        inp = out.clone()
        edges.append(IdentityEdge().bind(inp, out))
        inputs.add(inp)
        if out in optionals:
            optionals.add(inp)

    # right virtuals
    for name in set(left_outputs) & (right.virtual_nodes | left.persistent_nodes):
        inp = left_outputs[name]
        out = inp.clone()
        edges.append(IdentityEdge().bind(inp, out))
        outputs.add(out)
        if inp in optionals:
            optionals.add(out)

    check_for_duplicates(outputs)
    return EdgesBag(
        inputs, outputs, edges,
        ChainContext(left.context, right.context),
        virtual_nodes=left.virtual_nodes & right.virtual_nodes,
        optional_nodes=optionals,
        persistent_nodes=left.persistent_nodes | right.persistent_nodes,
    )
