from typing import Tuple, Union, Iterable

from . import BagContext
from ..engine import Nodes, BoundEdges
from .base import EdgesBag
from ..utils import NameSet, node_to_dict, check_for_duplicates, AntiSet


class ReversibleContainer(EdgesBag):
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, backward_inputs: Nodes = (),
                 backward_outputs: Nodes = (), *, optional_nodes: NameSet = None,
                 forward_virtual: Union[NameSet, Iterable[str]], backward_virtual: Union[NameSet, Iterable[str]],
                 persistent_nodes: NameSet = None):
        forward_virtual, valid = normalize_inherit(forward_virtual, node_to_dict(outputs))
        assert valid
        backward_virtual, valid = normalize_inherit(backward_virtual, node_to_dict(backward_outputs))
        assert valid

        check_for_duplicates(inputs)
        super().__init__(
            inputs, outputs, edges,
            BagContext(backward_inputs, backward_outputs, backward_virtual),
            virtual_nodes=forward_virtual, persistent_nodes=persistent_nodes,
            optional_nodes=optional_nodes,
        )


def normalize_inherit(value, outputs) -> Tuple[NameSet, bool]:
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
