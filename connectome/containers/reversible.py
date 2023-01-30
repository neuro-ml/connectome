from collections import defaultdict
from typing import Tuple, Union, Iterable

from ..engine import Nodes, BoundEdges, TreeNode
from ..engine.compiler import find_dependencies
from ..exceptions import GraphError
from ..utils import NameSet, node_to_dict, check_for_duplicates, AntiSet
from .base import EdgesBag, normalize_bag
from .context import BagContext


class ReversibleContainer(EdgesBag):
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, backward_inputs: Nodes = (),
                 backward_outputs: Nodes = (), *, optional_inputs: NameSet = None, optional_outputs: NameSet = None,
                 forward_virtual: Union[NameSet, Iterable[str]], backward_virtual: Union[NameSet, Iterable[str]],
                 persistent_nodes: NameSet = None):
        forward_virtual, valid = normalize_inherit(forward_virtual, node_to_dict(outputs))
        assert valid
        backward_virtual, valid = normalize_inherit(backward_virtual, node_to_dict(backward_outputs))
        assert valid

        if optional_inputs is None:
            optional_inputs = set()
        if optional_outputs is None:
            optional_outputs = set()
        if persistent_nodes is None:
            persistent_nodes = set()

        inputs, outputs, edges, forward_virtual = normalize_bag(
            inputs, outputs, edges, forward_virtual, set(), persistent_nodes
        )
        optional_nodes = detect_optionals(optional_inputs, optional_outputs, inputs, outputs, edges)
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


def detect_optionals(input_names, output_names, inputs, outputs, edges):
    mapping = TreeNode.from_edges(edges)
    inverse = {v: k for k, v in mapping.items()}
    inputs_mapping = node_to_dict(inputs)
    outputs_mapping = node_to_dict(outputs)

    optional_inputs, optional_outputs = set(), set()
    for name in input_names:
        if name not in inputs_mapping:
            raise GraphError(f'The optional node {name} is not present among the inputs')
        optional_inputs.add(inputs_mapping[name])
    for name in output_names:
        if name not in outputs_mapping:
            raise GraphError(f'The optional node {name} is not present among the outputs')
        optional_outputs.add(outputs_mapping[name])

    tree_outputs = {mapping[x] for x in outputs}
    dependencies = find_dependencies(tree_outputs)
    dependencies = {inverse[k]: {inverse[v] for v in vs} for k, vs in dependencies.items()}

    # reversed dependencies
    reach = defaultdict(set)
    for output in dependencies:
        for dep in dependencies[output]:
            reach[dep].add(output)

    # if all inputs are optional - the output becomes optional
    for output in dependencies:
        if dependencies[output] and dependencies[output] <= optional_inputs:
            optional_outputs.add(output)

    # if all outputs are optional - the input becomes optional
    for inp in inputs:
        if reach[inp] and reach[inp] <= optional_outputs:
            optional_inputs.add(inp)

    # TODO: warn
    # if not optional_inputs:
    return optional_outputs | optional_inputs
