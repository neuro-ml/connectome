import warnings
from typing import Sequence

from .base import EdgesBag
from ..engine.base import Node
from ..engine.edges import ConstantEdge, IdentityEdge
from ..layers.merge import SwitchEdge
from ..utils import node_to_dict


class SwitchContainer(EdgesBag):  # pragma: no cover
    def __init__(self, id_to_index: dict, layers: Sequence[EdgesBag], keys_name: str, persistent_names: Sequence[str]):
        warnings.warn('This class is deprecated and will be removed soon', DeprecationWarning)
        warnings.warn('This class is deprecated and will be removed soon', UserWarning)

        inputs = []
        groups = []
        edges = []
        # gather parts
        for layer in layers:
            params = layer.freeze()
            if len(params.inputs) != 1:
                raise ValueError('Each layer must have exactly one input')
            inputs.append(params.inputs[0])
            groups.append(node_to_dict(params.outputs))
            edges.extend(params.edges)

        # validate inputs
        inp = [x.name for x in inputs]
        if len(set(inp)) != 1:
            raise ValueError(f'Layer inputs must have the same name: {inp}')

        # create the new input
        inp = Node(inp[0])
        for node in inputs:
            edges.append(IdentityEdge().bind(inp, node))

        # create new outputs
        outputs = []
        common_outputs = set.intersection(*map(set, groups)) - {keys_name}
        for name in common_outputs:
            node = Node(name)
            branches = [group[name] for group in groups]
            outputs.append(node)
            edges.append(SwitchEdge(id_to_index, len(layers)).bind([inp] + branches, node))

        # and the keys
        ids = Node(keys_name)
        outputs.append(ids)
        edges.append(ConstantEdge(tuple(sorted(id_to_index))).bind([], ids))

        super().__init__([inp], outputs, edges, context=None, persistent_nodes=set(persistent_names))
        self.layers = layers
