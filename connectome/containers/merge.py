from typing import Sequence, Any, Generator

from ..engine.edges import ConstantEdge, IdentityEdge
from ..engine.base import Node, NodeHash, Edge, NodeHashes, HashOutput, Request, Response, Command
from ..engine.node_hash import MergeHash
from ..utils import node_to_dict
from .base import EdgesBag


class SwitchContainer(EdgesBag):
    def __init__(self, id_to_index: dict, layers: Sequence[EdgesBag], keys_name: str, persistent_names: Sequence[str]):
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


class SwitchEdge(Edge):
    def __init__(self, id_to_index: dict, n_branches: int):
        super().__init__(arity=1 + n_branches)
        self.id_to_index = id_to_index

    def compute_hash(self) -> Generator[Request, Response, HashOutput]:
        key = yield Command.ParentValue, 0
        try:
            idx = self.id_to_index[key]
        except KeyError:
            raise ValueError(f'Identifier {key} not found.') from None

        value = yield Command.ParentHash, idx + 1
        return value, idx

    def evaluate(self) -> Generator[Request, Response, Any]:
        payload = yield Command.Payload,
        value = yield Command.ParentValue, payload + 1
        return value

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return MergeHash(*inputs)
