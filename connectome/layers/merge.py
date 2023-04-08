from typing import Any, Generator, Sequence

from ..containers import EdgesBag
from ..engine import (
    ConstantEdge, IdentityEdge, Node, NodeHash, Edge, NodeHashes, HashOutput, Request, Response, Command, Details,
    CustomHash,
)
from ..interface.utils import format_arguments
from ..utils import node_to_dict
from .base import CallableLayer


class Merge(CallableLayer):
    def __init__(self, *layers: CallableLayer):
        properties = [set(layer._properties) for layer in layers]
        inter = set.intersection(*properties)
        union = set.union(*properties)
        if inter != union:
            raise ValueError(f'All inputs must have the same properties: {properties}')
        properties = inter
        if not properties:
            raise ValueError('The datasets do not contain properties.')
        if len(properties) > 1:
            raise ValueError(f'Can\'t decide which property to use as keys.')
        ids_name, = properties

        id_to_dataset = {}
        for index, dataset in enumerate(layers):
            # TODO: the ids should be computed lazily
            keys = getattr(dataset, ids_name)
            intersection = set(keys) & set(id_to_dataset)
            if intersection:
                raise RuntimeError(f'Ids {intersection} are duplicated in merged datasets.')

            id_to_dataset.update({i: index for i in keys})

        super().__init__(
            self._merge_containers(id_to_dataset, [layer._container for layer in layers], ids_name),
            properties,
        )
        self._layers = layers

    def __repr__(self):
        return 'Merge' + format_arguments(self._layers)

    def _merge_containers(self, id_to_index: dict, containers: Sequence[EdgesBag], keys_name: str):
        details = Details(type(self))
        containers = [container.freeze(details) for container in containers]

        inputs = []
        groups = []
        edges = []
        # gather parts
        for container in containers:
            if len(container.inputs) != 1:
                raise ValueError('Each layer must have exactly one input')
            inputs.append(container.inputs[0])
            groups.append(node_to_dict(container.outputs))
            edges.extend(container.edges)

        # validate inputs
        inp = [x.name for x in inputs]
        if len(set(inp)) != 1:
            raise ValueError(f'Layer inputs must have the same name: {inp}')

        # create the new input
        inp = Node(inp[0], details)
        for node in inputs:
            edges.append(IdentityEdge().bind(inp, node))

        # create new outputs
        outputs = []
        optional = set.union(*(c.optional for c in containers))
        common_outputs = set.intersection(*map(set, groups)) - {keys_name}
        for name in common_outputs:
            node = Node(name, details)
            outputs.append(node)
            branches = [group[name] for group in groups]
            # if all the outputs are optional - make this one too
            if set(branches) <= optional:
                optional.add(node)
            edges.append(SwitchEdge(id_to_index, len(containers)).bind([inp] + branches, node))

        # and the keys
        ids = Node(keys_name, details)
        outputs.append(ids)
        edges.append(ConstantEdge(tuple(sorted(id_to_index))).bind([], ids))

        return EdgesBag(
            [inp], outputs, edges, context=None, virtual=None,
            optional=optional,
            persistent=set.intersection(*(set(c.persistent) for c in containers)),
        )


class SwitchEdge(Edge):
    def __init__(self, id_to_index: dict, n_branches: int):
        super().__init__(arity=1 + n_branches)
        self.id_to_index = id_to_index

    def compute_hash(self) -> Generator[Request, Response, HashOutput]:
        key = yield Command.ParentValue, 0
        try:
            idx = self.id_to_index[key]
        except KeyError:
            raise ValueError(f'Identifier {key!r} not found.') from None

        value = yield Command.ParentHash, idx + 1
        return value, idx

    def evaluate(self) -> Generator[Request, Response, Any]:
        payload = yield Command.Payload,
        value = yield Command.ParentValue, payload + 1
        return value

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return CustomHash('connectome.SwitchEdge', *inputs)
