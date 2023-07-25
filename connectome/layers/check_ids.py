from typing import Any, Sequence

from ..containers import EdgesBag
from ..engine import Details, Node, StaticEdge, StaticGraph
from ..engine.node_hash import NodeHash, NodeHashes
from ..utils import node_to_dict
from .base import EdgesBag, Layer


class CheckIds(Layer):
    """
    Raise FieldError if id is not in ids
    """
    def __repr__(self):
        return 'CheckIds()'

    def _connect(self, previous: EdgesBag) -> EdgesBag:
        copy = previous.freeze()
        details = Details(type(self))
        edges, inputs, outputs = list(copy.edges), copy.inputs, node_to_dict(copy.outputs)
        assert len(inputs) == 1, 'Only one input is allowed'
        new_input = Node(inputs[0].name, details)
        edges.append(CheckIdsEdge().bind([new_input, outputs['ids']], inputs[0]))
        return EdgesBag([new_input], outputs.values(), edges, copy.context,
                        persistent=copy.persistent, optional=copy.optional,
                        virtual=copy.virtual)


class CheckIdsEdge(StaticGraph, StaticEdge):
    def __init__(self):
        super().__init__(arity=2)

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]

    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        id_, ids = inputs
        if id_ in ids:
            return id_
        raise KeyError(f'{id_} is not in ids')
