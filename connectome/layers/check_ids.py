from typing import Any, Sequence

from connectome.containers import EdgesBag

from ..containers import BagContext
from ..engine import Details, Graph, Node, StaticEdge, StaticGraph
from ..engine.node_hash import CustomHash, NodeHash, NodeHashes
from ..exceptions import FieldError
from ..utils import AntiSet, node_to_dict
from .base import EdgesBag, Layer


class CheckIds(Layer):
    """
    Raise FieldError if id is not in ids
    """

    def __init__(self, verbose: bool = False):
        self.verbose = verbose

    def __repr__(self):
        return 'CheckIds()'

    def _connect(self, previous: EdgesBag) -> EdgesBag:
        copy = previous.freeze()
        details = Details(type(self))
        edges, inputs, outputs = list(copy.edges), node_to_dict(copy.inputs), node_to_dict(copy.outputs)
        assert len(inputs) == 1, 'Only one input is allowed'
        key = list(inputs.keys())[0]
        new_input = Node('id', details)
        edges.append(CheckIdsEdge().bind([new_input, outputs['ids']], inputs[key]))
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
        raise FieldError(f'{id_} is not in ids')
