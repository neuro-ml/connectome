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
        new_input = Node('id', details)
        compiler = copy.compile()
        edges.append(CheckIdsEdge(compiler.compile('ids')).bind([new_input, outputs['ids']], inputs['id']))
        inputs['id'] = new_input
        return EdgesBag(inputs.values(), outputs.values(), edges, copy.context,
                        persistent=copy.persistent, optional=copy.optional,
                        virtual=copy.virtual & AntiSet(('ids', 'id')))


class CheckIdsEdge(StaticGraph, StaticEdge):
    def __init__(self, graph: Graph):
        super().__init__(arity=2)
        self.graph = graph
        self._hash = self.graph.hash()

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return CustomHash('connectome.GheckIdsEdge', self._hash, *inputs)

    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        id_, ids = inputs
        if id_ in ids:
            return id_
        raise FieldError(f'{id_} is not in ids')
