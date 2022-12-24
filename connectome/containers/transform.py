from .base import EdgesBag, NameSet, BagContext
from ..engine.base import Nodes, BoundEdges
from ..interface.factory import normalize_inherit
from ..layers.chain import connect, ChainContext
from ..utils import check_for_duplicates, node_to_dict, deprecation_warn


class TransformContainer(EdgesBag):  # pragma: no cover
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, backward_inputs: Nodes = (),
                 backward_outputs: Nodes = (), *, optional_nodes: NameSet = None,
                 forward_virtual: NameSet, backward_virtual: NameSet, persistent_nodes: NameSet = None):
        deprecation_warn()
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

    def wrap(self, container: 'EdgesBag') -> 'EdgesBag':
        return connect(container, self)


PipelineContext = ChainContext
