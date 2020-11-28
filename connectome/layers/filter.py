from typing import Callable

from .base import EdgesBag, Wrapper
from ..engine.base import BoundEdge, Node, TreeNode
from ..engine.edges import FilterEdge, ProductEdge
from ..engine.graph import Graph
from ..utils import extract_signature, node_to_dict


class FilterLayer(Wrapper):
    """
    Changes only the `ids` attribute.
    """

    def __init__(self, predicate: Callable):
        self.names = extract_signature(predicate)
        assert 'ids' not in self.names
        self.predicate = predicate

    @staticmethod
    def _find(nodes, name):
        for node in nodes:
            if node.name == name:
                return node

        raise ValueError(f'The previous layer must contain the attribute "{name}"')

    def _make_graph(self, layer):
        copy = layer.prepare()
        edges = list(copy.edges)
        outputs_mapping = node_to_dict(copy.outputs)
        out = Node('args')
        edges.append(BoundEdge(
            ProductEdge(len(self.names)),
            [outputs_mapping[name] for name in self.names], out
        ))
        mapping = TreeNode.from_edges(edges)
        return Graph([mapping[copy.inputs[0]]], mapping[out])

    def wrap(self, layer: EdgesBag) -> EdgesBag:
        main = layer.prepare()
        outputs = list(main.outputs)
        edges = list(main.edges)

        # change ids
        ids = self._find(outputs, 'ids')
        out = Node('ids')
        outputs.remove(ids)
        outputs.append(out)

        # filter
        graph = self._make_graph(layer)
        edges.append(BoundEdge(FilterEdge(self.predicate, graph), [ids], out))
        return EdgesBag(main.inputs, outputs, edges, [], [])
