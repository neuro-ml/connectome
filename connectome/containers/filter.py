from typing import Callable, Sequence, Any

from .base import EdgesBag, Wrapper
from .cache import IdentityContext
from ..engine.base import Node, TreeNode
from ..engine.edges import FunctionEdge, StaticEdge, StaticGraph
from ..engine.graph import Graph
from ..engine.node_hash import FilterHash
from ..utils import extract_signature, node_to_dict


class FilterContainer(Wrapper):
    """
    Changes only the `keys` attribute.
    """

    # TODO: remove default
    def __init__(self, predicate: Callable, keys: str = 'ids'):
        self.names, _ = extract_signature(predicate)
        assert keys not in self.names
        self.predicate = predicate
        self.keys = keys

    @staticmethod
    def _find(nodes, name):
        for node in nodes:
            if node.name == name:
                return node

        raise ValueError(f'The previous layer must contain the attribute "{name}"')

    def _make_graph(self, layer):
        copy = layer.freeze()
        edges = list(copy.edges)
        outputs_mapping = node_to_dict(copy.outputs)
        out = Node('$predicate')
        edges.append(
            FunctionEdge(self.predicate, len(self.names)).bind([outputs_mapping[name] for name in self.names], out))
        mapping = TreeNode.from_edges(edges)
        return Graph([mapping[copy.inputs[0]]], mapping[out])

    def wrap(self, container: EdgesBag) -> EdgesBag:
        main = container.freeze()
        outputs = list(main.outputs)
        edges = list(main.edges)

        # change ids
        keys = self._find(outputs, self.keys)
        out = Node(self.keys)
        outputs.remove(keys)
        outputs.append(out)

        # filter
        graph = self._make_graph(container)
        edges.append(FilterEdge(graph).bind(keys, out))
        return EdgesBag(main.inputs, outputs, edges, IdentityContext())


class FilterEdge(StaticGraph, StaticEdge):
    def __init__(self, graph: Graph):
        super().__init__(arity=1)
        self.graph = graph
        self._hash = self.graph.hash()

    def _make_hash(self, hashes):
        keys, = hashes
        return FilterHash(self._hash, keys)

    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        keys, = inputs
        return tuple(filter(self.graph.call, keys))
