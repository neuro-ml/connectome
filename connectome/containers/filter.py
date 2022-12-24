from typing import Callable

from .base import EdgesBag, Container
from .cache import IdentityContext
from ..engine.base import Node, TreeNode
from ..engine.edges import FunctionEdge
from ..engine.graph import Graph
from ..layers.filter import FilterEdge
from ..utils import extract_signature, node_to_dict


class FilterContainer(Container):
    """
    Changes only the `keys` attribute.
    """

    # TODO: remove default
    def __init__(self, predicate: Callable, verbose: bool, keys: str = 'ids'):
        self.names, _ = extract_signature(predicate)
        assert keys not in self.names
        self.predicate = predicate
        self.verbose = verbose
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
        edges.append(FilterEdge(graph, self.verbose).bind(keys, out))
        return EdgesBag(main.inputs, outputs, edges, IdentityContext(), persistent_nodes=main.persistent_nodes)

