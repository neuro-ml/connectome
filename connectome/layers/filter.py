from typing import Callable, Sequence, Any

from .base import EdgesBag, Wrapper
from .cache import IdentityContext
from ..engine import NodeHash
from ..engine.base import BoundEdge, Node, TreeNode, Edge
from ..engine.edges import FullMask, FunctionEdge
from ..engine.graph import Graph
from ..engine.node_hash import NodeHashes, FilterHash
from ..utils import extract_signature, node_to_dict


class FilterLayer(Wrapper):
    """
    Changes only the `ids` attribute.
    """

    def __init__(self, predicate: Callable):
        self.names, _ = extract_signature(predicate)
        assert 'ids' not in self.names
        self.predicate = predicate

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

    def wrap(self, layer: EdgesBag) -> EdgesBag:
        main = layer.freeze()
        outputs = list(main.outputs)
        edges = list(main.edges)

        # change ids
        ids = self._find(outputs, 'ids')
        out = Node('ids')
        outputs.remove(ids)
        outputs.append(out)

        # filter
        graph = self._make_graph(layer)
        edges.append(BoundEdge(FilterEdge(graph), [ids], out))
        return EdgesBag(main.inputs, outputs, edges, IdentityContext())


class FilterEdge(FullMask, Edge):
    def __init__(self, graph: Graph):
        super().__init__(arity=1, uses_hash=False)
        self.graph = graph
        self._hash = self.graph.hash()

    def _make_hash(self, hashes):
        keys, = hashes
        return FilterHash(self._hash, keys)

    def _propagate_hash(self, inputs: NodeHashes) -> NodeHash:
        return self._make_hash(inputs)

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return self._make_hash(inputs)

    def _evaluate(self, inputs: Sequence, output: NodeHash, hash_payload: Any, mask_payload: Any) -> Any:
        keys, = inputs
        return tuple(filter(self.graph.call, keys))
