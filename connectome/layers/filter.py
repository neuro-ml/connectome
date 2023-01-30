from typing import Callable, Sequence, Any, Iterable

from tqdm.auto import tqdm

from .base import EdgesBag, Layer
from ..containers import IdentityContext
from ..engine import Node, TreeNode, Graph, FilterHash, FunctionEdge, StaticEdge, StaticGraph, Details
from ..utils import extract_signature, node_to_dict


class Filter(Layer):
    """
    Filters the `ids` of the current pipeline given a ``predicate``.

    Examples
    --------
    >>> dataset = Chain(
    ...   source,  # dataset with `image` and `spacing` attributes
    ...   Filter(lambda image, spacing: min(image.shape) > 30 and max(spacing) < 5),
    ... )
    """

    def __init__(self, predicate: Callable, verbose: bool = False):
        # TODO: remove default
        self._keys: str = 'ids'
        self._names, _ = extract_signature(predicate)
        self.predicate = predicate
        self.verbose = verbose
        assert self._keys not in self._names

    @classmethod
    def drop(cls, ids: Iterable[str], verbose: bool = False):
        """Removes the provided ``ids`` from the dataset."""
        assert not isinstance(ids, str)
        ids = tuple(sorted(set(ids)))
        assert all(isinstance(i, str) for i in ids)
        return cls(lambda id: id not in ids, verbose=verbose)

    @classmethod
    def keep(cls, ids: Iterable[str], verbose: bool = False):
        """Removes all the ids not present in ``ids``."""
        assert not isinstance(ids, str)
        ids = tuple(sorted(set(ids)))
        assert all(isinstance(i, str) for i in ids)
        return cls(lambda id: id in ids, verbose=verbose)

    def __repr__(self):
        args = ', '.join(self._names)
        return f'Filter({args})'

    def _make_graph(self, layer, details):
        copy = layer.freeze(details)
        edges = list(copy.edges)
        outputs_mapping = node_to_dict(copy.outputs)
        out = Node('$predicate', details)
        edges.append(
            FunctionEdge(self.predicate, len(self._names)).bind([outputs_mapping[name] for name in self._names], out))
        mapping = TreeNode.from_edges(edges)
        return Graph([mapping[copy.inputs[0]]], mapping[out])

    def _connect(self, previous: EdgesBag) -> EdgesBag:
        details = Details(type(self))
        main = previous.freeze(details)
        outputs = list(main.outputs)
        edges = list(main.edges)

        # change ids
        keys = _find(outputs, self._keys)
        out = Node(self._keys, details)
        outputs.remove(keys)
        outputs.append(out)

        # filter
        graph = self._make_graph(previous, details)
        edges.append(FilterEdge(graph, self.verbose).bind(keys, out))
        return EdgesBag(
            main.inputs, outputs, edges, IdentityContext(), persistent_nodes=main.persistent_nodes,
            optional_nodes=main.optional_nodes, virtual_nodes=main.virtual_nodes,
        )


class FilterEdge(StaticGraph, StaticEdge):
    def __init__(self, graph: Graph, verbose: bool):
        super().__init__(arity=1)
        self.verbose = verbose
        self.graph = graph
        self._hash = self.graph.hash()

    def _make_hash(self, hashes):
        keys, = hashes
        return FilterHash(self._hash, keys)

    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        keys, = inputs
        return tuple(filter(self.graph.call, tqdm(
            keys, desc='Filtering', disable=not self.verbose,
        )))


def _find(nodes, name):
    for node in nodes:
        if node.name == name:
            return node

    raise ValueError(f'The previous layer must contain the attribute "{name}"')
