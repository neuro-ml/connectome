from typing import Callable, Iterable, Sequence, Tuple

from tqdm.auto import tqdm

from ..containers import BagContext
from ..engine import Details, FunctionEdge, Graph, Node, StaticEdge, StaticGraph, TreeNode
from ..engine.node_hash import ApplyHash
from ..exceptions import DependencyError
from ..utils import AntiSet, extract_signature, node_to_dict
from .base import EdgesBag
from .dynamic import DynamicConnectLayer


class Filter(DynamicConnectLayer):
    """
    Filters the `keys` of the current pipeline given a `predicate`.

    Examples
    --------
    >>> dataset = Chain(
    ...   source,  # dataset with `image` and `spacing` attributes
    ...   Filter(lambda image, spacing: min(image.shape) > 30 and max(spacing) < 5),
    ... )
    """

    def __init__(self, predicate: Callable, verbose: bool = False, keys: str = 'ids'):
        self._keys = keys
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
        missing = set(self._names) - set(outputs_mapping)
        if missing:
            raise DependencyError(
                f'The previous layer is missing the fields {missing}, which are required by the predicate'
            )

        out = Node('$predicate', details)
        edges.append(
            FunctionEdge(self.predicate, len(self._names)).bind([outputs_mapping[name] for name in self._names], out)
        )
        mapping = TreeNode.from_edges(edges)
        return Graph([mapping[copy.inputs[0]]], mapping[out])

    def _prepare_container(self, previous: EdgesBag) -> EdgesBag:
        details = Details(type(self))
        inp = Node(self._keys, details)
        out = Node(self._keys, details)

        # filter
        graph = self._make_graph(previous, details)
        edge = FilterEdge(graph, self.verbose).bind(inp, out)
        return EdgesBag(
            [inp], [out], [edge], BagContext((), (), AntiSet((self._keys,))),
            persistent=None, optional=None, virtual=AntiSet((self._keys,)),
        )


class FilterEdge(StaticGraph, StaticEdge):
    def __init__(self, graph: Graph, verbose: bool):
        super().__init__(arity=1)
        self.verbose = verbose
        self.graph = graph
        self._hash = self.graph.hash()

    def _make_hash(self, hashes):
        keys, = hashes
        return ApplyHash(filter, self._hash, keys)

    def _evaluate(self, inputs: Sequence) -> Tuple:
        keys, = inputs
        return tuple(filter(self.graph, tqdm(
            keys, desc='Filtering', disable=not self.verbose,
        )))
