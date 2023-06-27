from typing import Union, Collection, Callable, Iterable, Generator, Any

from .base import Layer
from .chain import connect
from ..cache import MemoryCache
from ..containers import EdgesBag
from ..engine import (
    CustomHash, NodeHashes, NodeHash, Response, Request, Command, StaticGraph, Graph,
    StaticHash, IdentityEdge, FunctionEdge, HashBarrier, Node, CacheEdge, Details, TreeNode
)
from ..exceptions import DependencyError
from ..interface.factory import TransformFactory
from ..interface.metaclasses import APIMeta
from ..utils import extract_signature, AntiSet, node_to_dict


class SplitBase(Layer):
    def __init__(self, split: Callable, transform: EdgesBag):
        self._transform = transform
        self._split = split
        self._split_names, _ = extract_signature(split)
        self._part_name = '__part__'
        self._key, self._keys = 'id', 'ids'

    def _make_graph(self, container: EdgesBag, details):
        edges = list(container.edges)
        outputs_mapping = node_to_dict(container.outputs)
        missing = set(self._split_names) - set(outputs_mapping)
        if missing:
            raise DependencyError(
                f'The previous layer is missing the fields {missing}, which are required by the predicate'
            )

        out = Node('__split__pairs__', details)
        edges.append(FunctionEdge(self._split, len(self._split_names)).bind(
            [outputs_mapping[name] for name in self._split_names], out
        ))
        mapping = TreeNode.from_edges(edges)
        return Graph([mapping[container.inputs[0]]], mapping[out])

    def _connect(self, previous: EdgesBag) -> EdgesBag:
        details = Details(type(self))
        previous = previous.freeze(details)

        key = Node(self._key, details)
        keys = Node(self._keys, details)
        key_output = Node(self._key, details)
        key_final = Node(self._key, details)
        edges = [IdentityEdge().bind(key, key_final)]

        previous_outputs = node_to_dict(previous.outputs)
        assert self._key in previous_outputs
        assert self._keys in previous_outputs
        old_keys = previous_outputs[self._keys]

        # build a key -> (left, right) mapping
        mapping = Node('$mapping', details)
        edges.append(SplitMapping(self._make_graph(previous, details)).bind(old_keys, mapping))

        tmp = mapping
        mapping = Node('$mapping', details)
        edges.append(CacheEdge(MemoryCache(None)).bind(tmp, mapping))

        # new keys
        edges.append(FunctionEdge(lambda m: tuple(sorted(m)), 1).bind(mapping, keys))
        # new id to old id
        edges.extend(chain_edges(
            [key, mapping], key_output,
            FunctionEdge(lambda i, m: m[i][0], 2),
            HashBarrier(),
        ))
        # old id part
        part = Node(self._part_name, details)
        edges.append(FunctionEdge(lambda i, m: m[i][1], 2).bind([key, mapping], part))

        return connect(
            # change the key
            EdgesBag(
                [key], [key_output], edges, None, virtual=AntiSet((key_output.name,)),
            ),
            # base layer
            previous,
            # add the __part__
            EdgesBag(
                [], [part], [], None, virtual=AntiSet((part.name,)),
            ),
            # add the transform
            self._transform.freeze(),
            # finally add the new `key` and `keys`
            EdgesBag(
                [], [key_final, keys], [], None, virtual=AntiSet((key_final.name, keys.name, part.name)),
            ),
            freeze=False,
        )


class SplitFactory(TransformFactory):
    _part_name = '__part__'
    _split_name = '__split__'
    layer_cls = SplitBase

    def __init__(self, layer: str, scope):
        self._split: Callable = None
        super().__init__(layer, scope)

    def _prepare_layer_arguments(self, container: EdgesBag, properties: Iterable[str]):
        assert not properties, properties
        return self._split, container

    def _before_collect(self):
        super()._before_collect()
        self.edges.append(IdentityEdge().bind(self.inputs[self._part_name], self.parameters[self._part_name]))
        self.magic_dispatch[self._split_name] = self._handle_split

    def _handle_split(self, value):
        assert self._split is None, self._split
        self._split = value

    def _after_collect(self):
        super()._after_collect()
        assert self._split is not None
        assert not self.special_methods, self.special_methods


# TODO: Examples
class Split(SplitBase, metaclass=APIMeta, __factory=SplitFactory):
    """
    Split a dataset entries into several parts.

    This layer requires a `__split__` magic method, which takes an entry id, and returns a list of parts -
    (part_id, part_context) pairs, "part_id" will become the part's id, and "part_context" is accessible in other
    methods as part-specific useful info.
    """

    __inherit__: Union[str, Collection[str], bool] = ()
    __exclude__: Union[str, Collection[str]] = ()

    def __split__(*args, **kwargs):
        raise NotImplementedError


class SplitMapping(StaticGraph, StaticHash):
    def __init__(self, graph: Graph):
        super().__init__(arity=1)
        self.graph = graph
        self._hash = graph.hash()

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return CustomHash('connectome.Split', self._hash, *inputs)

    def evaluate(self) -> Generator[Request, Response, Any]:
        keys = yield Command.ParentValue, 0
        mapping = {}
        for key in keys:
            for new, part in self.graph(key):
                assert new not in mapping, new
                mapping[new] = key, part

        return mapping
