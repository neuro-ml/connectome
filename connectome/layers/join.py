import hashlib
import itertools
from collections import defaultdict
from enum import Enum
from typing import Any, Callable, Generator, Sequence, Union

from jboc import composed
from tqdm.auto import tqdm

from ..containers import EdgesBag
from ..engine import (
    Request, Response, Command, Node, Edge, TreeNode, StaticGraph, StaticHash, FunctionEdge, ProductEdge, CacheEdge,
    Graph, HashOutput, NodeHashes, Details, IdentityEdge, NodeHash, LeafHash, CustomHash, HashBarrier
)
from ..layers import CallableLayer
from ..utils import Strings, StringsLike, node_to_dict, to_seq
from .cache import MemoryCache


class JoinMode(Enum):
    inner, left, right, outer = 'inner', 'left', 'right', 'outer'


def _maybe_to_hash_id(values):
    if len(values) == 1:
        return values[0]
    return to_hash_id(values)


class Join(CallableLayer):
    def __init__(self, left: CallableLayer, right: CallableLayer, on: StringsLike, *, verbose: bool = False,
                 to_key: Callable = _maybe_to_hash_id, cache: CacheEdge = None,
                 how: Union[JoinMode, str] = JoinMode.inner):
        if isinstance(how, str):
            how = JoinMode[how.lower()]
        if not isinstance(how, JoinMode):
            raise TypeError(f'Unknown join mode: {how}')

        super().__init__(JoinContainer(
            left._container, right._container, to_seq(on), to_key,
            cache=cache, verbose=verbose, how=how,
        ), ['ids'])


class JoinContainer(EdgesBag):
    def __init__(self, left: EdgesBag, right: EdgesBag, on: Strings,
                 combiner: Callable, cache: Edge, verbose: bool, how: JoinMode):
        assert len(set(on)) == len(on), on
        details = Details(type(self))
        left, right = left.freeze(details), right.freeze(details)
        edges = [*left.edges, *right.edges]

        if len(left.inputs) != 1 or len(right.inputs) != 1:
            raise ValueError('Both layers should have exactly one input')
        left_key, = left.inputs
        right_key, = right.inputs

        # TODO: parametrize these names
        keys_name = 'ids'
        key_name = 'id'
        # build the core mapping
        outputs_left, outputs_right = node_to_dict(left.outputs), node_to_dict(right.outputs)
        keys_left, keys_right = outputs_left.pop(keys_name), outputs_right.pop(keys_name)
        outputs_left.pop(left_key.name)
        outputs_right.pop(right_key.name)

        join_on_keys = {left_key.name, right_key.name} & set(on)
        if join_on_keys:
            raise ValueError(f'Join on the kay values os not supported yet: {join_on_keys}')

        intersection = set(outputs_left) & set(outputs_right)
        missing = set(on) - intersection
        if missing:
            raise ValueError(f'Fields {missing} are missing')
        conflict = intersection - set(on)
        if conflict:
            raise ValueError(f'Field conflicts resolution not supported yet. Conflicts: {conflict}')

        # build a key -> (left, right) mapping
        mapping = Node('$mapping', details)
        edges.append(JoinMapping(
            self._make_graph(left_key, outputs_left, left.edges, on, details),
            self._make_graph(right_key, outputs_right, right.edges, on, details),
            combiner, verbose,
        ).bind([keys_left, keys_right], mapping))

        # cache the mapping, if needed
        if cache is not None:
            tmp = mapping
            mapping = Node('$mapping', details)
            edges.append(cache.bind(tmp, mapping))

        tmp = mapping
        mapping = Node('$mapping', details)
        edges.append(CacheEdge(MemoryCache(None)).bind(tmp, mapping))

        # add the new key input and output
        inp = Node(key_name, details)
        key_output = Node(key_name, details)
        edges.extend(_chain_edges(
            [inp, mapping], left_key,
            FunctionEdge(id_maker(0), 2),
            HashBarrier(),
        ))
        edges.extend(_chain_edges(
            [inp, mapping], right_key,
            FunctionEdge(id_maker(1), 2),
            HashBarrier(),
        ))
        edges.append(
            IdentityEdge().bind(inp, key_output)
        )

        outputs = [key_output]
        # add the new keys
        keys = Node(keys_name, details)
        outputs.append(keys)
        edges.append(FunctionEdge(ids_maker(how), 1).bind(mapping, keys))
        # TODO: also add the other properties here

        # in the intersection it doesn't matter left or right, but we might have an outer join which complicates things
        for name in intersection:
            local = Node(name, details)
            edges.append(SwitchBranch().bind([inp, mapping, outputs_left[name], outputs_right[name]], local))
            outputs.append(local)

        left_nodes = [outputs_left[x] for x in set(outputs_left) - intersection]
        right_nodes = [outputs_right[x] for x in set(outputs_right) - intersection]
        if how in [JoinMode.right, JoinMode.outer]:
            for node in left_nodes:
                local = Node(node.name, details)
                edges.append(SwitchMissing(0).bind([inp, mapping, node], local))
                outputs.append(local)
        else:
            outputs.extend(left_nodes)

        if how in [JoinMode.left, JoinMode.outer]:
            for node in right_nodes:
                local = Node(node.name, details)
                edges.append(SwitchMissing(1).bind([inp, mapping, node], local))
                outputs.append(local)
        else:
            outputs.extend(right_nodes)

        super().__init__(
            [inp], outputs, edges, None, persistent=left.persistent & right.persistent,
            optional=left.optional | right.optional, virtual=None,
        )

    @staticmethod
    def _make_graph(key, outputs, edges, on, details):
        edges = list(edges)
        output = Node('$output', details)
        edges.append(ProductEdge(len(on)).bind([outputs[x] for x in on], output))
        mapping = TreeNode.from_edges(edges)
        return Graph([mapping[key]], mapping[output])


class JoinMapping(StaticGraph, StaticHash):
    def __init__(self, left: Graph, right: Graph, to_key, verbose):
        super().__init__(arity=2)
        self.verbose = verbose
        self.to_key = to_key
        self.left = left
        self.right = right
        self._hashes = left.hash(), right.hash()

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return CustomHash('connectome.JoinMapping', LeafHash(self.to_key), *self._hashes, *inputs)

    def evaluate(self) -> Generator[Request, Response, Any]:
        left_keys, right_keys = yield Command.Await, (Command.ParentValue, 0), (Command.ParentValue, 1)
        precomputed_left, precomputed_right = defaultdict(list), defaultdict(list)
        reverse_left, reverse_right = {}, {}
        # TODO: optimize these loops?
        for i in tqdm(left_keys, desc='Computing left join keys', disable=not self.verbose):
            key = reverse_func(self.to_key, self.left(i), reverse_left)
            precomputed_left[key].append(i)

        for i in tqdm(right_keys, desc='Computing right join keys', disable=not self.verbose):
            key = reverse_func(self.to_key, self.right(i), reverse_right)
            precomputed_right[key].append(i)

        left, right = set(precomputed_left), set(precomputed_right)
        common = left & right

        mapping = {}
        for key in common:
            for i, j in itertools.product(precomputed_left[key], precomputed_right[key]):
                mapping[key] = i, j

        return mapping, slice_dict(precomputed_left, left - common), slice_dict(precomputed_right, right - common)


class SwitchBranch(Edge):
    """
    Inputs: key, mappings, left_value, right_value
    """

    def __init__(self):
        super().__init__(arity=4)

    def compute_hash(self) -> Generator[Request, Response, HashOutput]:
        key, (inner, left, right) = yield Command.Await, (Command.ParentValue, 0), (Command.ParentValue, 1)
        if key in inner or key in left:
            index = 2
        elif key in right:
            index = 3
        else:
            raise KeyError(f'The key {key} was not present during the join operation')

        value = yield Command.ParentHash, index
        return value, index

    def evaluate(self) -> Generator[Request, Response, Any]:
        index = yield Command.Payload,
        value = yield Command.ParentValue, index
        return value

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return CustomHash('connectome.SwitchBranch', *inputs)


# todo: this is a special case of switchBarrier now
class SwitchMissing(Edge):
    """
    Inputs: key, mappings, value
    """

    def __init__(self, index: int):
        super().__init__(arity=3)
        self.index = index

    def compute_hash(self) -> Generator[Request, Response, HashOutput]:
        key, (inner, *rest) = yield Command.Await, (Command.ParentValue, 0), (Command.ParentValue, 1)
        this, other = rest[self.index], rest[1 - self.index]
        if key in inner or key in this:
            value = yield Command.ParentHash, 2
            return value, True

        if key in other:
            return LeafHash(None), False

        raise KeyError(f'The key {key} was not present during the join operation')

    def evaluate(self) -> Generator[Request, Response, Any]:
        present = yield Command.Payload,
        if present:
            value = yield Command.ParentValue, 2
        else:
            value = None
        return value

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return CustomHash('connectome.SwitchMissing', LeafHash(self.index), *inputs)


@composed(dict)
def slice_dict(d, keys):
    for k in keys:
        vs = d[k]
        if len(vs) > 1:
            raise ValueError(f'Multiple ids {tuple(vs)} were mapped to the same key "{k}"')

        yield k, vs[0]


def reverse_func(func, arg, mapping):
    value = func(arg)
    if value in mapping:
        raise ValueError(
            f'The provided key function is not reversible: value {value} already present for {mapping[value]}'
        )

    mapping[value] = arg
    return value


def ids_maker(how):
    def ids(mappings):
        inner, left, right = mappings
        result = set(inner)
        if how in [JoinMode.left, JoinMode.outer]:
            result |= set(left)
        if how in [JoinMode.right, JoinMode.outer]:
            result |= set(right)

        return tuple(sorted(result))

    return ids


def id_maker(index):
    def key(i, mappings):
        inner, *rest = mappings
        if i in inner:
            return inner[i][index]
        if i in rest[index]:
            return rest[index][i]

        raise KeyError(f'Key "{i}" not found')

    return key


def to_hash_id(values: Sequence[str]):
    algo = hashlib.sha256()
    for value in values:
        algo.update(hashlib.sha256(value.encode()).digest())

    return algo.hexdigest()


def _chain_edges(inputs, output, *edges):
    tmp = inputs
    for idx, edge in enumerate(edges, 1):
        inputs = tmp
        tmp = output if idx == len(edges) else Node('$aux')
        yield edge.bind(inputs, tmp)
