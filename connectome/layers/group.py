from collections import defaultdict

from typing import Callable, Union, Sequence, Any


from ..cache import MemoryCache
from ..containers import EdgesBag
from ..engine import (
    Details, Node, FunctionEdge, StaticGraph, StaticHash, Graph, NodeHashes, NodeHash, CustomHash, StaticEdge, CacheEdge
)
from ..engine import EvalGen, Command
from ..utils import node_to_dict, extract_signature
from .chain import connect
from .dynamic import DynamicConnectLayer
from .join import to_hash_id


class GroupBy(DynamicConnectLayer):
    def __init__(self, by: Union[str, Sequence[str], Callable]):
        self.by = by

    def _by_layer(self, details):
        output = Node('$by', details)
        if isinstance(self.by, str):
            inputs = [Node(self.by, details)]
            edges = [FunctionEdge(to_key, len(inputs)).bind(inputs, output)]
        elif callable(self.by):
            inputs, _ = extract_signature(self.by)
            inputs = [Node(x, details) for x in inputs]
            func = Node('$func', details)
            edges = [
                FunctionEdge(self.by, len(inputs)).bind(inputs, func),
                FunctionEdge(to_key, 1).bind(func, output),
            ]
        else:
            inputs = [Node(x, details) for x in self.by]
            edges = [FunctionEdge(to_key, len(inputs)).bind(inputs, output)]

        return EdgesBag(inputs, [output], edges, None)

    def _prepare_container(self, previous: EdgesBag) -> EdgesBag:
        details = Details(type(self))
        main = previous.freeze(details)

        assert len(main.inputs) == 1, main.inputs
        edges = list(main.edges)
        outputs = []
        key_name, keys_name = 'id', 'ids'
        changed_input = Node(key_name, details)
        mapping_node = Node('$mapping', details)
        output_nodes = node_to_dict(main.outputs)
        assert keys_name in output_nodes
        keys = output_nodes[keys_name]
        outputs.append(changed_input)

        # create a mapping: {new_id: [old_ids]} and store it in memory
        by_layer = self._by_layer(details)
        raw_mapping = Node('$mapping', details)
        edges.append(GroupMapping(
            connect(main, by_layer).compile().compile('$by')
        ).bind(keys, raw_mapping))
        edges.append(CacheEdge(MemoryCache(None)).bind(raw_mapping, mapping_node))

        compiler = main.compile()
        # evaluate each output
        for node in main.outputs:
            if node.name in [keys_name, key_name]:
                continue

            output = Node(node.name, details)
            outputs.append(output)
            edges.append(GroupEdge(compiler.compile(node.name)).bind([changed_input, mapping_node], output))

        if len(outputs) == 1:
            raise RuntimeError('Previous layer must contain at least 2 fields in order to perform a GroupBy operation')

        # update ids
        output_ids = Node(keys_name, details)
        outputs.append(output_ids)
        edges.append(FunctionEdge(lambda x: tuple(sorted(x)), arity=1).bind(mapping_node, output_ids))

        return EdgesBag(
            [changed_input], outputs, edges, None, persistent=main.persistent,
            optional=main.optional, virtual=None,
        )


class GroupEdge(StaticGraph, StaticEdge):
    def __init__(self, graph):
        super().__init__(arity=2)
        self.graph = graph
        self._hash = self.graph.hash()

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return CustomHash('connectome.GroupEdge', self._hash, *inputs)

    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        """ arguments: id, mapping """
        # get the required ids
        new_key, mapping = inputs
        if new_key not in mapping:
            raise KeyError(f'The key {new_key} is not found')
        return {old_key: self.graph(old_key) for old_key in sorted(mapping[new_key])}


class GroupMapping(StaticGraph, StaticHash):
    def __init__(self, graph: Graph):
        super().__init__(arity=1)
        self.graph = graph
        self._hash = graph.hash()

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return CustomHash('connectome.GroupMapping', self._hash, *inputs)

    def evaluate(self) -> EvalGen:
        keys = yield Command.ParentValue, 0
        mapping = defaultdict(set)
        for key in keys:
            new = self.graph(key)
            assert key not in mapping[new], (key, mapping[new])
            mapping[new].add(key)

        return dict(mapping)


def to_key(*args) -> str:
    assert args
    if len(args) > 1:
        return to_hash_id(list(map(to_key, args)))
    x, = args
    if isinstance(x, str):
        return x
    if isinstance(x, (list, tuple)):
        return to_key(*x)

    raise TypeError(f"Can't convert an object of type {type(x).__name__!r} to key")
