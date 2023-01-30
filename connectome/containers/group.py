from collections import defaultdict
from hashlib import sha256
from typing import Sequence, Any, Generator

from .base import EdgesBag, Container
from ..engine import NodeHash
from ..engine import Node, TreeNode, NodeHashes, Command, Request, Response, Details
from ..engine.edges import FunctionEdge, ProductEdge, StaticHash, StaticGraph, StaticEdge
from ..engine.graph import Graph
from ..engine.node_hash import LeafHash, GroupByHash, DictFromKeys, MultiMappingHash


class GroupContainer(Container):
    def __init__(self, name: str):
        self.name = name

    @staticmethod
    def _find(nodes, name):
        for node in nodes:
            if node.name == name:
                return node

        raise ValueError(f'The previous layer must contain the attribute "{name}"')

    def wrap(self, container: EdgesBag) -> EdgesBag:
        parent = Details(type(self))
        main = container.freeze(parent)

        inp, = main.inputs
        edges = list(main.edges)
        outputs = []
        mapping = TreeNode.from_edges(edges)
        changed_input = Node('id', parent)
        mapping_node = Node('$mapping', parent)
        ids_node = self._find(main.outputs, 'ids')
        outputs.append(changed_input)

        # create a mapping: {new_id: [old_ids]}
        edges.append(MappingEdge(Graph([mapping[inp]], mapping[self._find(main.outputs, self.name)])).bind(
            [ids_node], mapping_node))

        # evaluate each output
        for node in main.outputs:
            if node.name in [self.name, 'ids', 'id']:
                continue

            output = Node(node.name, parent)
            outputs.append(output)
            edges.append(GroupEdge(Graph([mapping[inp]], mapping[node])).bind(
                [changed_input, mapping_node], output))

        if len(outputs) == 1:
            raise RuntimeError('Previous layer must contain at least 2 fields in order to perform a GroupBy operation')

        # update ids
        output_ids = Node('ids', parent)
        outputs.append(output_ids)
        edges.append(FunctionEdge(extract_keys, arity=1).bind(mapping_node, output_ids))

        return EdgesBag(
            [changed_input], outputs, edges, None, persistent_nodes=main.persistent_nodes,
            optional_nodes=main.optional_nodes, virtual_nodes=None,
        )


class MappingEdge(StaticGraph, StaticHash):
    """ Groups the incoming values using `graph` as a key function."""

    def __init__(self, graph):
        super().__init__(arity=1)
        self.graph = graph
        # TODO: this is potentially dangerous. should use a composition of Mapping and MemoryCache
        self._mapping = None
        self._hash = self.graph.hash()

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return GroupByHash(self._hash, *inputs)

    def evaluate(self) -> Generator[Request, Response, Any]:
        if self._mapping is not None:
            return self._mapping

        values = yield Command.ParentValue, 0
        mapping = defaultdict(list)
        for i in values:
            mapping[self.graph.call(i)].append(i)

        self._mapping = mapping = {k: tuple(sorted(v)) for k, v in mapping.items()}
        return mapping


class GroupEdge(StaticGraph, StaticEdge):
    def __init__(self, graph):
        super().__init__(arity=2)
        self.graph = graph
        self._hash = self.graph.hash()

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return DictFromKeys(self._hash, *inputs)

    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        """ arguments: id, mapping """
        # get the required ids
        key, mapping = inputs

        result = {}
        for i in mapping[key]:
            result[i] = self.graph.call(i)

        return result


def extract_keys(d):
    return tuple(sorted(d))


# prototype for multiple groupby


class MultiGroupLayer(Container):
    def __init__(self, comparators: dict):
        self.names = sorted(comparators)
        self.comparators = [comparators[x] for x in self.names]

    @staticmethod
    def _find(nodes, name):
        for node in nodes:
            if node.name == name:
                return node

        raise ValueError(f'The previous layer must contain the attribute "{name}"')

    def wrap(self, container: EdgesBag) -> EdgesBag:
        parent = Details(type(self))
        main = container.freeze(parent)

        inp, = main.inputs
        edges = list(main.edges)
        graph_outputs, group_outputs = [], []

        for node in main.outputs:
            if node.name not in {'id', 'ids'}:
                if node.name in self.names:
                    graph_outputs.append(node)
                else:
                    group_outputs.append(node)

        graph_outputs = sorted(graph_outputs, key=lambda x: x.name)
        assert [x.name for x in graph_outputs] == self.names

        # create a mapping: {new_id: [old_ids]}
        graph_output = Node('$product', parent)
        mapping_node = Node('$mapping', parent)
        ids_node = self._find(main.outputs, 'ids')
        edges.append(ProductEdge(len(graph_outputs)).bind(graph_outputs, graph_output))
        mapping = TreeNode.from_edges(edges)

        edges.append(HashMappingEdge(Graph([mapping[inp]], mapping[graph_output]), self.comparators).bind(
            [ids_node], mapping_node))

        # evaluate each output
        changed_input = Node('id', parent)
        outputs = [changed_input]
        for node in group_outputs:
            output = Node(node.name, parent)
            outputs.append(output)
            edges.append(GroupEdge(Graph([mapping[inp]], mapping[node])).bind(
                [changed_input, mapping_node], output))

        # update ids
        output_ids = Node('ids', parent)
        outputs.append(output_ids)
        edges.append(FunctionEdge(extract_keys, arity=1).bind(mapping_node, output_ids))

        return EdgesBag(
            [changed_input], outputs, edges, None, persistent_nodes=main.persistent_nodes,
            optional_nodes=main.optional_nodes, virtual_nodes=None,
        )


class HashMappingEdge(StaticGraph, StaticHash):
    def __init__(self, graph, comparators):
        super().__init__(arity=1)
        self.graph = graph
        self._mapping = None
        self.comparators = comparators
        self.hasher = sha256
        self._graph_hash = self.graph.hash()

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return MultiMappingHash(
            *inputs, *(LeafHash(x) for x in self.comparators), LeafHash(self.hasher),
            self._graph_hash,
        )

    def evaluate(self) -> Generator[Request, Response, Any]:
        if self._mapping is not None:
            return self._mapping

        ids = yield Command.ParentValue, 0
        groups = []
        for i in ids:
            keys = self.graph.call(i)
            assert len(keys) == len(self.comparators)
            # either find a group
            for entry, container in groups:
                if all(cmp(x, y) for cmp, x, y in zip(self.comparators, entry, keys)):
                    container.append(i)
                    break
            # or create a new one
            else:
                groups.append((keys, [i]))

        mapping = {}
        for _, ids in groups:
            ids = tuple(sorted(ids))
            # double hashing lets us get rid of separators
            hashes = b''.join(self.hasher(i.encode()).digest() for i in ids)
            mapping[self.hasher(hashes).hexdigest()] = ids

        assert len(mapping) == len(groups)
        self._mapping = mapping
        return mapping
