import inspect
from collections import defaultdict
from typing import Sequence, Union

from .base import TreeNode, NodeHash
from .utils import ExpirationCache


def compile_graph(inputs: Sequence[TreeNode], outputs: Union[TreeNode, Sequence[TreeNode]], use_hash=True):
    squeeze = isinstance(outputs, TreeNode)
    if squeeze:
        outputs = [outputs]

    validate_graph(inputs, outputs)
    counts = count_entries(inputs, outputs)
    inputs = [x for x in inputs if counts.get(x, 0)]
    inputs_map = {x.name: x for x in inputs}
    signature = inspect.Signature([
        inspect.Parameter(name, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        for name in inputs_map
    ])

    def caller(*args, **kwargs):
        scope = signature.bind(*args, **kwargs)
        # drop unnecessary branches
        hashes, masks = prune(inputs_map, outputs, scope.arguments, use_hash=use_hash)
        # prepare for render
        local_counts = counts.copy()
        hashes = ExpirationCache(local_counts, hashes)

        local_counts = count_entries(inputs, outputs, masks)
        cache = ExpirationCache(local_counts)

        for name, n in inputs_map.items():
            if n in local_counts:
                cache[n] = scope.arguments[name]

        # render
        result = tuple(render(node, cache, masks, hashes) for node in outputs)
        if squeeze:
            result = result[0]
        return result

    caller.__signature__ = signature
    return caller


def validate_graph(inputs, outputs):
    def visitor(nodes):
        for node in nodes:
            # input doesn't need parents
            if node in inputs:
                continue

            # no edges - must be an input
            if not node.edge:
                assert node in inputs, (node, inputs)

            else:
                group = node.edge[1]
                visitor(group)

    visitor(outputs)


def count_entries(inputs: Sequence[TreeNode], outputs: Sequence[TreeNode], masks=None):
    def visitor(node: TreeNode):
        entry_counts[node] += 1
        # input doesn't need parents
        if node in inputs:
            return

        group = node.edge[1]
        if masks is not None:
            group = [group[idx] for idx in masks[node]]

        for n in group:
            visitor(n)

    entry_counts = defaultdict(int)
    for x in outputs:
        visitor(x)
    return dict(entry_counts)


def precompute_hashes(inputs, outputs):
    def visitor(node: TreeNode):
        if node in hashes:
            return True

        if not node.edge:
            assert node in inputs
            return False

        # we visit the root nodes and build a cache of immutable hashes
        edge, group = node.edge
        visited = all(visitor(x) for x in group)

        if edge.uses_hash or not visited:
            # the edge doesn't have a constant hash
            return False

        hashes[node], masks[node] = edge.process_hashes([hashes[x] for x in group])
        return True

    hashes, masks = {}, {}
    for n in outputs:
        visitor(n)
    return hashes, masks


class LazyHashes:
    def __init__(self, current_nodes, hashes, masks):
        self.current_nodes = current_nodes
        self.hashes = hashes
        self.masks = masks

    def sync(self, index):
        return self[index]

    def _render(self, node):
        if node not in self.hashes:
            edge, group = node.edge
            self.hashes[node], self.masks[node] = edge.process_hashes(LazyHashes(group, self.hashes, self.masks))

        return self.hashes[node]

    def __getitem__(self, index):
        return self._render(self.current_nodes[index])

    def __iter__(self):
        return map(self._render, self.current_nodes)

    def __len__(self):
        return len(self.current_nodes)


def prune(inputs_map, outputs, arguments, use_hash=True):
    def visitor(node: TreeNode):
        # if node in hashes:
        #     return hashes[node]

        edge, group = node.edge
        hashes[node], masks[node] = edge.process_hashes(LazyHashes(group, hashes, masks))
        # result, mask = edge.process_hashes([visitor(x) for x in group])
        # hashes[node] = result
        # masks[node] = mask
        # return result

    hashes, masks = {}, {}
    for name, n in inputs_map.items():
        # put objects into inputs if hashes are not required
        hash_data = arguments[name] if use_hash else object()
        hashes[n] = NodeHash.from_leaf(hash_data)
    for n in outputs:
        visitor(n)

    return hashes, masks


def render(node, cache, masks, hashes):
    if node not in cache:
        edge, inputs = node.edge
        mask = masks[node]

        inputs = [inputs[idx] for idx in mask]
        cache[node] = edge.evaluate([render(x, cache, masks, hashes) for x in inputs], mask, hashes[node])

    return cache[node]
