import inspect
from collections import defaultdict
from typing import Sequence, Dict

from .base import TreeNode, NodeHash, TreeNodes
from .utils import ExpirationCache


class Graph:
    def __init__(self, inputs: TreeNodes, output: TreeNode):
        validate_graph(inputs, output)
        counts = count_entries(inputs, output)
        inputs = [x for x in inputs if counts.get(x, 0)]
        inputs_map = {x.name: x for x in inputs}
        signature = inspect.Signature([
            inspect.Parameter(name, inspect.Parameter.POSITIONAL_OR_KEYWORD)
            for name in inputs_map
        ])
        use_hash = uses_hash(output)
        self.inputs = inputs
        self.output = output

        def caller(*args, **kwargs):
            scope = signature.bind(*args, **kwargs)
            # put objects into inputs if hashes are not required
            input_hashes = {
                node: NodeHash.from_leaf(scope.arguments[name] if use_hash else object())
                for name, node in inputs_map.items()
            }
            # drop unnecessary branches
            hashes, masks = prune(input_hashes, output)
            # prepare for render
            local_counts = counts.copy()
            hashes = ExpirationCache(local_counts, hashes)

            local_counts = count_entries(inputs, output, masks)
            cache = ExpirationCache(local_counts)

            for name, n in inputs_map.items():
                if n in local_counts:
                    cache[n] = scope.arguments[name]

            return render(output, cache, masks, hashes)

        caller.__signature__ = signature
        self.eval = caller

    def eval_hash(self, *hashes: NodeHash):
        assert len(hashes) == len(self.inputs)
        hashes, masks = prune(dict(zip(self.inputs, hashes)), self.output)
        return hashes[self.output]

    def hash(self):
        return hash_graph(self.inputs, self.output)


# TODO: deprecate?
def compile_graph(inputs: Sequence[TreeNode], outputs: TreeNode):
    return Graph(inputs, outputs).eval


def uses_hash(node: TreeNode) -> bool:
    if node.edge is None:
        return False
    return node.edge[0].uses_hash or any(map(uses_hash, node.edge[1]))


def validate_graph(inputs: TreeNodes, output: TreeNode):
    def visitor(node):
        # input doesn't need parents
        if node in inputs:
            return
        # no edges - must be an input
        if not node.edge:
            assert node in inputs, (node, inputs)
        else:
            for inp in node.edge[1]:
                visitor(inp)

    visitor(output)


def count_entries(inputs: TreeNodes, output: TreeNode, masks=None):
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
    visitor(output)
    return dict(entry_counts)


# def precompute_hashes(inputs, outputs):
#     def visitor(node: TreeNode):
#         if node in hashes:
#             return True
#
#         if not node.edge:
#             assert node in inputs
#             return False
#
#         # we visit the root nodes and build a cache of immutable hashes
#         edge, group = node.edge
#         visited = all(visitor(x) for x in group)
#
#         if edge.uses_hash or not visited:
#             # the edge doesn't have a constant hash
#             return False
#
#         hashes[node], masks[node] = edge.process_hashes([hashes[x] for x in group])
#         return True
#
#     hashes, masks = {}, {}
#     for n in outputs:
#         visitor(n)
#     return hashes, masks


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


def prune(inputs: Dict[TreeNode, NodeHash], output: TreeNode):
    masks = {}
    hashes = inputs.copy()
    edge, group = output.edge
    hashes[output], masks[output] = edge.process_hashes(LazyHashes(group, hashes, masks))
    return hashes, masks


def hash_graph(inputs: Sequence[TreeNode], output: TreeNode):
    def visitor(node: TreeNode):
        if node not in hashes:
            edge, group = node.edge
            hashes[node] = edge.hash_graph(list(map(visitor, group)))

        return hashes[node]

    hashes = dict.fromkeys(inputs, NodeHash.from_leaf(Placeholder))
    return visitor(output)


def render(node, cache, masks, hashes):
    if node not in cache:
        edge, inputs = node.edge
        mask = masks[node]

        inputs = [inputs[idx] for idx in mask]
        cache[node] = edge.evaluate(
            tuple(render(x, cache, masks, hashes) for x in inputs),
            mask, hashes[node]
        )

    return cache[node]


class Placeholder:
    """
    A placeholder used to calculate the graph hash without inputs.
    """

    # TODO: singleton
    def __init__(self):
        raise RuntimeError("Don't init me!")
