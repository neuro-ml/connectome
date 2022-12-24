from abc import ABC, abstractmethod
from typing import Tuple, AbstractSet

from ..engine.base import TreeNode, Node, Nodes, BoundEdges, TreeNodes
from ..engine.edges import IdentityEdge
from ..utils import node_to_dict

__all__ = 'Context', 'NoContext', 'IdentityContext', 'BagContext'


class Context(ABC):
    @abstractmethod
    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        pass

    @abstractmethod
    def update(self, mapping: dict) -> 'Context':
        pass


class NoContext(Context):
    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        raise ValueError('The layer is not reversible')

    def update(self, mapping: dict) -> 'Context':
        return self


class IdentityContext(Context):
    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        # just propagate everything
        return outputs, edges

    def update(self, mapping: dict) -> 'Context':
        return self


class BagContext(Context):
    def __init__(self, inputs: Nodes, outputs: Nodes, inherit: AbstractSet[str]):
        self.inputs = inputs
        self.outputs = outputs
        self.inherit = inherit

    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        edges = list(edges)
        outputs = node_to_dict(outputs)
        # backward transforms
        for node in self.inputs:
            name = node.name
            if name in outputs:
                edges.append(IdentityEdge().bind(outputs[name], node))

        # collect the actual outputs
        actual = []
        mapping = TreeNode.from_edges(edges)
        leaves = [mapping[node] for node in inputs]
        for node in self.outputs:
            if is_reachable(leaves, mapping[node]):
                actual.append(node)

        # add inheritance
        add = self.inherit - set(node_to_dict(actual))
        for name, node in outputs.items():
            name = node.name
            if name in add:
                out = Node(name)
                edges.append(IdentityEdge().bind(node, out))
                actual.append(out)

        return actual, edges

    def update(self, mapping: dict) -> 'Context':
        return BagContext(
            update_map(self.inputs, mapping),
            update_map(self.outputs, mapping),
            self.inherit,
        )


def update_map(nodes, node_map):
    for node in nodes:
        if node not in node_map:
            node_map[node] = Node(node.name)
    return [node_map[x] for x in nodes]


def is_reachable(inputs: TreeNodes, output: TreeNode):
    def reachable(x: TreeNode):
        if x.is_leaf:
            return x in inputs

        return all(map(reachable, x.parents))

    inputs = set(inputs)
    return reachable(output)
