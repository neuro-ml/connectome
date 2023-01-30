from abc import ABC, abstractmethod
from typing import Tuple, AbstractSet

from ..engine import TreeNode, Node, Nodes, BoundEdges, TreeNodes, IdentityEdge, Details
from ..utils import node_to_dict

__all__ = 'Context', 'NoContext', 'IdentityContext', 'BagContext'


class Context(ABC):
    @abstractmethod
    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        """ Return the new edges, that need to be added to the graph and the updated outputs  """

    @abstractmethod
    def update(self, mapping: dict) -> 'Context':
        """ Update the nodes and edges contained in the context. Used during `EdgesBag.freeze` """


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
            if node.name in add:
                out = node.clone()
                edges.append(IdentityEdge().bind(node, out))
                actual.append(out)

        return actual, edges

    def update(self, mapping: dict) -> 'Context':
        return BagContext(
            update_map(self.inputs, mapping),
            update_map(self.outputs, mapping),
            self.inherit,
        )


def update_map(nodes: Nodes, node_map: dict, parent: Details = None, layer_map: dict = None):
    for node in nodes:
        if node not in node_map:
            details = node.details
            if details is None:
                details = parent
            elif layer_map is not None:
                if node.details in layer_map:
                    details = layer_map[node.details]
                else:
                    details = layer_map[node.details] = node.details.update(layer_map, parent)

            node_map[node] = Node(node.name, details)

    return [node_map[x] for x in nodes]


def is_reachable(inputs: TreeNodes, output: TreeNode):
    def reachable(x: TreeNode):
        if x.is_leaf:
            return x in inputs

        return all(map(reachable, x.parents))

    inputs = set(inputs)
    return reachable(output)
