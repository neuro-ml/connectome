from abc import ABC, abstractmethod
from typing import Tuple, AbstractSet, Dict

from ..engine import Node, Nodes, BoundEdges, IdentityEdge, Details
from ..utils import node_to_dict

__all__ = 'Context', 'NoContext', 'IdentityContext', 'BagContext', 'ChainContext'


class Context(ABC):
    @abstractmethod
    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        """ Return the new edges, that need to be added to the graph and the updated outputs  """

    @abstractmethod
    def update(self, nodes_map: Dict[Node, Node], layers_map: Dict[Details, Details]) -> 'Context':
        """ Update the nodes and edges contained in the context. Used during `EdgesBag.freeze` """


class NoContext(Context):
    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        raise ValueError('The layer is not reversible')

    def update(self, nodes_map: Dict[Node, Node], layers_map: Dict[Details, Details]) -> 'Context':
        return self


class IdentityContext(Context):
    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        # just propagate everything
        return outputs, edges

    def update(self, nodes_map: Dict[Node, Node], layers_map: Dict[Details, Details]) -> 'Context':
        return self


class BagContext(Context):
    def __init__(self, inputs: Nodes, outputs: Nodes, inherit: AbstractSet[str]):
        self.inputs = inputs
        self.outputs = outputs
        self.inherit = inherit

    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        edges = list(edges)
        outputs = node_to_dict(outputs)
        new_outputs = node_to_dict(self.outputs)
        # stitch the layers
        for node in self.inputs:
            name = node.name
            if name in outputs:
                edges.append(IdentityEdge().bind(outputs[name], node))

        # add inheritance
        for node in outputs.values():
            name = node.name
            if name in self.inherit and name not in new_outputs:
                out = node.clone()
                edges.append(IdentityEdge().bind(node, out))
                new_outputs[name] = out

        return tuple(new_outputs.values()), edges

    def update(self, nodes_map: Dict[Node, Node], layers_map: Dict[Details, Details]) -> 'Context':
        return BagContext(
            update_map(self.inputs, nodes_map, layers_map=layers_map),
            update_map(self.outputs, nodes_map, layers_map=layers_map),
            self.inherit,
        )


class ChainContext(Context):
    def __init__(self, previous: Context, current: Context):
        self.previous = previous
        self.current = current

    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        outputs, edges = self.current.reverse(inputs, outputs, edges)
        outputs, edges = self.previous.reverse(inputs, outputs, edges)
        return outputs, edges

    def update(self, nodes_map: Dict[Node, Node], layers_map: Dict[Details, Details]) -> 'Context':
        return ChainContext(self.previous.update(nodes_map, layers_map), self.current.update(nodes_map, layers_map))


def update_map(nodes: Nodes, node_map: dict, parent: Details = None, layers_map: Dict[Details, Details] = None):
    for node in nodes:
        if node not in node_map:
            details = node.details
            if details is None:
                details = parent
            elif layers_map is not None:
                if node.details in layers_map:
                    details = layers_map[node.details]
                else:
                    details = layers_map[node.details] = node.details.update(layers_map, parent)

            node_map[node] = Node(node.name, details)

    return [node_map[x] for x in nodes]
