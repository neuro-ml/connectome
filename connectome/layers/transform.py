from typing import Tuple, Sequence

from .base import Context, EdgesBag, update_map
from ..engine.base import BoundEdge, Node, Nodes, BoundEdges, TreeNode, TreeNodes
from ..engine.edges import IdentityEdge
from ..engine.graph import count_entries
from ..utils import check_for_duplicates, node_to_dict

INHERIT_ALL = True


class TransformLayer(EdgesBag):
    def __init__(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges, backward_inputs: Nodes = None,
                 backward_outputs: Nodes = None, optional_nodes: Sequence[str] = None,
                 inherit_nodes: Sequence[str] = None, persistent_nodes: Sequence[str] = None):
        super().__init__(
            inputs, outputs, edges,
            BagContext(backward_inputs or [], backward_outputs or [], inherit_nodes or [])
        )
        check_for_duplicates(node_to_dict(inputs).keys())

        self.inherit_nodes = inherit_nodes or []
        self.optional_nodes = optional_nodes or []
        self.persistent_nodes = persistent_nodes or []

    def wrap(self, layer: 'EdgesBag') -> 'EdgesBag':
        previous = layer.freeze()
        current = self.freeze()

        prev_outputs = node_to_dict(previous.outputs)
        cur_inputs = node_to_dict(current.inputs)

        if self.inherit_nodes == INHERIT_ALL:
            inherit_nodes = list(prev_outputs.keys())
        else:
            inherit_nodes = self.inherit_nodes + list(self.persistent_nodes)

        outputs = []
        edges = list(previous.edges) + list(current.edges)
        active_input_names = []

        # connect common nodes
        for i in current.inputs:
            if i.name in prev_outputs:
                active_input_names.append(i.name)
                edges.append(IdentityEdge().bind(prev_outputs[i.name], i))

        # check for inherited nodes
        defined_outputs = [o.name for o in current.outputs]
        for name, prev_output in prev_outputs.items():
            if name in inherit_nodes and (
                    name not in active_input_names or
                    name not in defined_outputs):
                output = Node(name)
                outputs.append(output)
                active_input_names.append(name)
                edges.append(BoundEdge(IdentityEdge(), [prev_output], output))

        # check that unused nodes are @optional
        unused_names = set(cur_inputs.keys()).difference(set(active_input_names))
        for name in unused_names:
            if name not in self.optional_nodes:
                raise RuntimeError(f"Previous layer must contain '{name}' node.")

        essential_input_names = self.get_essential_input_names(current.inputs, current.outputs, current.edges)
        for o in current.outputs:
            # drop nodes that depend on inactive inputs
            if all(name in active_input_names for name in essential_input_names[o]):
                outputs.append(o)

        return EdgesBag(
            previous.inputs, outputs, edges,
            PipelineContext(previous.context, current.context),
        )

    @staticmethod
    def get_essential_input_names(inputs: Sequence[Node], outputs: Sequence[Node], edges: BoundEdges):
        check_for_duplicates(node_to_dict(inputs).keys())

        tree_node_map = TreeNode.from_edges(edges)
        inputs = [tree_node_map[x] for x in inputs]

        essential_input_names = {}
        for o in outputs:
            output = tree_node_map[o]
            counts = count_entries(inputs, output)
            input_names = [x.name for x in inputs if counts.get(x, 0)]
            essential_input_names[o] = input_names
        return essential_input_names


def is_reachable(inputs: TreeNodes, output: TreeNode):
    def find_parents(x: TreeNode):
        if not x.edge:
            yield x
            return

        for parent in x.parents:
            yield from find_parents(parent)

    return set(find_parents(output)).issubset(inputs)


class PipelineContext(Context):
    def __init__(self, previous: Context, current: Context):
        self.previous = previous
        self.current = current

    def reverse(self, inputs: Nodes, outputs: Nodes, edges: BoundEdges) -> Tuple[Nodes, BoundEdges]:
        outputs, edges = self.current.reverse(inputs, outputs, edges)
        outputs, edges = self.previous.reverse(inputs, outputs, edges)
        return outputs, edges

    def update(self, mapping: dict) -> 'Context':
        return PipelineContext(self.previous.update(mapping), self.current.update(mapping))


class BagContext(Context):
    def __init__(self, inputs: Nodes, outputs: Nodes, inherit: Sequence[str]):
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
        output_names = node_to_dict(actual)
        for node in self.inputs:
            name = node.name
            if name not in output_names and (self.inherit == INHERIT_ALL or name in self.inherit):
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
