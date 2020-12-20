from collections import defaultdict
from typing import Callable, Sequence

from .pipeline import PipelineLayer
from .transform import TransformLayer
from ..engine.edges import ProductEdge, SwitchEdge, ProjectionEdge, IdentityEdge, FunctionEdge
from ..engine.base import Node, BoundEdge, NodeHash, HashType
from .base import EdgesBag


def merge_tuple(x: Sequence[tuple]):
    return sum(map(tuple, x), ())


class SwitchLayer(PipelineLayer):
    """
    Parameters
    ----------
    selector
        returns the index of the branch to be evaluated
    """

    def __init__(self, selector: Callable, *layers: EdgesBag):
        self.selector = selector
        self.layers = layers
        self.core = TransformLayer(*self.create_graph())
        super().__init__(self.make_switch(), self.core, self.make_projector())

    def make_switch(self):
        def find_leaves(nh: NodeHash):
            if nh.kind == HashType.LEAF:
                yield nh.data

            else:
                for child in nh.children:
                    yield from find_leaves(child)

        def selector(idx):
            def func(value: NodeHash):
                leaf, = find_leaves(value)
                selected = self.selector(leaf)
                assert 0 <= selected < len(self.core.inputs), selected
                return selected == idx

            return func

        inputs = [Node('input')]
        edges, outputs = [], []
        for i, output in enumerate(self.core.inputs):
            output = Node(output.name)
            outputs.append(output)
            edges.append(BoundEdge(SwitchEdge(selector(i)), inputs, output))

        return TransformLayer(inputs, outputs, edges)

    def create_graph(self):
        # TODO: backwards support?
        inputs = []
        all_edges = []
        output_groups = defaultdict(list)
        for layer in self.layers:
            layer_params = layer.freeze()
            inp, = layer_params.inputs
            inputs.append(inp)

            for output in layer_params.outputs:
                output_groups[output.name].append(output)

            all_edges.extend(layer_params.edges)

        arity = len(self.layers)
        outputs = []
        for name, nodes in output_groups.items():
            if len(nodes) != arity:
                continue

            output = Node(name)
            outputs.append(output)
            all_edges.append(BoundEdge(ProductEdge(arity), nodes, output))

        assert outputs

        # avoiding name clashes
        unique_inputs = []
        for idx, node in enumerate(inputs):
            inp = Node(f'arg{idx}')
            unique_inputs.append(inp)
            all_edges.append(BoundEdge(IdentityEdge(), [inp], node))

        return unique_inputs, outputs, all_edges

    def make_projector(self):
        inputs, outputs, edges = [], [], []
        for node in self.core.outputs:
            inp, out = Node(node.name), Node(node.name)
            inputs.append(inp)
            outputs.append(out)
            # FIXME: this is a dirty hack for now
            if node.name == 'ids':
                edge = FunctionEdge(merge_tuple, arity=1)
            else:
                edge = ProjectionEdge()

            edges.append(BoundEdge(edge, [inp], out))

        return TransformLayer(inputs, outputs, edges)
