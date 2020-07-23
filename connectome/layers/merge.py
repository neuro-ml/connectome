from collections import defaultdict
from typing import Callable

from .pipeline import PipelineLayer
from ..engine.edges import ProductEdge, SwitchEdge, ProjectionEdge, IdentityEdge
from ..engine.base import Node, BoundEdge
from .base import EdgesBag


class ProductLayer(EdgesBag):
    def __init__(self, *layers: EdgesBag):
        self.layers = layers
        super().__init__(*self.create_graph())

    def create_graph(self):
        # TODO: backwards support?
        inputs = []
        all_edges = []
        output_groups = defaultdict(list)
        for layer in self.layers:
            layer_params = layer.prepare()
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


class SwitchLayer(PipelineLayer):
    """
    Parameters
    ----------
    selector
        returns the index of the branch to be evaluated
    """

    def __init__(self, selector: Callable, *layers: EdgesBag):
        self.selector = selector
        self.core = ProductLayer(*layers)
        super().__init__(self.make_switch(), self.core, self.make_projector())

    def make_switch(self):
        def selector(idx):
            def func(value):
                selected = self.selector(value)
                assert 0 <= selected < len(self.core.inputs), selected
                return selected == idx

            return func

        inputs = [Node('input')]
        edges, outputs = [], []
        for i, output in enumerate(self.core.inputs):
            output = Node(output.name)
            outputs.append(output)
            edges.append(BoundEdge(SwitchEdge(selector(i)), inputs, output))

        return EdgesBag(inputs, outputs, edges)

    def make_projector(self):
        inputs, outputs, edges = [], [], []
        for node in self.core.outputs:
            inp, out = Node(node.name), Node(node.name)
            inputs.append(inp)
            outputs.append(out)
            edges.append(BoundEdge(ProjectionEdge(), [inp], out))

        return EdgesBag(inputs, outputs, edges)
