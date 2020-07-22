from collections import defaultdict
from typing import Sequence, Callable

from . import PipelineLayer
from ..engine.edges import MuxEdge, ProductEdge, SwitchEdge, ProjectionEdge, IdentityEdge
from ..utils import count_duplicates
from ..engine import Node, BoundEdge
from .base import FreeLayer, EdgesBag


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


# TODO add backwards
class MuxLayer(FreeLayer):
    def __init__(self, branch_selector: Callable, *layers: FreeLayer):
        super().__init__()

        self._outputs, self._inputs, self._edges = self.create_graph(branch_selector, layers)
        self._forward_methods = self.create_methods(self._outputs, self._inputs, self._edges)

    def get_forward_params(self, *args, **kwargs):
        raise RuntimeError("Mux layer can't be attached")

    def create_graph(self, branch_selector: Callable, layers: Sequence[FreeLayer]):
        inputs = []
        shared_output_names = set()

        for layer in layers:
            inputs.extend(layer.get_inputs())
            # check for outputs with the same name
            output_names = [o.name for o in layer.get_outputs()]
            assert any(v < 2 for _, v in count_duplicates(output_names).items())
            shared_output_names.update(output_names)

        layers_outputs_map = []
        layers_input_names = []

        for layer in layers:
            cur_outputs_map = {o.name: o for o in layer.get_outputs() if o.name in shared_output_names}
            cur_essential_inputs_map, _, _ = self.graph.get_graph_structure(cur_outputs_map.values(),
                                                                            layer.get_inputs(), layer.get_edges())
            # check for essential inputs with the same name
            assert any(v < 2 for _, v in count_duplicates(cur_essential_inputs_map.keys()).items())

            layers_outputs_map.append(cur_outputs_map)
            layers_input_names.append(cur_essential_inputs_map.keys())

        # all layers must contain the same set of input nodes
        input_names_union = set.union(*[set(names) for names in layers_input_names])
        input_names_intersection = set.intersection(*[set(names) for names in layers_input_names])
        assert input_names_intersection == input_names_union

        outputs = []
        mux_edges = []
        for name in shared_output_names:
            output_node = Node(name)
            edge = MuxEdge(branch_selector, [layer_map[name] for layer_map in layers_outputs_map], output_node)
            mux_edges.append(edge)
            outputs.append(output_node)

        all_edges = []
        for layer in layers:
            all_edges.extend(layer.get_edges())

        all_edges.extend(mux_edges)
        return outputs, inputs, all_edges
