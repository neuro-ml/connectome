from typing import Sequence, Callable

from ..edges import MuxEdge
from ..utils import count_duplicates
from ..engine import Node
from .base import FreeLayer


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
