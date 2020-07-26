from ..utils import node_to_dict
from .base import EdgesBag, Attachable
from .cache import CacheLayer


class PipelineLayer(EdgesBag):
    def __init__(self, head: EdgesBag, *tail: Attachable):
        head_params = head.prepare()

        edges = head_params.edges
        forward_inputs = head_params.inputs
        forward_outputs = head_params.outputs
        backward_inputs = head_params.backward_inputs

        for layer in tail:
            forward_outputs, backward_inputs, new_edges = layer.attach(forward_outputs, backward_inputs)
            for edge in new_edges:
                assert edge not in edges
                edges.append(edge)

        backward_outputs = []
        # drop inactive outputs
        for name, value in node_to_dict(head_params.backward_outputs).items():
            if name in node_to_dict(backward_inputs):
                backward_outputs.append(value)

        self.layers = [head, *tail]
        super().__init__(forward_inputs, forward_outputs, edges, backward_inputs, backward_outputs)

    def remove_cache_layers(self):
        not_cache_layers = []
        for layer in self.layers:
            if not isinstance(layer, CacheLayer):
                not_cache_layers.append(layer)
        return PipelineLayer(*not_cache_layers)

    def index(self, index):
        return self.slice(index, index + 1)

    def slice(self, start, stop):
        assert start >= 0

        if issubclass(type(self.layers[start]), EdgesBag):
            return PipelineLayer(*self.layers[start:stop])
        else:
            raise IndexError('First layer must be a EdgesBag')
