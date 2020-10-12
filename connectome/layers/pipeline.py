from ..utils import node_to_dict
from .base import EdgesBag, Attachable
from .cache import CacheLayer


class PipelineLayer(EdgesBag):
    def __init__(self, head: EdgesBag, *tail: Attachable):
        self.layers = [head, *tail]
        for layer in tail:
            head = layer.wrap(head)

        params = head.prepare()
        super().__init__(params.inputs, params.outputs, params.edges, params.backward_inputs, params.backward_outputs)

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
