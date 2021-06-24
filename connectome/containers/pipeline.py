from .base import Wrapper, EdgesBag


class PipelineLayer(EdgesBag):
    def __init__(self, head: EdgesBag, *tail: Wrapper):
        self.layers = [head, *tail]
        for layer in tail:
            head = layer.wrap(head)

        state = head.freeze()
        super().__init__(state.inputs, state.outputs, state.edges, state.context)

    def wrap(self, layer: EdgesBag) -> EdgesBag:
        return PipelineLayer(layer, *self.layers)


class LazyPipelineLayer(Wrapper):
    def __init__(self, *layers: Wrapper):
        self.layers = layers

    def wrap(self, layer: EdgesBag) -> EdgesBag:
        return PipelineLayer(layer, *self.layers)
