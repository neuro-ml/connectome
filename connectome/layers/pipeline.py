from .base import EdgesBag, Attachable


class PipelineLayer(EdgesBag):
    def __init__(self, head: EdgesBag, *tail: Attachable):
        edges, node_map = head.prepare()

        forward_inputs = self.update_map(head.inputs, node_map)
        forward_outputs = self.update_map(head.outputs, node_map)

        backward_inputs = self.update_map(head.backward_inputs, node_map)
        backward_outputs = self.update_map(head.backward_outputs, node_map)

        for layer in tail:
            forward_outputs, backward_inputs, new_edges = layer.attach(forward_outputs, backward_inputs)
            for edge in new_edges:
                assert edge not in edges
                edges.append(edge)

        self.layers = [head, *tail]
        super().__init__(forward_inputs, forward_outputs, edges, backward_inputs, backward_outputs)

    def index(self, index):
        return self.slice(index, index + 1)

    def slice(self, start, stop):
        assert start >= 0

        if issubclass(type(self.layers[start]), EdgesBag):
            return PipelineLayer(*self.layers[start:stop])
        else:
            raise IndexError('First layer must be a EdgesBag')

    # def create_forward_connections(self, layers):
    #     for layer in layers[1:]:
    #         self._outputs, new_edges = layer.get_forward_params(self._outputs)
    #
    #         for e in new_edges:
    #             assert e not in self._edges
    #             self._edges.append(e)
    #
    #     self._forward_methods = self.create_methods(self._outputs, self._inputs, self._edges)
    #
    # def create_backward_connections(self, new_layers):
    #     for layer in reversed(new_layers):
    #         self._backward_outputs, new_edges = layer.get_backward_params(self._backward_outputs)
    #
    #         for e in new_edges:
    #             assert e not in self._edges
    #             self._edges.append(e)
    #
    #     self.check_backwards()
    #     self._backward_methods = self.create_methods(self._backward_outputs, self._backward_inputs, self._edges)
    #
    # def get_forward_params(self, outputs: Sequence[Node]):
    #     all_edges = []
    #     for layer in self.layers:
    #         outputs, edges = layer.get_forward_params(outputs)
    #         all_edges.extend(edges)
    #
    #     return outputs, all_edges
    #
    # # TODO add operators?
    # def index(self, index):
    #     return self.slice(index, index + 1)
    #
    # def slice(self, start, stop):
    #     assert start >= 0, start > stop
    #
    #     if issubclass(type(self.layers[start]), FreeLayer):
    #         return PipelineLayer(*self.layers[start:stop])
    #     else:
    #         raise IndexError('First layer must be a Free layer')
