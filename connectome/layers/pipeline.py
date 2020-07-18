from ..engine.edges import IdentityEdge
from ..engine import BoundEdge, TreeNode
from .base import EdgesBag
from ..engine.graph import compile_graph


def add_identity(outputs, inputs):
    for inp in inputs:
        for out in outputs:
            if inp.name == out.name:
                yield BoundEdge(IdentityEdge(), [inp], out)
                continue

        raise ValueError


class PipelineLayer(EdgesBag):
    def __init__(self, head: EdgesBag, *tail: EdgesBag):
        inputs = head.inputs
        edges = list(head.edges)
        outputs = head.outputs
        for layer in tail:
            edges.extend(add_identity(outputs, layer.inputs))
            edges.extend(layer.edges)
            outputs = layer.outputs

        mapping = TreeNode.from_edges(edges)
        inputs = [mapping[x] for x in inputs]
        outputs = [mapping[x] for x in outputs]

        self._methods = {}
        for node in outputs:
            self._methods[node.name] = compile_graph(inputs, node)

        super().__init__(inputs, outputs, edges)

    def get_method(self, name):
        return self._methods[name]

    #     self.set_graph_forwards_from_layer(layers[0])
    #     self.create_forward_connections(layers)
    #
    #     self.set_graph_backwards_from_layer(layers[-1])
    #     self.create_backward_connections(layers[:-1])
    #     self.layers = layers
    #
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
