from typing import Callable

from .base import EdgesBag, Wrapper
from .utils import make_static_product
from ..engine.base import BoundEdge, Node
from ..engine.edges import FunctionEdge
from ..utils import extract_signature


def make_mapper(predicate):
    def func(ids, *groups):
        result = []
        for i, *group in zip(ids, *groups):
            if predicate(*group):
                result.append(i)
        return tuple(result)

    return func


class FilterLayer(Wrapper):
    """
    Changes only the `ids` attribute.
    """

    def __init__(self, predicate: Callable):
        self.names = extract_signature(predicate)
        assert 'ids' not in self.names and 'id' not in self.names
        self.func = make_mapper(predicate)

    @staticmethod
    def _find(nodes, name):
        for node in nodes:
            if node.name == name:
                return node

        raise ValueError(f'The previous layer must contain the attribute "{name}"')

    def wrap(self, layer: EdgesBag) -> EdgesBag:
        # FIXME: can we do something about this?
        keys = layer.get_forward_method('ids')()

        main = layer.prepare()
        inputs = main.inputs
        outputs = list(main.outputs)
        edges = list(main.edges)

        # change ids
        ids = self._find(outputs, 'ids')
        out = Node('ids')
        outputs.remove(ids)
        outputs.append(out)

        # collect all required products
        prod_edges, prod_outputs = make_static_product(layer, keys, self.names)
        assert len(prod_outputs) == len(self.names), set(prod_outputs) - set(self.names)
        edges.extend(prod_edges)

        # filter
        inp = [ids] + [prod_outputs[name] for name in self.names]
        edges.append(BoundEdge(FunctionEdge(self.func, len(self.names) + 1), inp, out))
        return EdgesBag(inputs, outputs, edges, [], [])
