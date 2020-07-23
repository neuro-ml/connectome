from typing import Callable, Sequence

from ..layers import PipelineLayer, Layer, EdgesBag, SwitchLayer
from ..utils import MultiDict
from .factory import SourceFactory, TransformFactory


class BaseBlock:
    _layer: Layer

    def wrap_predict(self, function: Callable, forward_names: Sequence[str], backward_name: str):
        if isinstance(self._layer, EdgesBag):
            return self._layer.get_loopback(function, forward_names, backward_name)
        else:
            raise TypeError


class CallableBlock(BaseBlock):
    _layer: EdgesBag

    def __getattr__(self, name):
        return self._layer.get_forward_method(name)

    def __dir__(self):
        return tuple(x.name for x in self._layer.inputs)


class Chain(CallableBlock):
    def __init__(self, head: CallableBlock, *tail: BaseBlock):
        super().__init__()
        self._layer: PipelineLayer = PipelineLayer(head._layer, *(layer._layer for layer in tail))

    def __getitem__(self, index):
        # FIXME: must return a Chain instead
        return FromLayer(self._layer.slice(index.start, index.stop))


class FromLayer(BaseBlock):
    def __init__(self, layer):
        super().__init__()
        self._layer = layer


class SourceBase(type):
    def __new__(mcs, class_name, bases, namespace):
        def __init__(*args, **kwargs):
            # TODO: error message
            self, = args
            # TODO: split into two objects: the first one holds the scope
            #  the second one compiles the layer
            factory = SourceFactory(namespace)
            scope = factory.get_init_signature().bind_partial(**kwargs)
            scope.apply_defaults()
            # TODO: should only build if not called from super
            factory.build(scope.kwargs)
            self._layer = factory.get_layer()

        return super().__new__(mcs, class_name, bases, {'__init__': __init__})


class TransformBase(type):
    @classmethod
    def __prepare__(mcs, *args):
        return MultiDict()

    def __new__(mcs, class_name, bases, namespace):
        def __init__(*args, **kwargs):
            # TODO: error message
            self, = args
            # TODO: split into two objects: the first one holds the scope
            #  the second one compiles the layer
            factory = TransformFactory(namespace)
            scope = factory.get_init_signature().bind_partial(**kwargs)
            scope.apply_defaults()
            # TODO: should only build if not called from super
            factory.build(scope.kwargs)
            self._layer = factory.get_layer()

        return super().__new__(mcs, class_name, bases, {'__init__': __init__})


class Transform(CallableBlock, metaclass=TransformBase):
    pass


# TODO add inheritance
class Source(CallableBlock, metaclass=SourceBase):
    pass


# TODO: move to blocks
class Merge(CallableBlock):
    def __init__(self, *blocks: CallableBlock):
        super().__init__()

        # FIXME: hope this works :)
        idx_union = set.union(*[set(layer.ids()) for layer in blocks])
        if sum(len(layer.ids()) for layer in blocks) != len(idx_union):
            raise RuntimeError('Datasets have same indices')

        def branch_selector(identifier):
            for idx, ds in enumerate(blocks):
                if identifier in ds.ids():
                    return idx

            raise ValueError(identifier)

        self._layer = SwitchLayer(branch_selector, *(s._layer for s in blocks))
