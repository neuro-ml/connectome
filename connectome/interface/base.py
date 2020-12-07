from typing import Callable, Sequence

from ..engine.base import TreeNode
from ..layers.base import Layer, EdgesBag
from ..layers.pipeline import PipelineLayer
from ..utils import MultiDict
from .factory import SourceFactory, TransformFactory


class BaseBlock:
    _layer: Layer

    # TODO: think of a better interface for loopback
    def _wrap_predict(self, function: Callable, forward_names: Sequence[str], backward_name: str):
        if isinstance(self._layer, EdgesBag):
            return self._layer.get_loopback(function, forward_names, backward_name)
        else:
            raise TypeError


class CallableBlock(BaseBlock):
    _layer: EdgesBag

    def __getattr__(self, name):
        method = self._layer.get_forward_method(name)
        # FIXME: hardcoded
        if name == 'ids':
            ids = method()
            if not isinstance(ids, (tuple, list)):
                raise ValueError(f'The ids must be a tuple of strings, not {type(ids)}')
            if not all(isinstance(x, str) for x in ids):
                raise ValueError(f'The ids must be a tuple of strings, not tuple of {type(ids[0])}')

            return ids

        return method

    def __rshift__(self, block: BaseBlock) -> 'Chain':
        return Chain(self, block)

    def __dir__(self):
        return [x.name for x in self._layer.outputs]

    def _visualize(self, name, path):
        mapping = TreeNode.from_edges(self._layer.edges)
        for o in self._layer.outputs:
            if o.name == name:
                mapping[o].visualize(path)


class FromLayer(BaseBlock):
    def __init__(self, layer):
        super().__init__()
        self._layer = layer


class Chain(CallableBlock):
    def __init__(self, head: CallableBlock, *tail: BaseBlock):
        super().__init__()
        self._layer: PipelineLayer = PipelineLayer(head._layer, *(layer._layer for layer in tail))

    def __getitem__(self, index):
        return Chain(*map(FromLayer, self._layer.slice(index.start, index.stop).layers))

    # def remove_cache(self):
    #     return Chain.from_pipeline(self._layer.remove_cache_layers())
    #
    # @classmethod
    # def from_pipeline(cls, pipeline: PipelineLayer):
    #     return cls(*map(FromLayer, pipeline.layers))


def chained(*blocks: BaseBlock):
    def decorator(klass):
        class Chained(Chain):
            def __init__(self, *args, **kwargs):
                super().__init__(klass(*args, **kwargs), *blocks)

        return Chained

    return decorator


class SourceBase(type):
    def __new__(mcs, class_name, bases, namespace):
        def __init__(*args, **kwargs):
            assert args
            if len(args) > 1:
                raise TypeError('This constructor accepts only keyword arguments.')
            self = args[0]

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
            assert args
            if len(args) > 1:
                raise TypeError('This constructor accepts only keyword arguments.')
            self = args[0]

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
    __inherit__ = ()


# TODO add inheritance
class Source(CallableBlock, metaclass=SourceBase):
    pass
