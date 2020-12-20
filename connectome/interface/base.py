from typing import Callable, Sequence, Union

from .utils import MaybeStr
from ..engine.base import TreeNode
from ..layers.base import Layer, EdgesBag
from ..layers.pipeline import PipelineLayer
from ..utils import MultiDict
from .factory import SourceFactory, TransformFactory


class BaseBlock:
    _layer: Layer


class CallableBlock(BaseBlock):
    _layer: EdgesBag
    _methods: dict

    def __getattr__(self, name):
        method = self._methods[name]
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

    def _wrap(self, func: Callable, inputs: MaybeStr, outputs: MaybeStr = None, final: MaybeStr = None) -> Callable:
        return self._decorate(inputs, outputs, final)(func)

    def _decorate(self, inputs: MaybeStr, outputs: MaybeStr = None, final: MaybeStr = None) -> Callable:
        if outputs is None:
            outputs = inputs
        if final is None:
            final = outputs
        if not isinstance(final, str):
            final = tuple(final)

        def decorator(func: Callable) -> Callable:
            return self._layer.loopback(func, inputs, outputs)[final]

        return decorator

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
        self._methods = self._layer.compile()

    def __getitem__(self, index):
        if isinstance(index, int):
            index = slice(index, index + 1)

        if isinstance(index, slice):
            start = index.start if index.start is not None else 0
            return Chain(*map(FromLayer, self._layer.slice(start, index.stop).layers))

        raise ValueError('The index can be either an int or slice.')

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
            self._methods = self._layer.compile()

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
            self._methods = self._layer.compile()

        return super().__new__(mcs, class_name, bases, {'__init__': __init__})


class Transform(CallableBlock, metaclass=TransformBase):
    __inherit__: Union[str, Sequence[str], bool] = ()


# TODO add inheritance
class Source(CallableBlock, metaclass=SourceBase):
    pass
