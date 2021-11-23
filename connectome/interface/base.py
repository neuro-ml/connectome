import logging
from typing import Callable, Iterable, TypeVar

from .compat import Generic
from .utils import format_arguments
from ..containers.base import Container, EdgesBag
from ..containers.pipeline import PipelineContainer, LazyPipelineContainer
from ..exceptions import FieldError
from ..utils import StringsLike

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=Container)


class BaseLayer(Generic[T]):
    def __init__(self, container: T):
        self._container: T = container


class CallableLayer(BaseLayer[EdgesBag]):
    def __init__(self, container: EdgesBag, properties: Iterable[str]):
        super().__init__(container)
        self._methods = self._container.compile()
        self._properties = set(properties)

    def __getattr__(self, name):
        try:
            method = self._methods[name]
        except FieldError as e:
            raise AttributeError(name) from e

        if name in self._properties:
            return method()
        return method

    def __rshift__(self, layer: BaseLayer) -> 'Chain':
        return Chain(self, layer)

    def __call__(self, *args, **kwargs) -> 'Instance':
        return Instance(self, args, kwargs)

    def __dir__(self):
        return list(self._methods.outputs)

    def _wrap(self, func: Callable, inputs: StringsLike, outputs: StringsLike = None,
              final: StringsLike = None) -> Callable:
        return self._decorate(inputs, outputs, final)(func)

    def _decorate(self, inputs: StringsLike, outputs: StringsLike = None, final: StringsLike = None) -> Callable:
        if outputs is None:
            outputs = inputs
        if final is None:
            final = outputs
        if not isinstance(final, str):
            final = tuple(final)
        if isinstance(inputs, str):
            inputs = [inputs]

        def decorator(func: Callable) -> Callable:
            loopback = self._container.loopback(func, inputs, outputs)
            logger.info('Loopback compiled: %s', list(loopback.methods))
            return loopback[final]

        return decorator

    def _compile(self, inputs: StringsLike):
        if not isinstance(inputs, str):
            inputs = tuple(inputs)
        return self._methods[inputs]


class Instance:
    def __init__(self, layer: CallableLayer, args, kwargs):
        self._layer = layer
        self._args, self._kwargs = args, kwargs

    def _get(self, key):
        method = self._layer._compile(key)
        return method(*self._args, **self._kwargs)

    def __getattr__(self, name):
        return self._get(name)

    def __getitem__(self, name):
        return self._get(name)

    def __dir__(self):
        return dir(self._layer)


class Chain(CallableLayer):
    _container: PipelineContainer

    def __init__(self, head: CallableLayer, *tail: BaseLayer):
        super().__init__(
            PipelineContainer(head._container, *(layer._container for layer in tail)),
            head._properties,
        )
        self._layers = [head, *tail]

    def __getitem__(self, index):
        if isinstance(index, int):
            return self._layers[index]

        if isinstance(index, slice):
            return Chain(*self._layers[index])

        raise ValueError('The index can be either an int or slice.')

    def _filter(self, func, *args, **kwargs):
        filtered = []

        for layer in self._layers:
            if isinstance(layer, Chain):
                filtered.append(layer._filter(func, *args, **kwargs))
            elif func(layer, *args, **kwargs):
                filtered.append(layer)

        return Chain(*filtered)

    def _filterfalse(self, func, *args, **kwargs):
        def not_func(x, *a, **kw):
            return not func(x, *a, **kw)

        return self._filter(not_func, *args, **kwargs)

    def _drop_cache(self):
        from .blocks import CacheLayer

        return self._filterfalse(isinstance, CacheLayer)

    def __repr__(self):
        if len(self._layers) == 2:
            a, b = self._layers
            return f'{a} >> {b}'

        return 'Chain' + format_arguments(self._layers)


class LazyChain(BaseLayer[LazyPipelineContainer]):
    def __init__(self, *layers: BaseLayer):
        super().__init__(LazyPipelineContainer(*(layer._container for layer in layers)))
        self._layers = layers

    def __repr__(self):
        return 'LazyChain' + format_arguments(self._layers)


def chained(*layers: BaseLayer, lazy: bool = False):
    base = LazyChain if lazy else Chain

    def decorator(klass):
        class Chained(base):
            def __init__(self, *args, **kwargs):
                super().__init__(klass(*args, **kwargs), *layers)

        return Chained

    return decorator
