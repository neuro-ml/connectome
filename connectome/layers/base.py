import logging
import warnings
from typing import Callable, Iterable

from .chain import connect
from ..containers.base import EdgesBag
from ..engine import Details
from ..exceptions import FieldError
from ..interface.utils import format_arguments
from ..utils import StringsLike

logger = logging.getLogger(__name__)


class Layer:
    def _connect(self, previous: EdgesBag) -> EdgesBag:
        """
        Connect to a `previous` layer in a chain

        Parameters
        ----------
        previous:
            the contents of the previous layer

        Returns
        -------
        The contents of the new merged layer
        """
        raise NotImplementedError


class CallableLayer(Layer):
    def __init__(self, container: EdgesBag, properties: Iterable[str]):
        self._container = container
        self._methods = self._container.compile()
        self._properties = set(properties)

    def __getattr__(self, name):
        try:
            method = self._compile(name)
        except FieldError as e:
            raise AttributeError(name) from e

        if name in self._properties:
            return method()
        return method

    def _connect(self, previous: EdgesBag) -> EdgesBag:
        if not hasattr(self._container, 'wrap'):
            return connect(previous, self._container)

        warnings.warn(
            f'The method Container.wrap is deprecated and will be removed soon: {type(self._container).__name__}',
            UserWarning
        )
        warnings.warn(
            f'The method Container.wrap is deprecated and will be removed soon: {type(self._container).__name__}',
            DeprecationWarning
        )
        return self._container.wrap(previous)

    def __rshift__(self, layer: Layer) -> 'Chain':
        return Chain(self, layer)

    def __call__(self, *args, **kwargs) -> 'Instance':
        return Instance(self, args, kwargs)

    def __dir__(self):
        return self._methods.fields()

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
            logger.info('Loopback compiled: %s', loopback.fields())
            return loopback[final]

        return decorator

    def _compile(self, inputs: StringsLike):
        if not isinstance(inputs, str):
            inputs = tuple(inputs)
        return self._methods.compile(inputs)


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
    def __init__(self, head: CallableLayer, *tail: Layer):
        self._layers = [head, *tail]
        container = head._container
        for layer in tail:
            container = layer._connect(container)

        container = container.freeze(Details(type(self)))
        super().__init__(container, head._properties)

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
        from .cache import CacheLayer

        return self._filterfalse(isinstance, CacheLayer)

    def __repr__(self):
        if len(self._layers) == 2:
            a, b = self._layers
            return f'{a} >> {b}'

        return 'Chain' + format_arguments(self._layers)


class LazyChain(Layer):
    def __init__(self, *layers: Layer):
        self._layers = layers

    def _connect(self, previous: EdgesBag) -> EdgesBag:
        for layer in self._layers:
            previous = layer._connect(previous)
        previous = previous.freeze(Details(type(self)))
        return previous

    def __repr__(self):
        return 'LazyChain' + format_arguments(self._layers)


def chained(*layers: CallableLayer, lazy: bool = False):
    base = LazyChain if lazy else Chain

    def decorator(klass):
        class Chained(base):
            def __init__(self, *args, **kwargs):
                super().__init__(klass(*args, **kwargs), *layers)

        return Chained

    return decorator
