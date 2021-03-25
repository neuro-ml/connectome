import logging
from typing import Callable, Iterable, TypeVar, Generic

from .utils import MaybeStr, format_arguments
from ..engine.base import TreeNode
from ..layers.base import Layer, EdgesBag
from ..layers.pipeline import PipelineLayer
from ..layers.shortcuts import IdentityLayer

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=Layer)


class BaseBlock(Generic[T]):
    def __init__(self, layer: T):
        self._layer: T = layer


class CallableBlock(BaseBlock[EdgesBag]):
    def __init__(self, layer: EdgesBag, properties: Iterable[str]):
        super().__init__(layer)
        self._methods: dict = self._layer.compile()
        self._properties = set(properties)

    def __getattr__(self, name):
        method = self._methods[name]
        if name in self._properties:
            return method()
        return method

    def __rshift__(self, block: BaseBlock) -> 'Chain':
        return Chain(self, block)

    def __call__(self, *args, **kwargs) -> 'Instance':
        return Instance(self, args, kwargs)

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
        if isinstance(inputs, str):
            inputs = [inputs]

        layer = self._layer
        if set(inputs) - {node.name for node in layer.outputs}:
            # FIXME: temporary dirty hack
            layer = PipelineLayer(IdentityLayer(set(inputs) | {node.name for node in layer.inputs}), layer)

        def decorator(func: Callable) -> Callable:
            loopback = layer.loopback(func, inputs, outputs)
            logger.info('Loopback compiled: %s', list(loopback.methods))
            return loopback[final]

        return decorator

    def _compile(self, inputs: MaybeStr):
        if not isinstance(inputs, str):
            inputs = tuple(inputs)
        return self._methods[inputs]

    def _visualize(self, name, path):
        mapping = TreeNode.from_edges(self._layer.edges)
        for o in self._layer.outputs:
            if o.name == name:
                mapping[o].visualize(path)


class Instance:
    def __init__(self, block: CallableBlock, args, kwargs):
        self._block = block
        self._args, self._kwargs = args, kwargs

    def __getattr__(self, name):
        method = getattr(self._block, name)
        return method(*self._args, **self._kwargs)

    def __dir__(self):
        return dir(self._block)


class Chain(CallableBlock):
    _layer: PipelineLayer

    def __init__(self, head: CallableBlock, *tail: BaseBlock):
        super().__init__(
            PipelineLayer(head._layer, *(layer._layer for layer in tail)),
            head._properties,
        )
        self._blocks = [head, *tail]

    def __getitem__(self, index):
        if isinstance(index, int):
            return self._blocks[index]

        if isinstance(index, slice):
            return Chain(*self._blocks[index])

        raise ValueError('The index can be either an int or slice.')

    def _drop_cache(self):
        from .blocks import CacheBlock

        not_cache = []
        for block in self._blocks:
            if isinstance(block, Chain):
                block = block._drop_cache()
            if not isinstance(block, CacheBlock):
                not_cache.append(block)

        return Chain(*not_cache)

    def __str__(self):
        return 'Chain' + format_arguments(self._blocks)

    def __repr__(self):
        return str(self)


def chained(*blocks: BaseBlock):
    def decorator(klass):
        class Chained(Chain):
            def __init__(self, *args, **kwargs):
                super().__init__(klass(*args, **kwargs), *blocks)

        return Chained

    return decorator
