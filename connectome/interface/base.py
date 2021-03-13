from typing import Callable, Iterable

from .utils import MaybeStr
from ..engine.base import TreeNode
from ..layers.base import Layer, EdgesBag
from ..layers.pipeline import PipelineLayer
from ..layers.shortcuts import IdentityLayer
from ..layers.transform import InheritType
from ..utils import MultiDict
from .factory import SourceFactory, TransformFactory


class BaseBlock:
    def __init__(self, layer: Layer):
        self._layer: Layer = layer


class CallableBlock(BaseBlock):
    _layer: EdgesBag
    _methods: dict

    def __init__(self, layer: EdgesBag, properties: Iterable[str]):
        super().__init__(layer)
        self._methods = self._layer.compile()
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
            return layer.loopback(func, inputs, outputs)[final]

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


def chained(*blocks: BaseBlock):
    def decorator(klass):
        class Chained(Chain):
            def __init__(self, *args, **kwargs):
                super().__init__(klass(*args, **kwargs), *blocks)

        return Chained

    return decorator


class SourceBase(type):
    def __new__(mcs, class_name, bases, namespace, **flags):
        if flags.get('__root', False):
            def __init__(*args, **kwargs):
                raise RuntimeError("\"Source\" can't be directly initialized. You must subclass it first.")

            scope = {'__init__': __init__}

        else:
            # TODO: exception
            assert bases == (Source,)
            scope = SourceFactory.make_scope(namespace)
            bases = CallableBlock,

        return super().__new__(mcs, class_name, bases, scope)


class TransformBase(type):
    @classmethod
    def __prepare__(mcs, *args, **kwargs):
        return MultiDict()

    def __new__(mcs, class_name, bases, namespace, **flags):
        if flags.get('__root', False):
            # we can construct transforms on the fly
            def __init__(*args, __inherit__=(), **kwargs):
                assert args
                if len(args) > 1:
                    raise TypeError('This constructor accepts only keyword arguments.')
                self, = args

                local = MultiDict()
                local['__inherit__'] = __inherit__
                for name, value in kwargs.items():
                    assert callable(value)
                    local[name] = value

                factory = TransformFactory(local)
                super(type(self), self).__init__(factory.build({}), factory.property_names)

            scope = {'__init__': __init__, '__doc__': namespace['__doc__']}

        else:
            # TODO: exception
            assert bases == (Transform,)
            scope = TransformFactory.make_scope(namespace)
            bases = CallableBlock,

        return super().__new__(mcs, class_name, bases, scope)


class Transform(CallableBlock, metaclass=TransformBase, __root=True):
    """
    Base class for all transforms.

    Can also be used as an inplace factory for transforms.

    Examples
    --------
    # class-based transforms
    >>> class Zoom(Transform):
    >>>     def image(image):
    >>>         return zoom(image, scale_factor=2)
    # inplace transforms
    >>> Transform(image=lambda image: zoom(image, scale_factor=2))
    """
    __inherit__: InheritType = ()

    # these methods are ignored in the metaclass
    # we use them only to help IDEs
    def __init__(self, *args, **kwargs):
        pass


# TODO add inheritance
class Source(CallableBlock, metaclass=SourceBase, __root=True):
    def __init__(self, *args, **kwargs):
        pass
