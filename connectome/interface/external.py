import inspect
from typing import Optional, Collection, Any, Iterable, Sequence

from .edges import EdgeFactory, TypedEdge
from .metaclasses import TransformBase
from .nodes import Default
from ..engine import StaticEdge, CustomHash, NodeHashes, NodeHash, LeafHash, StaticGraph
from ..layers import CallableLayer


Names = Collection[str]


class External(CallableLayer):
    def __init__(self, obj: Any, *, fields: Optional[Names] = None, properties: Optional[Names] = None,
                 inputs: Names, inherit: Names = (), marker: Any = None):
        cls = type(obj)
        if marker is None:
            marker = cls

        if fields is None or properties is None:
            props, methods = [], []
            for name in dir(cls):
                if name.startswith('_'):
                    continue
                attr = getattr(cls, name)
                if isinstance(attr, property):
                    props.append(name)
                # don't include classmedthods and staticmethods
                elif callable(attr) and not inspect.ismethod(attr):
                    methods.append(name)

            if fields is None:
                fields = methods
            if properties is None:
                properties = props

        properties = set(properties)
        methods = set(fields) - properties
        fields = set(fields) | properties
        items = []
        for name in fields:
            if name in methods:
                value = SimpleHash(inputs, (marker, name), getattr(obj, name))
            else:
                value = SimpleHash((), (marker, name), getter(obj, name))

            items.append((name, value))

        self._obj = obj
        container = TransformBase(items)._container
        container.persistent = properties
        container.virtual = set(inherit)
        super().__init__(container, properties)

    def __dir__(self):
        return dir(self._obj)

    def __getattr__(self, name):
        return getattr(self._obj, name)


class ExternalBase(External):
    def __init__(self, *, fields: Optional[Names] = None, properties: Optional[Names] = None,
                 inputs: Optional[Names], inherit: Names = (), marker: Any = None):
        super().__init__(self, fields=fields, properties=properties, inputs=inputs, inherit=inherit, marker=marker)


class SimpleHash(EdgeFactory):
    def __init__(self, inputs, obj, func):
        self.obj = obj
        self.func = func
        self.inputs = inputs

    def build(self, name: str) -> Iterable[TypedEdge]:
        yield TypedEdge(
            SimpleHashEdge(len(self.inputs), self.func, self.obj), list(map(Default, self.inputs)), Default(name)
        )


class SimpleHashEdge(StaticGraph, StaticEdge):
    def __init__(self, arity: int, func, marker):
        super().__init__(arity)
        self.func = func
        self.marker = marker

    def _make_hash(self, inputs: NodeHashes) -> NodeHash:
        return CustomHash('connectome.External', LeafHash(self.marker), *inputs)

    def _evaluate(self, inputs: Sequence[Any]) -> Any:
        return self.func(*inputs)


def getter(obj, name):
    return lambda: getattr(obj, name)
