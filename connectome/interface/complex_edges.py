from typing import Callable, Iterable

from .edges import EdgeFactory, TypedEdge, Function, FunctionWrapper, FunctionBase
from .nodes import Intermediate, Default, NodeTypes, NodeType
from ..engine import Edge, ComputableHashEdge


class HashByValue(FunctionWrapper):
    def _wrap(self, edge: Edge, inputs: NodeTypes, output: NodeType) -> Iterable[TypedEdge]:
        yield TypedEdge(ComputableHashEdge(edge), inputs, output)


class CombinedHashByValue(EdgeFactory):
    def __init__(self, prepare: Callable, compute: Callable):
        if not isinstance(prepare, FunctionBase):
            prepare = Function.decorate(prepare)
        if not isinstance(compute, FunctionBase):
            compute = Function.decorate(compute)
        self.prepare = prepare
        self.compute = compute

    def build(self, name: str) -> Iterable[TypedEdge]:
        # TODO: here `name` doesn't matter, probably should pass Default(name) inside factory
        (edge, inputs, output), = self.prepare.build(name)
        assert isinstance(output, Default)
        inter = Intermediate()
        yield TypedEdge(ComputableHashEdge(edge), inputs, inter)

        (edge, inputs, output), = self.compute.build(name)
        inputs = list(inputs)
        inputs[0] = inter
        yield TypedEdge(edge, inputs, output)


def hash_by_value(func: Callable = None, *, prepare: Callable = None, compute: Callable = None):
    def decorator(f):
        return CombinedHashByValue(prepare or f, compute or f)

    if (prepare is None) ^ (compute is None):
        assert func is None
        return decorator

    if prepare is not None:
        assert func is None
        return CombinedHashByValue(prepare, compute)

    assert func is not None
    return HashByValue.decorate(func)
