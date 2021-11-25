from typing import Callable, Iterable

from .edges import EdgeFactory, TypedEdge, Function
from .nodes import Intermediate, Default, Input, Output
from ..engine.edges import ComputableHashEdge, FunctionEdge


class HashByValue(EdgeFactory):
    default_input = Input
    default_output = Output

    def __init__(self, func: Callable):
        self.func = func

    def build(self, name: str) -> Iterable[TypedEdge]:
        inputs = Function.extract_arguments(self.func)
        yield TypedEdge(ComputableHashEdge(self.func, len(inputs)), inputs, Default(name))


class CombinedHashByValue(EdgeFactory):
    default_input = Input
    default_output = Output

    def __init__(self, prepare: Callable, compute: Callable):
        self.prepare = prepare
        self.compute = compute

    def build(self, name: str) -> Iterable[TypedEdge]:
        inputs = Function.extract_arguments(self.prepare)
        inter = Intermediate()
        yield TypedEdge(ComputableHashEdge(self.prepare, len(inputs)), inputs, inter)

        inputs = list(Function.extract_arguments(self.compute))
        inputs[0] = inter
        yield TypedEdge(FunctionEdge(self.prepare, len(inputs)), inputs, Default(name))


def hash_by_value(func: Callable = None, *, prepare: Callable = None, compute: Callable = None):
    def decorator(f):
        return CombinedHashByValue(prepare or f, compute or f)

    if (prepare is None) ^ (compute is None):
        assert func is None
        return decorator

    if prepare is not None:
        return CombinedHashByValue(prepare, compute)

    assert func is not None
    return HashByValue(func)


# TODO: deprecated
ComputableHash = CombinedHashByValue
