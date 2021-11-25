import inspect
from abc import ABC, abstractmethod
from typing import Iterable, Callable, NamedTuple

from .decorators import RuntimeAnnotation
from ..engine.base import Edge
from ..engine.edges import FunctionEdge, ImpureFunctionEdge
from .utils import replace_annotation
from .nodes import *

__all__ = (
    'TypedEdge', 'EdgeFactory',
    'Function',
    'Inverse', 'inverse', 'Impure', 'impure', 'Positional', 'positional'
)


class TypedEdge(NamedTuple):
    edge: Edge
    inputs: NodeTypes
    output: NodeType


class EdgeFactory(ABC):
    default_input: Type[NodeType]
    default_output: Type[NodeType]

    @abstractmethod
    def build(self, name: str) -> Iterable[TypedEdge]:
        """ Returns an iterable of edges that represent the factory's logic """


class Function(EdgeFactory):
    func: Callable
    args: tuple
    kwargs: dict
    default_input = Input
    default_output = Output

    def __init__(*args, **kwargs):
        assert args
        assert len(args) >= 2
        self, func, *args = args
        assert callable(func)

        self.func = func
        self.args = tuple(args)
        self.kwargs = kwargs

    def build(self, name: str) -> Iterable[TypedEdge]:
        kwargs = sorted(self.kwargs.items())
        args = self.args + tuple(x[0] for x in kwargs)
        names = tuple(x[1] for x in kwargs)

        silent, inputs = [], []
        for idx, arg in enumerate(args):
            if isinstance(arg, Silent):
                silent.append(idx)
                arg = arg.node
            if isinstance(arg, str):
                arg = Default(arg)
            if not isinstance(arg, NodeType):
                raise ValueError(arg)
            if isinstance(arg, AsOutput):
                arg = Default(name)

            inputs.append(arg)

        yield TypedEdge(
            FunctionEdge(self.func, len(inputs), names, tuple(silent)),
            inputs, Default(name),
        )

    @staticmethod
    def _process_annotation(annotation, name, is_positional):
        # detect the node type
        if isinstance(annotation, NodeType):
            raise ValueError(f'Invalid argument "{name}" annotation ({annotation})')

        if is_positional:
            return AsOutput()
        # need the `isinstance` part for faulty annotations, such as np.array
        elif isinstance(annotation, type) and issubclass(annotation, NodeType):
            return annotation(name)
        elif is_private(name):
            return Parameter(name)

        return Default(name)

    @classmethod
    def extract_arguments(cls, func: Callable):
        signature = list(inspect.signature(func).parameters.values())
        args = []
        for idx, parameter in enumerate(signature):
            assert parameter.default == parameter.empty, parameter
            is_positional = idx == 0 and parameter.kind == parameter.POSITIONAL_ONLY
            if not is_positional:
                assert parameter.kind == parameter.POSITIONAL_OR_KEYWORD, parameter

            args.append(replace_annotation(
                cls._process_annotation, parameter.annotation, parameter.name, is_positional
            ))

        return args

    @classmethod
    def decorate(cls, func: Callable) -> 'Function':
        if isinstance(func, RuntimeAnnotation):
            raise ValueError(f'The decorator of type {type(func)} must be at the top')
        if isinstance(func, Function):
            # TODO: potentially dangerous
            return cls(func.func, *func.args, **func.kwargs)
        return cls(func, *cls.extract_arguments(func))


class Inverse(Function):
    default_input = InverseInput
    default_output = InverseOutput

    def build(self, name: str) -> Iterable[TypedEdge]:
        (edge, inputs, output), = super().build(name)
        output = InverseOutput(output.name) if isinstance(output, Default) else output
        inputs = [
            replace_annotation(lambda a: InverseInput(a.name) if isinstance(a, Default) else a, x)
            for x in inputs
        ]
        yield TypedEdge(edge, inputs, output)


class Positional(Function):
    r"""
    Marks the first argument as positional.

    Can be used as an alternative to
    >>> def f(x, \, y):
    >>>     ...
    for older versions of Python.
    """

    def build(self, name: str) -> Iterable[TypedEdge]:
        (edge, inputs, output), = super().build(name)
        if not inputs:
            raise ValueError('The "positional" can\'t be used with a function without arguments')

        inputs = [replace_annotation(lambda a: AsOutput(), inputs[0]), *inputs[1:]]
        yield TypedEdge(edge, inputs, output)


class Impure(Function):
    # TODO: unify
    def build(self, name: str) -> Iterable[TypedEdge]:
        kwargs = sorted(self.kwargs.items())
        args = self.args + tuple(x[0] for x in kwargs)
        names = tuple(x[1] for x in kwargs)

        silent, inputs = [], []
        for idx, arg in enumerate(args):
            if isinstance(arg, str):
                arg = Default(arg)
            if not isinstance(arg, NodeType):
                raise ValueError(arg)
            if isinstance(arg, AsOutput):
                arg = Default(name)

            inputs.append(arg)

        yield TypedEdge(
            ImpureFunctionEdge(self.func, len(inputs), names),
            inputs, Default(name),
        )


inverse, positional, impure = Inverse.decorate, Positional.decorate, Impure.decorate
