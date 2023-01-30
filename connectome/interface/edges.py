import inspect
from abc import ABC, abstractmethod
from typing import Iterable, Callable, NamedTuple

from ..engine import Edge, FunctionEdge, ImpureEdge
from ..exceptions import FieldError
from .decorators import RuntimeAnnotation
from .utils import replace_annotation
from .nodes import *

__all__ = (
    'TypedEdge', 'EdgeFactory',
    'Function', 'FunctionWrapper', 'FunctionBase',
    'Inverse', 'inverse', 'Impure', 'impure', 'Positional', 'positional'
)


class TypedEdge(NamedTuple):
    edge: Edge
    inputs: NodeTypes
    output: NodeType


class EdgeFactory(ABC):
    @abstractmethod
    def build(self, name: str) -> Iterable[TypedEdge]:
        """ Returns an iterable of edges that represent the factory's logic """


class FunctionBase(EdgeFactory, ABC):
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
            if parameter.default != parameter.empty:
                raise ValueError(
                    f'Function {func} has a default value for parameter {parameter.name}. '
                    'Default parameters are currently not supported.'
                )

            is_positional = idx == 0 and parameter.kind == parameter.POSITIONAL_ONLY
            if not is_positional:
                assert parameter.kind == parameter.POSITIONAL_OR_KEYWORD, parameter

            args.append(replace_annotation(
                cls._process_annotation, parameter.annotation, parameter.name, is_positional
            ))

        return args


class Function(FunctionBase):
    func: Callable
    args: tuple
    kwargs: dict

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
        args = self.args + tuple(x[1] for x in kwargs)
        names = tuple(x[0] for x in kwargs)

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

    @classmethod
    def decorate(cls, func: Union[Callable, FunctionBase]) -> 'Function':
        if isinstance(func, RuntimeAnnotation):
            raise ValueError(f'The decorator of type {type(func)} must be at the top')
        return cls(func, *cls.extract_arguments(func))


class FunctionWrapper(FunctionBase, ABC):
    function: FunctionBase

    def __init__(*args, **kwargs):
        assert args
        assert len(args) >= 2
        self, func, *args = args
        if isinstance(func, EdgeFactory):
            assert not args
            assert not kwargs
        else:
            func = Function(func, *args, **kwargs)

        self.function = func

    @classmethod
    def decorate(cls, instance: Union[Callable, EdgeFactory]) -> 'FunctionWrapper':
        if not isinstance(instance, EdgeFactory):
            instance = Function.decorate(instance)
        return cls(instance)

    def build(self, name: str) -> Iterable[TypedEdge]:
        edge, = self.function.build(name)
        yield from self._wrap(*edge)

    @abstractmethod
    def _wrap(self, edge: Edge, inputs: NodeTypes, output: NodeType) -> Iterable[TypedEdge]:
        pass


class Inverse(FunctionWrapper):
    def _wrap(self, edge: Edge, inputs: NodeTypes, output: NodeType) -> Iterable[TypedEdge]:
        if isinstance(output, Default):
            output = InverseOutput(output.name)
        if not isinstance(output, InverseOutput):
            raise FieldError(f"The function can't be inverted, because its output is already of type {type(output)}")

        inputs = [
            replace_annotation(lambda a: InverseInput(a.name) if isinstance(a, Default) else a, x)
            for x in inputs
        ]
        yield TypedEdge(edge, inputs, output)


class Positional(FunctionWrapper):
    r"""
    Marks the first argument as positional.

    Can be used as an alternative to
    >>> def f(x, \, y):
    >>>     ...
    for older versions of Python.
    """

    def __init__(*args, **kwargs):
        assert args
        self, *args = args
        assert len(args) >= 1
        self._not_a_function(args[0])
        super(Positional, self).__init__(*args, **kwargs)

    @staticmethod
    def _not_a_function(func):
        if isinstance(func, EdgeFactory):
            raise TypeError(
                '"positional" can be used only for functions. '
                'If it is in a chain of decorators - it must be the lowest one'
            )

    @classmethod
    def decorate(cls, instance: Callable) -> 'FunctionWrapper':
        cls._not_a_function(instance)
        return cls(instance, *cls.extract_arguments(instance))

    def _wrap(self, edge: Edge, inputs: NodeTypes, output: NodeType) -> Iterable[TypedEdge]:
        if not inputs:
            raise ValueError('The "positional" can\'t be used with a function without arguments')

        inputs = [replace_annotation(lambda a: Default(output.name), inputs[0]), *inputs[1:]]
        yield TypedEdge(edge, inputs, output)


class Impure(FunctionWrapper):
    def _wrap(self, edge: Edge, inputs: NodeTypes, output: NodeType) -> Iterable[TypedEdge]:
        yield TypedEdge(ImpureEdge(edge), inputs, output)


inverse, positional, impure = Inverse.decorate, Positional.decorate, Impure.decorate
