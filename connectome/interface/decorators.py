from abc import ABC, abstractmethod
from typing import Callable

from .nodes import NodeTypes, NodeType
from ..engine.base import Edge

from ..engine.edges import ImpureFunctionEdge


class FactoryAnnotation:
    def __init__(self, func: Callable):
        if not callable(func):
            raise TypeError('Can only decorate callable objects')
        self.__func__ = func

    def __call__(self, *args, **kwargs):
        return self.__func__(*args, **kwargs)


class NodeAnnotation(FactoryAnnotation):
    """ Used to alter the behaviour of input/output nodes of an edge. """


class EdgeAnnotation(ABC, FactoryAnnotation):
    """ Used to customize the edge's behaviour. """

    @staticmethod
    @abstractmethod
    def build(func: Callable, inputs: NodeTypes, output: NodeType) -> Edge:
        """ Returns an edge that will map ``inputs`` to ``output``. """


class RuntimeAnnotation(FactoryAnnotation):
    pass


# implementations

class Inverse(NodeAnnotation):
    pass


class Positional(NodeAnnotation):
    r"""
    Marks the first argument as positional.

    Can be used as an alternative to
    >>> def f(x, \, y):
    >>>     ...
    for older versions of Python.
    """


class Impure(EdgeAnnotation):
    @staticmethod
    def build(func: Callable, inputs: NodeTypes, output: NodeType) -> Edge:
        return ImpureFunctionEdge(func, len(inputs))


class Optional(RuntimeAnnotation):
    pass


class Meta(RuntimeAnnotation):
    pass


# low case shortcuts
inverse, optional, positional, meta, impure = Inverse, Optional, Positional, Meta, Impure
