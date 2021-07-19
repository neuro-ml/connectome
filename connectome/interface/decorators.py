import warnings
from typing import Callable


class FactoryAnnotation:
    def __init__(self, func: Callable):
        if not callable(func):
            raise TypeError('Can only decorate callable objects')
        self.__func__ = func

    def __call__(self, *args, **kwargs):
        return self.__func__(*args, **kwargs)


class NodeAnnotation(FactoryAnnotation):
    """ Used to alter the behaviour of input/output nodes of an edge. """


class EdgeAnnotation(FactoryAnnotation):
    """ Used to customize the edge's behaviour. """


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
    pass


class Optional(RuntimeAnnotation):
    pass


class Meta(RuntimeAnnotation):
    pass


# low case shortcuts
inverse, optional, positional, meta, impure = Inverse, Optional, Positional, Meta, Impure


def insert(x):
    warnings.warn('The `insert` decorator is deprecated. Currently it has no effect.')
    return x
