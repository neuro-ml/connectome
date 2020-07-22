from functools import wraps
from typing import Callable


class DecoratorAdapter:
    name = None

    def __init__(self, func):
        self.__func__ = func

    def __get__(self, instance, owner):
        self.instance = instance
        return self.__func__

    def __call__(self, *args, **kwargs):
        return self.__func__(*args, **kwargs)


class InverseDecoratorAdapter(DecoratorAdapter):
    name = 'inverse'


class OptionalDecoratorAdapter(DecoratorAdapter):
    name = 'optional'


def inverse(func: Callable):
    return wraps(func)(InverseDecoratorAdapter(func))


def optional(func: Callable):
    return wraps(func)(OptionalDecoratorAdapter(func))
