from typing import Callable


class DecoratorAdapter:
    name: str

    def __init__(self, func: Callable):
        if not callable(func):
            raise TypeError('Can only decorate callable objects')
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


class InsertDecoratorAdapter(DecoratorAdapter):
    name = 'insert'


class PositionalDecoratorAdapter(DecoratorAdapter):
    name = 'positional'


class PropertyDecoratorAdapter(DecoratorAdapter):
    name = 'property'


def inverse(func: Callable):
    return InverseDecoratorAdapter(func)


def optional(func: Callable):
    return OptionalDecoratorAdapter(func)


def positional(func: Callable):
    return PositionalDecoratorAdapter(func)


def insert(func: Callable):
    return InsertDecoratorAdapter(func)


def meta(func: Callable):
    return PropertyDecoratorAdapter(func)
