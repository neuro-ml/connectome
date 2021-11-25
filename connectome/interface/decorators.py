from typing import Callable

__all__ = 'RuntimeAnnotation', 'Meta', 'meta', 'Optional', 'optional'


class RuntimeAnnotation:
    def __init__(self, func: Callable):
        self.__func__ = func


class Optional(RuntimeAnnotation):
    pass


class Meta(RuntimeAnnotation):
    pass


# low case shortcuts
optional, meta = Optional, Meta
