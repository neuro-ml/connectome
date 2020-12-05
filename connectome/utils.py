import inspect
from functools import wraps
from collections import Counter
from pathlib import Path
from typing import Union
from contextlib import suppress
from typing import Sequence, Callable

PathLike = Union[Path, str]


class MultiDict(dict):
    def items(self):
        for key, values in super().items():
            for value in values:
                yield key, value

    def __setitem__(self, key, value):
        if key not in self:
            container = []
            super().__setitem__(key, container)
        else:
            container = super().__getitem__(key)

        container.append(value)

    def __getitem__(self, key):
        container = super().__getitem__(key)
        assert len(container) == 1
        return container[0]


def extract_signature(func):
    res = []
    signature = inspect.signature(func)
    for parameter in signature.parameters.values():
        assert parameter.default == parameter.empty, parameter
        assert parameter.kind == parameter.POSITIONAL_OR_KEYWORD, parameter

        res.append(parameter.name)

    return res


def atomize(attribute: str = '_lock'):
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            mutex = getattr(self, attribute)
            if mutex is None:
                return func(self, *args, **kwargs)
            with mutex:
                return func(self, *args, **kwargs)

        return wrapper

    return decorator


# TODO add error message
def check_for_duplicates(collection):
    counts: dict = Counter(list(collection))
    assert not any(v > 1 for k, v in counts.items())


def node_to_dict(nodes):
    return {node.name: node for node in nodes}


class InsertError(KeyError):
    pass


class ChainDict:
    def __init__(self, dicts: Sequence, selector: Callable = None):
        self.dicts = dicts
        self.selector = selector

    def __contains__(self, key):
        return any(key in d for d in self.dicts)

    def __getitem__(self, key):
        for d in self.dicts:
            with suppress(KeyError):
                return d[key]

        raise KeyError(key)

    def __setitem__(self, key, value):
        if self.selector is None:
            raise ValueError('Insertion is not supported.')

        for d in self.dicts:
            if self.selector(d):
                d[key] = value
                return

        raise InsertError('No appropriate mapping was found.')
