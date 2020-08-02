from contextlib import suppress
from typing import Sequence, Callable


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
