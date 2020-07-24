from typing import Sequence, Callable


class InsertError(KeyError):
    pass


class ChainDict:
    def __init__(self, dicts: Sequence, selector: Callable):
        self.dicts = dicts
        self.selector = selector

    def __contains__(self, key):
        return any(key in d for d in self.dicts)

    def __getitem__(self, key):
        # small optimization
        if len(self.dicts) == 1:
            return self.dicts[0][key]

        for d in self.dicts:
            # TODO: should better suppress?
            if key in d:
                return d[key]

        raise KeyError(key)

    def __setitem__(self, key, value):
        for d in self.dicts:
            if self.selector(d):
                d[key] = value
                return

        raise InsertError('No appropriate mapping was found.')
