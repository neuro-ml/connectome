import inspect
from collections import Counter
from pathlib import Path
from typing import Union, Dict, List, Sequence, Callable
from contextlib import suppress

PathLike = Union[Path, str]


class MultiDict(Dict[str, List]):
    def items(self):
        for key, values in self.groups():
            for value in values:
                yield key, value

    def to_dict(self):
        result = {}
        for name, values in self.groups():
            assert len(values) == 1
            result[name], = values
        return result

    def groups(self):
        return super().items()

    def __setitem__(self, key, value):
        if key in self:
            super().__getitem__(key).append(value)
        else:
            super().__setitem__(key, [value])

    def __getitem__(self, key):
        return super().__getitem__(key)[-1]

    def __delitem__(self, key):
        raise ValueError("Can't delete names from this scope")


def extract_signature(func):
    names = []
    annotations = {}
    signature = inspect.signature(func)
    for parameter in signature.parameters.values():
        assert parameter.default == parameter.empty, parameter
        assert parameter.kind == parameter.POSITIONAL_OR_KEYWORD, parameter
        names.append(parameter.name)
        annotations[parameter.name] = parameter.annotation

    return names, annotations


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
