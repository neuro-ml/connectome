import inspect
from collections import Counter
from pathlib import Path
from typing import Union, Dict, List, Sequence

PathLike = Union[Path, str]
Strings = Sequence[str]
StringsLike = Union[str, Strings]


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


class AntiSet(set):
    def __init__(self, excluded: Union[Sequence, set], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.excluded = set(excluded)

    def __contains__(self, item):
        return not self.excluded.__contains__(item)

    def __repr__(self):
        return f'All elements except for {self.excluded.__repr__()}'

    def intersection(self, other: set) -> set:
        if isinstance(other, AntiSet):
            return AntiSet(self.excluded.union(other.excluded))

        return other.difference(self.excluded)

    def difference(self, other: set) -> set:
        if isinstance(other, AntiSet):
            return self.excluded.difference(other.excluded)

        return AntiSet(self.excluded.union(other))

    def union(self, other: set) -> set:
        if isinstance(other, AntiSet):
            return AntiSet(self.excluded.intersection(other.excluded))

        return AntiSet(self.excluded.difference(other))


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


def check_for_duplicates(nodes):
    counts: dict = Counter(node.name for node in nodes)
    assert not any(v > 1 for k, v in counts.items()), counts


def node_to_dict(nodes):
    nodes = tuple(nodes)
    check_for_duplicates(nodes)
    return {node.name: node for node in nodes}
