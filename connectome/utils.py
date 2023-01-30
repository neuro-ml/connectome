import inspect
import warnings
from collections import Counter
from pathlib import Path
from typing import Union, Dict, List, Sequence, AbstractSet, Iterable

PathLike = Union[Path, str]
Strings = Sequence[str]
StringsLike = Union[str, Strings]
NameSet = AbstractSet[str]


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
        if len(self.get(key, ())) > 1:
            raise ValueError("Can't delete a name with multiple definitions")

        super().__delitem__(key)


class AntiSet(AbstractSet):
    def __init__(self, excluded: Iterable = ()):
        super().__init__()
        assert not isinstance(excluded, AntiSet)
        self.excluded = set(excluded)

    def __iter__(self):
        raise TypeError("Can't iterate over infinite sets")

    def __len__(self):
        raise TypeError("Infinite sets don't have length")

    def __contains__(self, item) -> bool:
        return item not in self.excluded

    def __repr__(self):
        if self.excluded:
            return f'{{*}} - {self.excluded}'
        return '{*}'

    def __bool__(self) -> bool:
        return True

    # operations
    def __and__(self, other) -> AbstractSet:
        if isinstance(other, AntiSet):
            return AntiSet(self.excluded | other.excluded)

        return other - self.excluded

    __rand__ = __and__

    def __sub__(self, other: set) -> AbstractSet:
        if isinstance(other, AntiSet):
            return other.excluded - self.excluded

        return AntiSet(self.excluded | other)

    def __rsub__(self, other):
        if isinstance(other, AntiSet):
            return other - self

        return self.excluded & other

    def __or__(self, other: set) -> AbstractSet:
        if isinstance(other, AntiSet):
            return AntiSet(self.excluded & other.excluded)

        return AntiSet(self.excluded - other)

    __ror__ = __or__

    def __eq__(self, other: set) -> bool:
        return isinstance(other, AntiSet) and self.excluded == other.excluded

    def __ne__(self, other):
        return not (self == other)

    # safety first
    def __iand__(self, other):
        raise NotImplementedError

    __ge__ = __gt__ = __ixor__ = __le__ = __lt__ = \
        __ior__ = __isub__ = __rxor__ = __xor__ = __iand__


def extract_signature(func):
    names = []
    annotations = {}
    signature = inspect.signature(func)
    for parameter in signature.parameters.values():
        if parameter.default != parameter.empty:
            raise ValueError(
                f'Function {func} has a default value for parameter {parameter.name}. '
                'Default parameters are currently not supported.'
            )
        if parameter.kind != parameter.POSITIONAL_OR_KEYWORD:
            raise ValueError(
                f'Error for function {func}, parameter {parameter.name}: '
                f'all parameters must be "positional-or-keyword"'
            )

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


def deprecation_warn(level=3):
    warnings.warn('This class is deprecated', UserWarning, level)
    warnings.warn('This class is deprecated', DeprecationWarning, level)


def to_seq(x, cls=str):
    if isinstance(x, cls):
        x = [x]
    return x
