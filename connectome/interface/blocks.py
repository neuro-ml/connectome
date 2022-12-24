import operator

from ..containers.base import EdgesBag
from ..containers.goup import GroupContainer, MultiGroupLayer
from ..layers import *  # noqa
from ..utils import deprecation_warn, StringsLike  # noqa


class LegacyContainer(Layer):
    def __init__(self, container):
        deprecation_warn()
        self._container = container

    def _connect(self, previous: EdgesBag) -> EdgesBag:
        # FIXME
        return self._container.wrap(previous)


class GroupBy(LegacyContainer):
    def __init__(self, name):
        super().__init__(GroupContainer(name))

    @staticmethod
    def _multiple(*names, **comparators):
        assert set(comparators).issubset(names)
        for name in names:
            comparators.setdefault(name, operator.eq)
        return LegacyContainer(MultiGroupLayer(comparators))

    def __repr__(self):
        return f'GroupBy({repr(self._container.name)})'


def to_seq(x):
    if isinstance(x, str):
        x = [x]
    return x
