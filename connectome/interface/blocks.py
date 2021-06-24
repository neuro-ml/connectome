import operator
from pathlib import Path
from typing import Union, Sequence, Callable
from paramiko.config import SSH_PORT

from .base import BaseLayer, CallableLayer
from ..containers.cache import MemoryCacheContainer, DiskCacheContainer, RemoteStorageContainer, CacheColumnsContainer
from ..containers.debug import HashDigestContainer
from ..containers.filter import FilterContainer
from ..containers.goup import GroupContainer, MultiGroupLayer
from ..containers.merge import SwitchContainer
from ..containers.shortcuts import ApplyContainer
from ..serializers import Serializer, ChainSerializer
from ..storage import Storage
from ..storage.locker import Locker, DummyLocker
from ..storage.remote import RemoteOptions
from ..utils import PathLike
from .utils import MaybeStr, format_arguments


class Merge(CallableLayer):
    def __init__(self, *layers: CallableLayer):
        properties = [set(layer._properties) for layer in layers]
        inter = set.intersection(*properties)
        union = set.union(*properties)
        if inter != union:
            raise ValueError(f'All inputs must have the same properties: {properties}')
        properties = inter
        if not properties:
            raise ValueError('The datasets do not contain properties.')
        if len(properties) > 1:
            raise ValueError(f'Can\'t decide which property to use as ids.')
        ids_name, = properties

        id2dataset_index = {}
        for index, dataset in enumerate(layers):
            ids = getattr(dataset, ids_name)
            intersection = set(ids) & set(id2dataset_index)
            if intersection:
                raise RuntimeError(f'Ids {intersection} are duplicated in merged datasets.')

            id2dataset_index.update({i: index for i in ids})

        super().__init__(SwitchContainer(id2dataset_index, [s._container for s in layers], ids_name), properties)
        self._layers = layers

    def __repr__(self):
        return 'Merge' + format_arguments(self._layers)


class Filter(BaseLayer[FilterContainer]):
    """
    Filters the `ids` of the current pipeline given a ``predicate``.

    Examples
    --------
    >>> dataset = Chain(
    >>>   source,  # dataset with `image` and `spacing` attributes
    >>>   Filter(lambda image, spacing: min(image.shape) > 30 and max(spacing) < 5),
    >>> )
    """

    def __init__(self, predicate: Callable):
        super().__init__(FilterContainer(predicate))

    @classmethod
    def drop(cls, ids: Sequence[str]):
        """Removes the provided ``ids`` from the dataset."""
        assert all(isinstance(i, str) for i in ids)
        ids = set(ids)
        return cls(lambda id: id not in ids)

    @classmethod
    def keep(cls, ids: Sequence[str]):
        """Removes all the ids not present in ``ids``."""
        assert all(isinstance(i, str) for i in ids)
        ids = set(ids)
        return cls(lambda id: id in ids)

    def __repr__(self):
        args = ', '.join(self._container.names)
        return f'Filter({args})'


class GroupBy(BaseLayer[GroupContainer]):
    def __init__(self, name: str):
        super().__init__(GroupContainer(name))

    @staticmethod
    def _multiple(*names, **comparators):
        assert set(comparators).issubset(names)
        for name in names:
            comparators.setdefault(name, operator.eq)
        return BaseLayer(MultiGroupLayer(comparators))

    def __repr__(self):
        return f'GroupBy({repr(self._container.name)})'


class Apply(BaseLayer[ApplyContainer]):
    def __init__(self, **transform: Callable):
        self.names = sorted(transform)
        super().__init__(ApplyContainer(transform))

    def __repr__(self):
        args = ', '.join(self.names)
        return f'Apply({args})'


def to_seq(x):
    if isinstance(x, str):
        x = [x]
    return x


def _resolve_serializer(serializer):
    if not isinstance(serializer, Serializer):
        serializer = ChainSerializer(*serializer)
    return serializer


class CacheLayer(BaseLayer):
    def __repr__(self):
        return self.__class__.__name__


class CacheToRam(CacheLayer):
    def __init__(self, names: MaybeStr = None, size: int = None):
        super().__init__(MemoryCacheContainer(names, size))


class CacheToDisk(CacheLayer):
    def __init__(self, root: PathLike, storage: Storage,
                 serializer: Union[Serializer, Sequence[Serializer]],
                 names: MaybeStr, metadata: dict = None, locker: Locker = None):
        names = to_seq(names)
        if locker is None:
            locker = DummyLocker()
        super().__init__(DiskCacheContainer(names, root, storage, _resolve_serializer(serializer), metadata or {}, locker))


class CacheColumns(CacheLayer):
    def __init__(self, root: PathLike, storage: Storage,
                 serializer: Union[Serializer, Sequence[Serializer]],
                 names: MaybeStr, metadata: dict = None, locker: Locker = None, verbose: bool = False):
        names = to_seq(names)
        if locker is None:
            locker = DummyLocker()
        super().__init__(CacheColumnsContainer(
            names, root, storage, _resolve_serializer(serializer), metadata or {}, locker=locker, verbose=verbose))


class RemoteStorageBase(CacheLayer):
    def __init__(self, options: Sequence[RemoteOptions],
                 serializer: Union[Serializer, Sequence[Serializer]], names: MaybeStr = None):
        names = to_seq(names)
        super().__init__(RemoteStorageContainer(names, options, _resolve_serializer(serializer)))


class RemoteStorage(RemoteStorageBase):
    def __init__(self, hostname: str, storage: Union[PathLike, Sequence[PathLike]], port: int = SSH_PORT,
                 *, serializer: Union[Serializer, Sequence[Serializer]],
                 username: str = None, password: str = None, names: MaybeStr = None):
        if isinstance(storage, (str, Path)):
            storage = [storage]
        names = to_seq(names)
        options = [RemoteOptions(hostname, Path(path), port, username, password) for path in storage]
        super().__init__(options, _resolve_serializer(serializer), names)


class HashDigest(BaseLayer):
    def __init__(self, names: MaybeStr):
        super().__init__(HashDigestContainer(to_seq(names)))
