import operator
from pathlib import Path
from typing import Union, Sequence, Callable
from paramiko.config import SSH_PORT

from .base import BaseBlock, CallableBlock
from ..layers.cache import MemoryCacheLayer, DiskCacheLayer, RemoteStorageLayer, CacheRowsLayer
from ..layers.debug import HashDigestLayer
from ..layers.filter import FilterLayer
from ..layers.goup import GroupLayer, MultiGroupLayer
from ..layers.merge import SwitchLayer
from ..layers.shortcuts import ApplyLayer
from ..serializers import Serializer, ChainSerializer
from ..storage import DiskOptions, RemoteOptions
from ..utils import PathLike
from .utils import MaybeStr


class Merge(CallableBlock):
    def __init__(self, *blocks: CallableBlock):
        properties = [set(block._properties) for block in blocks]
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
        for index, dataset in enumerate(blocks):
            ids = getattr(dataset, ids_name)
            intersection = set(ids) & set(id2dataset_index)
            if intersection:
                raise RuntimeError(f'Ids {intersection} are duplicated in merged datasets.')

            id2dataset_index.update({i: index for i in ids})

        super().__init__(SwitchLayer(id2dataset_index, [s._layer for s in blocks]), properties)


class Filter(BaseBlock):
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
        super().__init__(FilterLayer(predicate))

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


class GroupBy(BaseBlock):
    def __init__(self, name: str):
        super().__init__(GroupLayer(name))

    @staticmethod
    def _multiple(*names, **comparators):
        assert set(comparators).issubset(names)
        for name in names:
            comparators.setdefault(name, operator.eq)
        return BaseBlock(MultiGroupLayer(comparators))


class Apply(BaseBlock):
    def __init__(self, **transform: Callable):
        super().__init__(ApplyLayer(transform))


def to_seq(x):
    if isinstance(x, str):
        x = [x]
    return x


def _resolve_serializer(serializer):
    if not isinstance(serializer, Serializer):
        serializer = ChainSerializer(*serializer)
    return serializer


class CacheBlock(BaseBlock):
    pass


class CacheToRam(CacheBlock):
    def __init__(self, names: MaybeStr = None, size: int = None):
        super().__init__(MemoryCacheLayer(names, size))


class CacheToDisk(CacheBlock):
    def __init__(self, root: PathLike, *storage: Union[PathLike, DiskOptions],
                 serializer: Union[Serializer, Sequence[Serializer]],
                 names: MaybeStr, metadata: dict = None):
        storage = [s if isinstance(s, DiskOptions) else DiskOptions(Path(s)) for s in storage]
        names = to_seq(names)
        super().__init__(DiskCacheLayer(names, root, storage, _resolve_serializer(serializer), metadata or {}))


class CacheRows(CacheBlock):
    def __init__(self, root: PathLike, *storage: Union[PathLike, DiskOptions],
                 serializer: Union[Serializer, Sequence[Serializer]],
                 names: MaybeStr, metadata: dict = None):
        storage = [s if isinstance(s, DiskOptions) else DiskOptions(Path(s)) for s in storage]
        names = to_seq(names)
        super().__init__(CacheRowsLayer(names, root, storage, _resolve_serializer(serializer), metadata or {}))


class RemoteStorageBase(CacheBlock):
    def __init__(self, options: Sequence[RemoteOptions],
                 serializer: Union[Serializer, Sequence[Serializer]], names: MaybeStr = None):
        names = to_seq(names)
        super().__init__(RemoteStorageLayer(names, options, _resolve_serializer(serializer)))


class RemoteStorage(RemoteStorageBase):
    def __init__(self, hostname: str, storage: Union[PathLike, Sequence[PathLike]], port: int = SSH_PORT,
                 *, serializer: Union[Serializer, Sequence[Serializer]],
                 username: str = None, password: str = None, names: MaybeStr = None):
        if isinstance(storage, (str, Path)):
            storage = [storage]
        names = to_seq(names)
        options = [RemoteOptions(hostname, Path(path), port, username, password) for path in storage]
        super().__init__(options, _resolve_serializer(serializer), names)


class HashDigest(BaseBlock):
    def __init__(self, names: Sequence[str]):
        super().__init__(HashDigestLayer(names))
