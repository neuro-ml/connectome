from pathlib import Path
from typing import Union, Sequence

from .base import FromLayer, CallableBlock
from ..layers.cache import MemoryCacheLayer, DiskCacheLayer, RemoteStorageLayer, CacheRowsLayer
from ..layers.merge import SwitchLayer
from ..layers.shortcuts import ApplyLayer
from ..serializers import Serializer, resolve_serializer
from ..storage.disk import DiskOptions
from ..storage.relative_remote import RemoteOptions, SSH_PORT
from ..utils import PathLike


class Merge(CallableBlock):
    def __init__(self, *blocks: CallableBlock):
        super().__init__()

        id2dataset_index = {}
        for index, dataset in enumerate(blocks):
            intersection = set(dataset.ids) & set(id2dataset_index.keys())
            if intersection:
                raise RuntimeError(f'Ids {intersection} are duplicated in merged datasets.')

            id2dataset_index.update({i: index for i in dataset.ids})

        def branch_selector(identifier):
            try:
                return id2dataset_index[identifier]
            except KeyError:
                raise ValueError(f'Identifier {identifier} not found.') from None

        self._layer = SwitchLayer(branch_selector, *(s._layer for s in blocks))


class Apply(FromLayer):
    def __init__(self, **transform):
        super().__init__(ApplyLayer(transform))


class CacheToRam(FromLayer):
    def __init__(self, names: Sequence[str] = None, size: int = None):
        super().__init__(MemoryCacheLayer(names, size))


class CacheToDisk(FromLayer):
    def __init__(self, root: PathLike, *storage: Union[PathLike, DiskOptions],
                 serializers: Union[Serializer, Sequence[Serializer]] = None,
                 names: Sequence[str] = None, metadata: dict = None):
        storage = [s if isinstance(s, DiskOptions) else DiskOptions(Path(s)) for s in storage]
        super().__init__(DiskCacheLayer(names, root, storage, resolve_serializer(serializers), metadata or {}))


class CacheRows(FromLayer):
    def __init__(self, root: PathLike, *storage: Union[PathLike, DiskOptions],
                 serializers: Union[Serializer, Sequence[Serializer]] = None,
                 names: Sequence[str] = None, metadata: dict = None):
        storage = [s if isinstance(s, DiskOptions) else DiskOptions(Path(s)) for s in storage]
        super().__init__(CacheRowsLayer(names, root, storage, resolve_serializer(serializers), metadata or {}))


class RemoteStorageBase(FromLayer):
    def __init__(self, options: Sequence[RemoteOptions],
                 serializers: Union[Serializer, Sequence[Serializer]] = None,
                 names: Sequence[str] = None):
        super().__init__(RemoteStorageLayer(names, options, resolve_serializer(serializers)))


class RemoteStorage(RemoteStorageBase):
    def __init__(self, hostname: str, storage: Union[PathLike, Sequence[PathLike]], port: int = SSH_PORT,
                 serializers: Union[Serializer, Sequence[Serializer]] = None,
                 username: str = None, password: str = None, names: Sequence[str] = None):
        if isinstance(storage, (str, Path)):
            storage = [storage]

        options = [RemoteOptions(hostname, Path(path), port, username, password) for path in storage]
        super().__init__(options, serializers, names)
