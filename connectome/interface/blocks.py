from pathlib import Path
from typing import Union, Sequence

from .base import FromLayer, CallableBlock
from ..layers.cache import MemoryCacheLayer, DiskCacheLayer, RemoteStorageLayer
from ..layers.merge import SwitchLayer
from ..layers.shortcuts import ApplyLayer
from ..serializers import Serializer, resolve_serializer
from ..storage.disk import DiskOptions
from ..storage.remote import RemoteOptions, SSH_PORT
from ..utils import PathLike


class Merge(CallableBlock):
    def __init__(self, *blocks: CallableBlock):
        super().__init__()

        idx_sum = []
        for layer in blocks:
            idx_sum.extend(layer.ids())

        if len(idx_sum) != len(set(idx_sum)):
            raise RuntimeError('Datasets have same indices')

        def branch_selector(identifier):
            for idx, ds in enumerate(blocks):
                if identifier in ds.ids():
                    return idx

            raise ValueError(identifier)

        self._layer = SwitchLayer(branch_selector, *(s._layer for s in blocks))


class Apply(FromLayer):
    def __init__(self, **transform):
        super().__init__(ApplyLayer(transform))


class CacheToRam(FromLayer):
    def __init__(self, names: Sequence[str] = None, size: int = None):
        super().__init__(MemoryCacheLayer(names, size))


class CacheToDisk(FromLayer):
    def __init__(self, *storage: Union[PathLike, DiskOptions],
                 serializers: Union[Serializer, Sequence[Serializer]] = None,
                 names: Sequence[str] = None, metadata: dict = None):
        storage = [s if isinstance(s, DiskOptions) else DiskOptions(Path(s)) for s in storage]
        super().__init__(DiskCacheLayer(names, storage, resolve_serializer(serializers), metadata or {}))


class RemoteStorage(FromLayer):
    def __init__(self, hostname: str, storage: Union[PathLike, Sequence[PathLike]], port: int = SSH_PORT,
                 serializers: Union[Serializer, Sequence[Serializer]] = None,
                 username: str = None, password: str = None, names: Sequence[str] = None):
        if isinstance(storage, (str, Path)):
            storage = [storage]

        options = [RemoteOptions(hostname, Path(path), port, username, password) for path in storage]
        super().__init__(RemoteStorageLayer(names, options, resolve_serializer(serializers)))
