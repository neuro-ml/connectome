import operator
from pathlib import Path
from typing import Union, Sequence, Callable

import numpy as np
from paramiko.config import SSH_PORT

from .base import BaseLayer, CallableLayer
from ..containers.cache import MemoryCacheContainer, DiskCacheContainer, RemoteStorageContainer, CacheColumnsContainer
from ..containers.debug import HashDigestContainer
from ..containers.filter import FilterContainer
from ..containers.goup import GroupContainer, MultiGroupLayer
from ..containers.merge import SwitchContainer
from ..containers.shortcuts import ApplyContainer
from ..serializers import Serializer, ChainSerializer, JsonSerializer, NumpySerializer, PickleSerializer
from ..storage import Storage, Disk
from ..storage.config import init_storage
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
    """ Caches the fields from ``names`` to RAM. """

    def __init__(self, names: MaybeStr = None, size: int = None):
        super().__init__(MemoryCacheContainer(names, size))


class CacheToDisk(CacheLayer):
    """
    A persistent cache stored on disk.

    Parameters
    ----------
    index
        the folder where the cache index is stored
    storage
        the storage which holds the actual data
    serializer
        the serializer used to save and load the data
    names
        field names that will be cached
    """

    def __init__(self, index: PathLike, storage: Storage, serializer: Union[Serializer, Sequence[Serializer]],
                 names: MaybeStr):
        names = to_seq(names)
        super().__init__(DiskCacheContainer(names, index, storage, _resolve_serializer(serializer)))

    @classmethod
    def simple(cls, *names, root: PathLike, serializer: Union[Serializer, Sequence[Serializer]] = None):
        """
        A simple version of caching to disk with adequate default settings.

        Parameters
        ----------
        names:
            the field names to cache
        root:
            the folder where the cache will be stored
        serializer
            the serializer used to save and load the data
        """
        root = Path(root)
        root.mkdir(exist_ok=True, parents=True)
        children = set(root.iterdir())
        index = root / 'index'
        storage = root / 'storage'

        if not children:
            init_storage(index, algorithm={'name': 'sha256'}, levels=[1, 31])
            init_storage(storage, algorithm={'name': 'sha256'}, levels=[1, 31])
        elif children != {index, storage}:
            names = tuple(x.name for x in children)
            raise FileNotFoundError(
                f'The root is expected to contain the "index" and "storage" folders, but found {names}'
            )

        if serializer is None:
            serializer = ChainSerializer(
                JsonSerializer(),
                NumpySerializer({np.bool_: 1, np.int_: 1}),
                PickleSerializer(),
            )

        return cls(index, Storage([Disk(storage)]), serializer, names)


class CacheColumns(CacheLayer):
    def __init__(self, root: PathLike, storage: Storage,
                 serializer: Union[Serializer, Sequence[Serializer]],
                 names: MaybeStr, verbose: bool = False):
        names = to_seq(names)
        super().__init__(CacheColumnsContainer(names, root, storage, _resolve_serializer(serializer), verbose=verbose))


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
