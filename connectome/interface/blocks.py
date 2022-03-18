import operator
import weakref
from pathlib import Path
from typing import Union, Sequence, Callable, Iterable

import numpy as np

from tarn import RemoteStorage
from tarn.cache import CacheIndex
from tarn.config import init_storage, StorageConfig

from .base import BaseLayer, CallableLayer
from ..containers.cache import MemoryCacheContainer, DiskCacheContainer, CacheColumnsContainer
from ..containers.debug import HashDigestContainer
from ..containers.filter import FilterContainer
from ..containers.goup import GroupContainer, MultiGroupLayer
from ..containers.join import JoinContainer
from ..containers.merge import SwitchContainer
from ..containers.transform import TransformContainer
from ..engine.base import Node
from ..engine.edges import FunctionEdge
from ..serializers import Serializer, ChainSerializer, JsonSerializer, NumpySerializer, PickleSerializer
from ..storage import Storage, Disk
from ..utils import PathLike, StringsLike, AntiSet
from .utils import format_arguments


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

        persistent = tuple(set.intersection(*(set(layer._container.persistent_nodes) for layer in layers)))
        super().__init__(SwitchContainer(
            id2dataset_index, [s._container for s in layers], ids_name, persistent),
            properties,
        )
        self._layers = layers

    def __repr__(self):
        return 'Merge' + format_arguments(self._layers)


class Join(CallableLayer):
    def __init__(self, left: CallableLayer, right: CallableLayer, on: StringsLike, pair_to_id: Callable):
        super().__init__(JoinContainer(left._container, right._container, to_seq(on), pair_to_id), ['ids'])


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

    def __init__(self, predicate: Callable, verbose: bool = False):
        super().__init__(FilterContainer(predicate, verbose))

    @classmethod
    def drop(cls, ids: Iterable[str], verbose: bool = False):
        """Removes the provided ``ids`` from the dataset."""
        assert not isinstance(ids, str)
        ids = tuple(sorted(set(ids)))
        assert all(isinstance(i, str) for i in ids)
        return cls(lambda id: id not in ids, verbose=verbose)

    @classmethod
    def keep(cls, ids: Iterable[str], verbose: bool = False):
        """Removes all the ids not present in ``ids``."""
        assert not isinstance(ids, str)
        ids = tuple(sorted(set(ids)))
        assert all(isinstance(i, str) for i in ids)
        return cls(lambda id: id in ids, verbose=verbose)

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


class Apply(CallableLayer):
    """
    A layer that applies separate functions to each of the specified names.

    `Apply` provides a convenient shortcut for transformations that only depend on the previous value of the name.

    Examples
    --------
    >>> Apply(image=zoom, mask=zoom_binary)
    >>> # is the same as using
    >>> class Zoom(Transform):
    >>>     __inherit__ = True
    >>>
    >>>     def image(image):
    >>>         return zoom(image)
    >>>
    >>>     def mask(mask):
    >>>         return zoom_binary(mask)
    """

    def __init__(self, **transform: Callable):
        self._names = sorted(transform)

        inputs, outputs, edges = [], [], []
        for name, func in transform.items():
            inp, out = Node(name), Node(name)
            inputs.append(inp)
            outputs.append(out)
            edges.append(FunctionEdge(func, arity=1).bind(inp, out))

        super().__init__(TransformContainer(
            inputs, outputs, edges, forward_virtual=AntiSet(transform), backward_virtual=AntiSet()
        ), ())

    def __repr__(self):
        args = ', '.join(self._names)
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
    """
    Caches the fields from ``names`` to RAM.

    If ``size`` is not None - a LRU cache is used.
    """

    def __init__(self, names: StringsLike = None, *, size: int = None, impure: bool = False):
        self._cache_instances = weakref.WeakSet()
        super().__init__(MemoryCacheContainer(names, size, impure, self._cache_instances))

    def _clear(self):
        """ Clears all the values cached by this layer. """
        for cache in self._cache_instances:
            cache.clear()


PathLikes = Union[PathLike, Sequence[PathLike]]
RemoteStorageLike = Union[RemoteStorage, Sequence[RemoteStorage]]
SerializersLike = Union[Serializer, Sequence[Serializer]]


class CacheToDisk(CacheLayer):
    """
    A persistent cache stored on disk.

    Parameters
    ----------
    index
        the folder(s) where the cache index is stored
    storage
        the storage which holds the actual data
    serializer
        the serializer used to save and load the data
    names
        field names that will be cached
    impure
        whether to allow caching of `impure` functions
    remote
        remote locations that are used to fetch the cache from (if available)
    """

    def __init__(self, index: PathLikes, storage: Storage, serializer: SerializersLike, names: StringsLike, *,
                 impure: bool = False, remote: RemoteStorageLike = ()):
        names, local, remote = _normalize_disk_arguments(index, remote, names, serializer, storage)
        super().__init__(DiskCacheContainer(names, local, remote, impure))

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
            init_storage(StorageConfig(hash='sha256', levels=[1, 31]), index)
            init_storage(StorageConfig(hash='sha256', levels=[1, 31]), storage)
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
    """
    A combination of a persistent cache stored on disk and a memory cache.
    The entries are stored on disk in shards, which speeds up read/write operations for large numbers of small files.

    Parameters
    ----------
    index
        the folder(s) where the cache index is stored
    storage
        the storage which holds the actual data
    serializer
        the serializer used to save and load the data
    names
        field names that will be cached
    remote
        remote locations that are used to fetch the cache from (if available)
    verbose
        whether to show a progressbar during cache generation
    shard_size
        the size of a disk storage shard. If int - an absolute size value is used,
        if float - a portion relative to the dataset is used,
        if None - all the entries are grouped in a single shard
    """

    def __init__(self, index: PathLikes, storage: Storage, serializer: SerializersLike, names: StringsLike, *,
                 verbose: bool = False, shard_size: Union[int, float, None] = None, remote: RemoteStorageLike = ()):
        if shard_size == 1:
            raise ValueError(f'Shard size of 1 is ambiguous. Use None if you want to have a single shard')
        names, local, remote = _normalize_disk_arguments(index, remote, names, serializer, storage)
        super().__init__(CacheColumnsContainer(names, local, remote, verbose=verbose, shard_size=shard_size))


class HashDigest(BaseLayer):
    def __init__(self, names: StringsLike):
        super().__init__(HashDigestContainer(to_seq(names)))


def _normalize_disk_arguments(local, remote, names, serializer, storage):
    names = to_seq(names)
    serializer = _resolve_serializer(serializer)
    if isinstance(local, (str, Path)):
        local = local,
    if isinstance(remote, RemoteStorage):
        remote = remote,
    local = [CacheIndex(root, storage, serializer) for root in local]
    return names, local, remote
