import warnings
import weakref
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union, Sequence, Container as ContainerType

import numpy as np
from tarn import RemoteStorage
from tarn.cache import CacheIndex
from tarn.config import init_storage, StorageConfig

from .base import Layer
from ..containers import EdgesBag, Container, IdentityContext
from ..serializers import Serializer, ChainSerializer, JsonSerializer, NumpySerializer, PickleSerializer
from ..storage import Storage, Disk
from ..utils import PathLike, StringsLike, node_to_dict, to_seq, AntiSet
from ..engine import TreeNode, Node, ImpureEdge, CacheEdge, IdentityEdge, Details
from ..cache import Cache, MemoryCache, DiskCache

PathLikes = Union[PathLike, Sequence[PathLike]]
RemoteStorageLike = Union[RemoteStorage, Sequence[RemoteStorage]]
SerializersLike = Union[Serializer, Sequence[Serializer]]


class CacheLayer(Layer, ABC):
    def __init__(self, container: Container = None):
        # TODO: legacy
        if container is not None:
            warnings.warn('Passing a container to CacheLayer is deprecated', UserWarning)
            warnings.warn('Passing a container to CacheLayer is deprecated', DeprecationWarning)

        self._container = container

    def __repr__(self):
        return self.__class__.__name__

    def _connect(self, previous: EdgesBag) -> EdgesBag:
        if self._container is None:
            raise NotImplementedError
        # TODO: legacy
        return self._container.wrap(previous)


class CacheToStorage(CacheLayer):
    def __init__(self, names: Union[ContainerType[str], None], impure: bool = False):
        super().__init__()
        if names is None:
            names = AntiSet()
        self.names = names
        self.impure = impure

    @abstractmethod
    def _get_storage(self) -> Cache:
        """ Create a cache storage instance """

    def _connect(self, previous: EdgesBag) -> EdgesBag:
        previous = previous.freeze()
        forward_outputs = node_to_dict(previous.outputs)

        details = Details(type(self))
        edges = list(previous.edges)
        outputs = [Node(name, details) for name in forward_outputs]
        mapping = TreeNode.from_edges(previous.edges)
        virtuals = previous.virtual_nodes - set(forward_outputs)

        for node in outputs:
            name = node.name
            output = forward_outputs[name]

            if name in self.names:
                if not self.impure:
                    self._detect_impure(mapping[output], name)
                edges.append(CacheEdge(self._get_storage()).bind(output, node))
            else:
                edges.append(IdentityEdge().bind(output, node))

        # TODO: proper support for optionals
        return EdgesBag(
            previous.inputs, outputs, edges, IdentityContext(), persistent_nodes=previous.persistent_nodes,
            virtual_nodes=virtuals, optional_nodes=None,
        )

    @staticmethod
    def _detect_impure(node: TreeNode, name: str):
        if node.is_leaf:
            return

        if isinstance(node.edge, ImpureEdge):
            raise ValueError(f'You are trying to cache the field "{name}", '
                             f'which has an `impure` dependency - "{node.name}"')

        for parent in node.parents:
            CacheToStorage._detect_impure(parent, name)


class CacheToRam(CacheToStorage):
    """
    Caches the fields from ``names`` to RAM.

    If ``size`` is not None - an LRU cache is used.
    """

    def __init__(self, names: StringsLike = None, *, size: int = None, impure: bool = False):
        super().__init__(names=names, impure=impure)
        self._cache_instances = weakref.WeakSet()
        self._size = size

    def _clear(self):
        """ Clears all the values cached by this layer. """
        for cache in self._cache_instances:
            cache.clear()

    def _get_storage(self):
        cache = MemoryCache(self._size)
        self._cache_instances.add(cache)
        return cache


class CacheToDisk(CacheToStorage):
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
        super().__init__(names=names, impure=impure)
        self.storage = DiskCache(local, remote, fetch=bool(remote))

    def _get_storage(self) -> Cache:
        return self.storage

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


def _normalize_disk_arguments(local, remote, names, serializer, storage):
    names = to_seq(names)
    serializer = _resolve_serializer(serializer)
    if isinstance(local, (str, Path)):
        local = local,
    if isinstance(remote, RemoteStorage):
        remote = remote,
    local = [CacheIndex(root, storage, serializer) for root in local]
    return names, local, remote


def _resolve_serializer(serializer):
    if not isinstance(serializer, Serializer):
        serializer = ChainSerializer(*serializer)
    return serializer
