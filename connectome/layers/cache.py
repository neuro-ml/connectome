import weakref
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Container as ContainerType, Sequence, Union

import numpy as np
import yaml
from tarn import DiskDict, HashKeyStorage, PickleKeyStorage
from tarn.config import StorageConfig, init_storage
from tarn.interface import MaybeLabels

from ..cache import Cache, DiskCache, MemoryCache
from ..containers import EdgesBag, IdentityContext
from ..engine import CacheEdge, Details, ImpureEdge, Node, TreeNode
from ..serializers import ChainSerializer, JsonSerializer, NumpySerializer, PickleSerializer, Serializer
from ..utils import AntiSet, PathLike, StringsLike, node_to_dict, to_seq
from .base import Layer
from .dynamic import DynamicConnectLayer

PathLikes = Union[PathLike, Sequence[PathLike]]
SerializersLike = Union[Serializer, Sequence[Serializer]]


class CacheLayer(Layer, ABC):
    def __repr__(self):
        return self.__class__.__name__

    @staticmethod
    def _detect_impure(node: TreeNode, name: str):
        if node.is_leaf:
            return

        if isinstance(node.edge, ImpureEdge):
            raise ValueError(f'You are trying to cache the field "{name}", '
                             f'which has an `impure` dependency - "{node.name}"')

        for parent in node.parents:
            CacheToStorage._detect_impure(parent, name)


class CacheToStorage(DynamicConnectLayer, CacheLayer):
    def __init__(self, names: Union[ContainerType[str], None], impure: bool = False):
        super().__init__()
        if names is None:
            names = AntiSet()
        self.names = names
        self.impure = impure

    @abstractmethod
    def _get_storage(self) -> Cache:
        """ Create a cache storage instance """

    def _prepare_container(self, previous: EdgesBag) -> EdgesBag:
        mapping = TreeNode.from_edges(previous.edges)
        forward_outputs = node_to_dict(previous.outputs)

        details = Details(type(self))
        inputs, outputs, edges = [], [], []
        for name in forward_outputs:
            if name in self.names:
                if not self.impure:
                    self._detect_impure(mapping[forward_outputs[name]], name)

                inp, out = Node(name, details), Node(name, details)
                inputs.append(inp)
                outputs.append(out)
                edges.append(CacheEdge(self._get_storage()).bind(inp, out))

        return EdgesBag(
            inputs, outputs, edges, IdentityContext(), persistent=None,
            virtual=AntiSet(node_to_dict(outputs)), optional=set(inputs) | set(outputs),
        )


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
    """

    def __init__(self, index: PathLikes, storage: HashKeyStorage, serializer: SerializersLike, names: StringsLike, *,
                 impure: bool = False, labels: MaybeLabels = None):
        super().__init__(names=names, impure=impure)
        names, serializer = _normalize_disk_arguments(names, serializer)
        self.storage = DiskCache(PickleKeyStorage(index, storage, serializer, algorithm=storage.algorithm),
                                 labels=labels)

    def _get_storage(self) -> Cache:
        return self.storage

    @classmethod
    def simple(cls, *names, root: PathLike, serializer: Union[Serializer, Sequence[Serializer]] = None,
               labels: MaybeLabels = None):
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

        # TODO: this is a bit of a legacy cleanup
        for c in index, storage:
            c = c / 'config.yml'
            with open(c) as file:
                config = yaml.safe_load(file)
            if 'version' in config:
                config.pop('version')
                with open(c, 'w') as file:
                    yaml.safe_dump(config, file)

        return cls(index, HashKeyStorage(DiskDict(storage)), serializer, names, labels=labels)


def _normalize_disk_arguments(names, serializer):
    return to_seq(names), _resolve_serializer(serializer)


def _resolve_serializer(serializer):
    if not isinstance(serializer, Serializer):
        serializer = ChainSerializer(*serializer)
    return serializer
