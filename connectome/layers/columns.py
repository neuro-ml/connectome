from itertools import starmap
from math import ceil
from typing import Union, Generator, Any

from tqdm.auto import tqdm

from .cache import CacheLayer, SerializersLike, PathLikes, RemoteStorageLike, _normalize_disk_arguments
from ..cache import DiskCache, MemoryCache
from ..containers import EdgesBag, IdentityContext
from ..engine import (
    Edge, TreeNode, Node, Graph, Request, Response, HashOutput, Command, TupleHash, NodeHashes, NodeHash, Details
)
from ..exceptions import DependencyError
from ..storage import Storage
from ..utils import StringsLike, node_to_dict


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
        super().__init__()
        self.names = names
        self.shard_size = shard_size
        self.verbose = verbose
        self.disk = DiskCache(local, remote, bool(remote))
        self.ram = MemoryCache(None)

    def _connect(self, previous: EdgesBag) -> EdgesBag:
        main = previous.freeze()
        edges = list(main.edges)
        key, = main.inputs
        main_outputs = node_to_dict(main.outputs)
        # TODO: containers must know about property names
        property_name = 'ids'
        if property_name not in main_outputs:
            raise DependencyError(f'The previous layer must contain the "{property_name}" property.')

        keys = main_outputs[property_name]

        details = Details(type(self))
        copy = previous.freeze(details)
        mapping = TreeNode.from_edges(copy.edges)
        outputs_copy = node_to_dict(copy.outputs)
        graph_inputs = [mapping[copy.inputs[0]]]

        outputs = []
        for output in main.outputs:
            name = output.name
            if name not in self.names:
                outputs.append(output)

            else:
                local = Node(name, details)
                # build a graph for each node
                graph = Graph(graph_inputs, mapping[outputs_copy[name]])
                edges.append(CachedColumn(
                    self.disk, self.ram, graph, self.verbose, self.shard_size).bind([output, key, keys], local))
                outputs.append(local)

        return EdgesBag(
            [key], outputs, edges, IdentityContext(), persistent_nodes=main.persistent_nodes,
            optional_nodes=main.optional_nodes, virtual_nodes=main.virtual_nodes,
        )


class CachedColumn(Edge):
    """
    Edge Inputs
    -----------
    entry: the entry at ``key``
    key: a unique key for each entry in the tuple
    keys: all available keys
    """

    def __init__(self, disk: DiskCache, ram: MemoryCache, graph: Graph, verbose: bool,
                 shard_size: Union[int, float, None]):
        super().__init__(arity=3)
        self.graph = graph
        self.disk = disk
        self.ram = ram
        self.verbose = verbose
        self.shard_size = shard_size

    def compute_hash(self) -> Generator[Request, Response, HashOutput]:
        # propagate the first value
        value = yield Command.ParentHash, 0
        return value, None

    def _get_shard(self, key, keys):
        keys = sorted(keys)
        if key not in keys:
            raise ValueError(f'The key "{key}" is not present among the {len(keys)} keys cached by this layer')

        size = self.shard_size
        if size is None:
            return keys, 1, 0
        if isinstance(size, float):
            size = ceil(size * len(keys))

        assert size > 0
        idx = keys.index(key) // size
        count = ceil(len(keys) / size)
        start = idx * size
        keys = keys[start:start + size]
        assert key in keys
        return keys, count, idx

    def evaluate(self) -> Generator[Request, Response, Any]:
        output = yield Command.CurrentHash,
        value, exists = self.ram.raw_get(output)
        if exists:
            return value

        key = yield Command.ParentValue, 1
        keys = yield Command.ParentValue, 2
        keys, shards_count, shard_idx = self._get_shard(key, keys)

        hashes, states = [], []
        for k in keys:
            h, state = self.graph.get_hash(k)
            hashes.append(h)
            states.append(state)
            if k == key:
                assert output == h, (output, h)
        # TODO: hash the graph?
        compound = TupleHash(*hashes)

        digest, context = self.disk.prepare(compound)
        values, exists = self.disk.get(digest, context)
        if not exists:
            suffix = '' if shards_count == 1 else f' ({shard_idx}/{shards_count})'
            values = tuple(starmap(self.graph.get_value, tqdm(
                states, desc=f'Generating the columns cache{suffix}', disable=not self.verbose,
            )))
            self.disk.set(digest, values, context)

        for k, h, value in zip(keys, hashes, values):
            self.ram.raw_set(h, value)
            if k == key:
                result = value

        return result  # noqa

    def _hash_graph(self, inputs: NodeHashes) -> NodeHash:
        return inputs[0]
