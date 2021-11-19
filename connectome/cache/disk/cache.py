import logging
from typing import Any, Tuple, Sequence

from .index import CacheIndexStorage
from ...engine import NodeHash
from ..base import Cache
from ..pickler import dumps, PREVIOUS_VERSIONS
from ...storage.interface import Key, RemoteStorage, HashStorage

logger = logging.getLogger(__name__)


class DiskCache(Cache):
    def __init__(self, local: Sequence[CacheIndexStorage], remote: Sequence[RemoteStorage], fetch: bool):
        super().__init__()
        self.cache = HashStorage(local, remote)
        self.algorithm = self.cache.hash.build()
        self.fetch = fetch

    def prepare(self, param: NodeHash) -> Tuple[Key, Any]:
        raw = param.value
        pickled, key = key_to_digest(self.algorithm, raw)
        # OPTIMIZATION: if PREVIOUS_VERSIONS is empty can return just the pickled part
        return key, (raw, pickled)

    def get(self, key: Key, context) -> Tuple[Any, bool]:
        raw, pickled = context
        logger.info('Reading %s', key)

        # try to load
        value, exists = self.cache.read(key, pickled, fetch=self.fetch)
        if exists:
            return value, exists

        # the cache is empty, but we can try and restore it from an older version
        for version in reversed(PREVIOUS_VERSIONS):
            local_pickled, local_digest = key_to_digest(self.algorithm, raw, version)

            # we can simply load the previous version, because nothing really changed
            value, exists = self.cache.read(local_digest, local_pickled, fetch=self.fetch)
            if exists:
                # and store it for faster access next time
                self.cache.write(key, value, pickled)
                return value, exists

        return None, False

    def set(self, key: Key, value: Any, context):
        raw, pickled = context
        logger.info('Saving %s', key)
        self.cache.write(key, value, pickled)


def key_to_digest(algorithm, key, version=None):
    pickled = dumps(key, version=version)
    digest = algorithm(pickled).hexdigest()
    return pickled, digest
