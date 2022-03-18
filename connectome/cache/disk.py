import logging
from typing import Sequence

from tarn.cache import CacheIndex, CacheStorage

from .base import Cache

logger = logging.getLogger(__name__)


class DiskCache(Cache):
    def __init__(self, local: Sequence[CacheIndex], remote, fetch: bool):
        super().__init__()
        self.cache = CacheStorage(local, remote=remote)
        self.fetch = fetch

    def prepare(self, param):
        raw = param.value
        context = self.cache.prepare(raw)
        return context.digest, context

    def get(self, key, context):
        return self.cache.read(context, fetch=self.fetch, error=False)

    def set(self, key, value, context):
        self.cache.write(context, value, error=False)
