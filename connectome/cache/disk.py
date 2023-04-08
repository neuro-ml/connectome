import logging

from tarn import PickleKeyStorage

from .base import Cache

logger = logging.getLogger(__name__)


class DiskCache(Cache):
    def __init__(self, pool: PickleKeyStorage):
        super().__init__()
        self.cache = pool

    def prepare(self, param):
        raw = param.value
        context = self.cache.prepare(raw)
        return context.digest, context

    def get(self, key, context):
        return self.cache.read(context, error=False)

    def set(self, key, value, context):
        self.cache.write(context, value, error=False)
