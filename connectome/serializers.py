from contextlib import suppress
from pathlib import Path

import numpy as np


class SerializerError(Exception):
    pass


class Serializer:
    def save(self, value, path: Path):
        raise NotImplementedError

    def load(self, path):
        raise NotImplementedError


class ChainSerializer(Serializer):
    def __init__(self, *serializers: Serializer):
        self.serializers = serializers

    def save(self, value, path: Path):
        for serializer in self.serializers:
            with suppress(SerializerError):
                serializer.save(value, path)
                return

        raise SerializerError(f'No serializer was able to save to {path}.')

    def load(self, path: Path):
        for serializer in self.serializers:
            with suppress(SerializerError):
                return serializer.load(path)

        raise SerializerError(f'No serializer was able to load from {path}.')


class NumpySerializer(Serializer):
    def save(self, value, path: Path):
        np.save(path / 'value.npy', value)

    def load(self, path):
        return np.load(path / 'value.npy')
