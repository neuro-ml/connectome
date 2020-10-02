from contextlib import suppress
from gzip import GzipFile
from pathlib import Path

import numpy as np


class SerializerError(Exception):
    pass


class Serializer:
    def save(self, value, folder: Path):
        """
        Saves the ``value`` to ``folder``.
        """
        raise NotImplementedError

    def load(self, folder: Path):
        """Loads the value from ``folder``."""
        raise NotImplementedError


def resolve_serializer(serializer):
    if serializer is None:
        serializer = NumpySerializer()
    if not isinstance(serializer, Serializer):
        serializer = ChainSerializer(*serializer)
    return serializer


class ChainSerializer(Serializer):
    def __init__(self, *serializers: Serializer):
        self.serializers = serializers

    def save(self, value, folder: Path):
        for serializer in self.serializers:
            with suppress(SerializerError):
                return serializer.save(value, folder)

        raise SerializerError(f'No serializer was able to save to {folder}.')

    def load(self, folder: Path):
        for serializer in self.serializers:
            with suppress(SerializerError):
                return serializer.load(folder)

        raise SerializerError(f'No serializer was able to load from {folder}.')


class NumpySerializer(Serializer):
    def __init__(self, compression: int = None):
        self.compression = compression

    def _choose_compression(self, value):
        if isinstance(self.compression, int) or self.compression is None:
            return self.compression

        if isinstance(self.compression, dict):
            for dtype in self.compression:
                if np.issubdtype(value.dtype, dtype):
                    return self.compression[dtype]

    def save(self, value, folder: Path):
        value = np.asarray(value)
        compression = self._choose_compression(value)
        if compression is not None:
            assert isinstance(compression, int)
            with GzipFile(folder / 'value.npy.gz', 'wb', compresslevel=compression, mtime=0) as file:
                np.save(file, value)

        else:
            np.save(folder / 'value.npy', value)

    def load(self, folder: Path):
        paths = list(folder.iterdir())
        if len(paths) != 1:
            raise SerializerError

        path, = paths
        if path.name == 'value.npy':
            return np.load(folder / 'value.npy')

        if path.name == 'value.npy.gz':
            with GzipFile(folder / 'value.npy.gz', 'rb') as file:
                return np.load(file)

        raise SerializerError
