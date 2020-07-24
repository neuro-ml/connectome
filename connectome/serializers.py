from contextlib import suppress
from pathlib import Path

import numpy as np


class SerializerError(Exception):
    pass


class Serializer:
    def save(self, value, folder: Path) -> int:
        """
        Saves the ``value`` to ``folder``.
        Returns the occupied space in bytes.
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

    def save(self, value, folder: Path) -> int:
        for serializer in self.serializers:
            with suppress(SerializerError):
                return serializer.save(value, folder)

        raise SerializerError(f'No serializer was able to save to {folder}.')

    def load(self, folder: Path):
        for serializer in self.serializers:
            with suppress(SerializerError):
                return serializer.load(folder)

        raise SerializerError(f'No serializer was able to load from {folder}.')


# TODO: gzip
class NumpySerializer(Serializer):
    def save(self, value, folder: Path) -> int:
        np.save(folder / 'value.npy', value)
        return value.nbytes

    def load(self, folder: Path):
        return np.load(folder / 'value.npy')
