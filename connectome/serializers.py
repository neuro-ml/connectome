from typing import Union, Dict

from contextlib import suppress
from gzip import GzipFile
from pathlib import Path

import os
import json
import pickle
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


class JsonSerializer(Serializer):
    def save(self, value, folder: Path):
        try:
            value = json.dumps(value)
        except TypeError as e:
            raise SerializerError from e

        with open(folder / 'value.json', 'w') as file:
            file.write(value)

    def load(self, folder: Path):
        paths = list(folder.iterdir())
        if len(paths) != 1:
            raise SerializerError

        path, = paths
        if path.name != 'value.json':
            raise SerializerError

        with open(folder / 'value.json', 'r') as file:
            return json.load(file)


class PickleSerializer(Serializer):
    def save(self, value, folder):
        try:
            value = pickle.dumps(value)
        except TypeError as e:
            raise SerializerError from e
        
        with open(folder / 'value.pkl', 'wb') as file:
            file.write(value)

    def load(self, folder):
        paths = list(folder.iterdir())
        if len(paths) != 1:
            raise SerializerError

        path, = paths
        if path.name != 'value.pkl':
            raise SerializerError

        with open(folder / 'value.pkl', 'rb') as file:
            return pickle.load(file)


class NumpySerializer(Serializer):
    def __init__(self, compression: Union[int, Dict[type, int]] = None):
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
            return np.load(folder / 'value.npy', allow_pickle=True)

        if path.name == 'value.npy.gz':
            with GzipFile(folder / 'value.npy.gz', 'rb') as file:
                return np.load(file, allow_pickle=True)

        raise SerializerError


class DictSerializer(Serializer):
    def __init__(self, serializer: Serializer):
        self.keys_filename = 'dict_keys.json'
        self.serializer = serializer

    def save(self, data: dict, folder: Path):
        if not isinstance(data, dict):
            raise SerializerError

        # TODO: remove all if at least one iteration fails
        keys_to_folder = {}
        for sub_folder, (key, value) in enumerate(data.items()):
            keys_to_folder[sub_folder] = key
            os.makedirs(folder / str(sub_folder), exist_ok=True)
            self.serializer.save(value, folder / str(sub_folder))

        with open(folder / self.keys_filename, 'w+') as f:
            json.dump(keys_to_folder, f)

    def load(self, folder: Path):
        keys = folder / self.keys_filename
        if not keys.exists():
            raise SerializerError

        with open(keys, 'r') as f:
            keys_map = json.load(f)

        data = {}
        for sub_folder, key in keys_map.items():
            data[key] = self.serializer.load(folder / sub_folder)
        return data
