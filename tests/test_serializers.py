import numpy as np

from connectome import Transform
from connectome.serializers import JsonSerializer, NumpySerializer, PickleSerializer, DictSerializer


def test_serializers(disk_cache_factory):
    config = {
        JsonSerializer(): [1, 2, {'2': 3}, [1]],
        NumpySerializer(): [np.array(1, int), np.array(False, bool), np.array(1123.1123, float), np.array([5])],
        PickleSerializer(): [1, '2', 3.3, {1: 2}, np.array(2.5, float)],
        DictSerializer(JsonSerializer()): [{'a': 2, 'b': [1, 2, 3]}]
    }
    for s, values in config.items():
        with disk_cache_factory(['x'], s) as cache:
            ds = Transform(x=lambda x: x) >> cache
            for value in values:
                assert ds.x(value) == value
                assert ds.x(value) == value
