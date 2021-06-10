import numpy as np

from connectome import Transform


def test_building():
    class A(Transform):
        _a: int

        # np.array is a function, which, strictly speaking, is not a type
        def f(x: str, _a: np.array):
            return _a
