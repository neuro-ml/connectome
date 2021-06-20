import numpy as np
import pytest

from connectome import Transform
from connectome.exceptions import FieldError


def test_building():
    class A(Transform):
        _a: int

        # np.array is a function, which, strictly speaking, is not a type
        def f(x: str, _a: np.array):
            return _a


def test_missing_param():
    with pytest.raises(FieldError):
        class A(Transform):
            def f(_param):
                return
