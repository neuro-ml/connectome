import pytest

from connectome import Transform
from connectome.exceptions import GraphError


def test_normalization():
    with pytest.raises(GraphError, match='multiple'):
        class A(Transform):
            def a(i):
                pass

            def a(j):
                pass

        A()

    with pytest.raises(GraphError, match='cycle'):
        class B(Transform):
            def _a(_b):
                pass

            def _b(_a):
                pass

        B()
