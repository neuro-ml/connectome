import re

import pytest

from connectome import Transform
from connectome.exceptions import GraphError, DependencyError


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

    with pytest.raises(GraphError, match='are both inherited and have defined edges'):
        class C(Transform):
            __inherit__ = 'x'

            def x(x):
                pass

        C()


def test_loopback():
    with pytest.raises(ValueError, match='The inputs contain duplicates'):
        Transform(__inherit__=True)._wrap(lambda x: x, inputs=['x', 'x'])


def test_missing_dependencies():
    class A(Transform):
        x = lambda x: x
        y = lambda y: y

    class B(Transform):
        __inherit__ = True

    class C(Transform):
        x = lambda x, y: 1
        y = lambda x, y: 2
        __inherit__ = True

    class D(Transform):
        x = lambda x, y, z: 2

    with pytest.raises(DependencyError, match=re.escape(
            '''The output 'x' (layer 'D' -> 'Chain') has unreachable inputs: 'z' (layer 'D' -> 'Chain')'''
    )):
        dir(A() >> B() >> C() >> D())
