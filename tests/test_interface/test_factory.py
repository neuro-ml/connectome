import re

import numpy as np
import pytest

from connectome import Transform, Function, Source, meta
from connectome.exceptions import FieldError, GraphError


def test_building():
    class A(Transform):
        _a: int

        # np.array is a function, which, strictly speaking, is not a type
        def f(x: str, _a: np.array):
            return _a


def test_missing_param():
    with pytest.raises(GraphError):
        class A(Transform):
            def f(_param):
                pass

        dir(A())


def test_builtin_decorators():
    with pytest.raises(FieldError, match='not supported'):
        class A(Transform):
            @property
            def f():
                pass

    for decorator in [staticmethod, classmethod, property]:
        with pytest.raises(FieldError, match='private'):
            class B(Transform):
                @decorator
                def _f():
                    pass

    class C(Transform):
        _a: int

        def f(_a):
            return _a

        def g(_a):
            return C.util(_a)

        @staticmethod
        def util(x):
            return x * 10

        @classmethod
        def add_one(cls, x):
            return cls(a=x + 1)

    c = C(a=0)
    assert c.f() == 0
    assert c.g() == 0
    c = C.add_one(0)
    assert c.f() == 1
    assert c.g() == 10

    assert C.util(1) == 10
    # instances don't have access to them
    with pytest.raises(AttributeError):
        c.add_one(0)
    with pytest.raises(AttributeError):
        c.util(0)


def test_explicit_definitions():
    class A(Transform):
        x = Function(lambda a, b, c: [a, b, c], 'name1', c='name3', b='name2')

    assert A().x(name1=1, name2=2, name3=3) == [1, 2, 3]


def test_multiple_keys():
    with pytest.raises(FieldError, match=re.escape("Trying to use multiple arguments as keys: ('x', 'y')")):
        class A(Source):
            @meta
            def ids():
                return

            def f(x, y):
                return
