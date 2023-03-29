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
    # instances have access to them too
    assert c.util(2) == 20
    assert c.add_one(0)


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


def test_exclude():
    class A(Transform):
        __exclude__ = 'a', 'b'

    a = A()
    assert a.f(1) == 1
    with pytest.raises(AttributeError):
        a.a(1)

    # multiple values
    a = Transform(__exclude__=('a', 'b'))
    assert a.f(1) == 1
    with pytest.raises(AttributeError):
        a.a(1)

    # single value
    a = Transform(__exclude__='a')
    assert a.f(1) == 1
    with pytest.raises(AttributeError):
        a.a(1)


def test_bad_exclude():
    with pytest.raises(TypeError):
        class A(Transform):
            __exclude__ = 1
    with pytest.raises(TypeError):
        class B(Transform):
            __exclude__ = 1, 2, 3
    with pytest.raises(ValueError):
        class C(Transform):
            __inherit__ = 'a'
            __exclude__ = 'b'


def test_bad_inherit():
    with pytest.raises(TypeError):
        class A(Transform):
            __inherit__ = 1

    with pytest.raises(TypeError):
        class B(Transform):
            __inherit__ = 1, 2, 3
