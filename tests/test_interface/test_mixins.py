import warnings
from typing import Callable

import pytest

from connectome import Mixin, Output, Source, Transform, inverse, meta


class SizeMixin(Mixin):
    def _size(i):
        return len(i)

    def delta(text: Output, _size):
        return len(text) - _size


class SizePositive(Mixin):
    def positive(delta: Output):
        return delta > 0


class Plain(Source):
    @meta
    def ids():
        return tuple('0123456789')

    def text(i):
        return f'text for id {i}'

    def _size(i):
        return len(i)

    def delta(text: Output, _size):
        return len(text) - _size


class Inherited(Source, SizeMixin):
    @meta
    def ids():
        return tuple('0123456789')

    def text(i):
        return f'text for id {i}'


class Multiple(Source, SizeMixin, SizePositive):
    @meta
    def ids():
        return tuple('0123456789')

    def text(i):
        return f'text for id {i}'


def test_source_mixins():
    one = Plain()
    two = Inherited()
    three = Multiple()
    assert one.ids == two.ids == three.ids

    for i in one.ids:
        assert one.text(i) == two.text(i) == three.text(i)
        assert one.delta(i) == two.delta(i) == three.delta(i)
        assert three.positive(i)


@pytest.mark.parametrize('nested', [False, True])
def test_transform_mixins(nested):
    class A(Mixin, SizeMixin):
        pass

    mixin = A if nested else SizeMixin
    with warnings.catch_warnings():
        warnings.simplefilter('error')

        class B(Transform, mixin):
            _size_copy = SizeMixin._size

            def double_size(_size_copy):
                return 2 * _size_copy

            def text(text):
                return text

    b = B()
    assert set(dir(b)) == {'delta', 'double_size', 'text'}
    assert b.double_size('123') == 6
    assert b.delta(text='abc', i='d') == 2


def test_external_callable_warning():
    class A(Mixin):
        _size = SizeMixin._size

        def size(_size):
            return _size

    with pytest.warns(UserWarning, match='Are you trying to pass a default value for an argument?'):
        class B(Transform, A):
            pass

    assert B().size('abc') == 3


def test_callable_mixin_default():
    with warnings.catch_warnings():
        warnings.simplefilter('error')

        class A(Transform, SizeMixin):
            _measure: Callable = SizeMixin._size

            def size(i, _measure):
                return _measure(i)

            def text(text):
                return text

    assert A().size('abc') == 3


def test_overwrite():
    class A(Mixin):
        def a(x):
            return x

        def b(x):
            return x

    with pytest.raises(RuntimeError):
        class B(Mixin, A):
            def b(x):
                return 2 * x

            def c(x):
                return x

    with pytest.raises(RuntimeError):
        class C(Source, A):
            def b(x):
                return 2 * x

            def c(x):
                return x

    with pytest.raises(RuntimeError):
        class D(Transform, A):
            def b(x):
                return 2 * x

            def c(x):
                return x


def test_inheritance():
    with pytest.raises(TypeError):
        class A(Mixin, int):
            pass
    with pytest.raises(TypeError):
        class C(Source, SizeMixin, int):
            pass
    with pytest.raises(TypeError):
        class D(Plain):
            pass
    with pytest.raises(TypeError):
        class E(Plain, SizeMixin):
            pass


def test_mixin_with_ids():
    class A(Mixin):
        @meta
        def ids():
            return tuple('012')

    class B(Source, A):
        pass

    assert B().ids == tuple('012')


def test_inverse():
    class A(Mixin):
        def f(f):
            return f

        @inverse
        def f(f):
            return f

    class B(Transform, A):
        pass

    b = B()
    assert 'f' in dir(b)
    f = b._wrap(lambda x: x, 'f')
    assert f(1) == 1
