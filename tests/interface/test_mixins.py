import pytest

from connectome import Mixin, Source, meta, Transform, Output, inverse


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


def test_transform_mixins():
    class B(Transform, SizeMixin):
        def double_size(_size):
            return 2 * _size

        def text(text):
            return text

    b = B()
    assert set(dir(b)) == {'delta', 'double_size', 'text'}
    assert b.double_size('123') == 6
    assert b.delta(text='abc', i='d') == 2


def test_overwrite():
    class A(Mixin):
        def a(x):
            return x

        def b(x):
            return x

    with pytest.raises(RuntimeError):
        class B(A):
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
        def f(x):
            pass

        @inverse
        def f(x):
            pass

    class B(Transform, A):
        pass

    assert 'f' in dir(B())
