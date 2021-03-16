import pytest

from connectome import Mixin, Local, Source, meta, Transform


class SizeMixin(Mixin):
    def _size(i):
        return len(i)

    def delta(text: Local, _size):
        return len(text) - _size


class SizePositive(Mixin):
    def positive(delta: Local):
        return delta > 0


class Plain(Source):
    @meta
    def ids():
        return tuple('0123456789')

    def text(i):
        return f'text for id {i}'

    def _size(i):
        return len(i)

    def delta(text: Local, _size):
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


def test_mixins():
    one = Plain()
    two = Inherited()
    three = Multiple()
    assert one.ids == two.ids == three.ids

    for i in one.ids:
        assert one.text(i) == two.text(i) == three.text(i)
        assert one.delta(i) == two.delta(i) == three.delta(i)
        assert three.positive(i)


def test_inheritance():
    with pytest.raises(TypeError):
        class A(Mixin, int):
            pass
    with pytest.raises(TypeError):
        class B(Transform, SizeMixin):
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
