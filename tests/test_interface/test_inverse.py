import pytest

from connectome import Transform, Apply, inverse
from connectome.engine.compiler import identity
from connectome.exceptions import FieldError


def test_different_inputs():
    class A(Transform):
        __inherit__ = True

        def _y(y):
            return y

        @inverse
        def output(output, _y):
            return output

        @inverse
        def output2(output2, _y):
            return output2 * 2

    class B(Transform):
        def x(x, y):
            return x + y

        @inverse
        def output(output):
            return output

        @inverse
        def output2(output2):
            return output2 * 2

    dec = (A() >> B())._decorate('x', 'output')

    @dec
    def func(x):
        return x

    assert func(x=1, y=2) == 3

    dec = (A() >> B())._decorate('x', ('output', 'output2'))

    @dec
    def func(x):
        return x, x

    assert func(x=1, y=2) == (3, 12)


def test_unused():
    class A(Transform):
        def x(x):
            return x

        @inverse
        def y(y):
            return y

        @inverse
        def z(z):
            return z

    class B(Transform):
        __inherit__ = 'z'

        def x(x):
            return x

        @inverse
        def y(y):
            return y

    assert (B() >> A())._wrap(lambda x: x, 'x', 'y')(1) == 1


def test_errors():
    with pytest.raises(ValueError, match='duplicates'):
        Transform()._wrap(lambda x: x, ('x', 'x'), 'y')
    with pytest.raises(ValueError, match='duplicates'):
        Transform()._wrap(lambda x: x, 'x', ('y', 'y'))


def test_inherit():
    class A(Transform):
        __inherit__ = 'image'

        @inverse
        def image(image):
            return image

    ds = A() >> Transform(__inherit__=True)
    dec = ds._decorate('image')

    @dec
    def func(x):
        return x

    assert func(1) == 1

    with pytest.raises(FieldError):
        ds._wrap(identity, 'some-other-name')

    dec = (A() >> Apply(image=lambda image: image))._decorate('image', 'image')

    @dec
    def func(image):
        return 2 * image

    assert func(image=4) == 8
