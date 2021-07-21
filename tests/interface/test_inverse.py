import pytest

from connectome import Transform, inverse


def test_different_inputs():
    class A(Transform):
        __inherit__ = True

        def _y(y):
            return y

        @inverse
        def output(output, _y):
            return output

    class B(Transform):
        def x(x, y):
            return x + y

        @inverse
        def output(output):
            return output

    dec = (A() >> B())._decorate('x', 'output')

    @dec
    def func(x):
        return x

    assert func(x=1, y=2) == 3


@pytest.mark.xfail
def test_inherit():
    class A(Transform):
        __inherit__ = 'image'

        @inverse
        def image(image):
            return image

    dec = (A() >> Transform(__inherit__=True))._decorate('image')

    @dec
    def func(x):
        return x

    assert func(1) == 1
