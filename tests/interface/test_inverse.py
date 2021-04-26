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


def test_missing_input():
    class A(Transform):
        __inherit__ = True

        def _param(param):
            return param

        @inverse
        def output(output, _param):
            return output, _param

    dec = A()._decorate('input', 'output')

    @dec
    def predict(x):
        return 1, 2
