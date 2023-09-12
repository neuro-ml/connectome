import pickle

import pytest

from connectome import Mixin, Source, Transform, meta


def test_subclasses():
    class A(Source):
        pass

    class B(Transform):
        pass

    class C(Mixin):
        pass

    assert issubclass(A, Source)
    assert issubclass(B, Transform)
    assert issubclass(C, Mixin)
    assert isinstance(A(), Source)
    assert isinstance(B(), Transform)

    with pytest.raises(TypeError):
        class D(Source, Transform):
            pass
    with pytest.raises(TypeError):
        class E(Source, Mixin):
            pass
    with pytest.raises(TypeError):
        class F(Transform, Mixin):
            pass
    with pytest.raises(TypeError):
        class G(C):
            pass

    assert str(B()) == 'B()'


class A(Transform):
    def x(x):
        return x

    def _t(x):
        return x ** 2

    def y(x, _t):
        return x + _t


class B(Transform):
    @meta
    def ids():
        return '123'

    def x(x):
        return x

    def _t(x):
        return x ** 2

    def y(x, _t):
        return x + _t


def test_pickleable():
    a = A()
    b = B()
    assert a.x != A.x

    for f in a.x, a.y, a._compile(dir(a)), b._compile(dir(b)):
        pickled = pickle.dumps(f)
        g = pickle.loads(pickled)
        assert f(0) == g(0)
