import pytest

from connectome import Source, Transform, Mixin


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
