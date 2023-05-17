import pytest

from connectome import Filter, Source


def test_default_args():
    with pytest.raises(ValueError):
        class A(Source):
            def f(x, y=1):
                pass

    def f(x, y=1):
        pass

    with pytest.raises(ValueError):
        Filter(f)
