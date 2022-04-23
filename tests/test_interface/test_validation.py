import pytest
from connectome import Source, Filter


def test_default_args():
    with pytest.raises(ValueError):
        class A(Source):
            def f(x, y=1):
                pass

    def f(x, y=1):
        pass

    with pytest.raises(ValueError):
        Filter(f)
