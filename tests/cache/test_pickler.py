from pickler_test_helpers import functions
from pickler_test_helpers import functions2
from pickler_test_helpers import classes
from pickler_test_helpers import classes2

from connectome.cache.pickler import dumps


def test_equal_functions():
    assert dumps(functions2.identity) == dumps(functions.identity)

    # FIXME: can't use functions.identity because the flag NESTED is different
    def identity(x):
        return x

    reference = dumps(identity)

    def identity(x: int) -> int:
        return x

    assert dumps(identity) == reference

    def identity(x: float):
        return x

    assert dumps(identity) == reference

    def identity(x):
        """Doc"""
        return x

    assert dumps(identity) == reference

    def identity(x):
        """Doc2"""
        return x

    assert dumps(identity) == reference

    def identity(x):
        return x

    identity.__doc__ = 'Doc3'

    assert dumps(identity) == reference

    class A:
        @staticmethod
        def identity(x):
            return x

    assert dumps(A.identity) == reference
    assert dumps(lambda x: 1 + 1) == dumps(lambda x: 2)


def test_different_functions():
    # an interesting case where 2 functions have same bodies, but behave differently
    def scope():
        def f(x):
            if x == 0:
                return [0]
            return [f(x - 1)]

        return f

    def f(x):
        return x

    class A:
        @staticmethod
        def f(x):
            if x == 0:
                return [0]
            return [f(x - 1)]

    real_f = scope()
    assert real_f(2) != A.f(2)
    assert dumps(real_f) != dumps(A.f)

    # defaults should affect pickling
    assert dumps(lambda x: x) != dumps(lambda x=1: x)


def test_class():
    reference = dumps(classes.One)
    assert reference == dumps(classes2.One)

    reference = dumps(classes.A)
    assert reference != dumps(classes.B)

    classes.A.y = 2
    assert reference == dumps(classes.A)
    classes.A.x = 2
    assert reference != dumps(classes.A)
