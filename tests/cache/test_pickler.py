from hashlib import sha256

import pytest
from pickler_test_helpers import functions
from pickler_test_helpers import functions2
from pickler_test_helpers import classes
from pickler_test_helpers import classes2

from connectome.cache.pickler import dumps, AVAILABLE_VERSIONS


def assert_same_hash(reference, version, obj, references):
    key = version, obj
    if key in references:
        assert sha256(reference).hexdigest() == references[key]


@pytest.mark.parametrize('version', AVAILABLE_VERSIONS)
def test_equal_functions(pickle_references, version):
    reference = dumps(functions.identity, version=version)
    assert_same_hash(reference, version, functions.identity, pickle_references)

    assert dumps(functions2.identity, version=version) == reference

    # FIXME: can't use functions.identity because the flag NESTED is different
    reference = dumps(functions.nested_identity, version=version)
    assert_same_hash(reference, version, functions.nested_identity, pickle_references)

    def identity(x: int) -> int:
        return x

    assert dumps(identity, version=version) == reference

    def identity(x: float):
        return x

    assert dumps(identity, version=version) == reference

    def identity(x):
        """Doc"""
        return x

    assert dumps(identity, version=version) == reference

    def identity(x):
        """Doc2"""
        return x

    assert dumps(identity, version=version) == reference

    def identity(x):
        return x

    identity.__doc__ = 'Doc3'

    assert dumps(identity, version=version) == reference

    class A:
        @staticmethod
        def identity(x):
            return x

    assert dumps(A.identity, version=version) == reference
    # TODO: do we need to enforce this?
    # assert dumps(lambda x: 1 + 1) == dumps(lambda x: 2)


@pytest.mark.parametrize('version', AVAILABLE_VERSIONS)
def test_different_functions(version):
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
    assert dumps(real_f, version=version) != dumps(A.f, version=version)

    # defaults should affect pickling
    assert dumps(lambda x: x, version=version) != dumps(lambda x=1: x, version=version)


@pytest.mark.parametrize('version', AVAILABLE_VERSIONS)
def test_class(version, pickle_references):
    reference = dumps(classes.One, version=version)
    assert_same_hash(reference, version, classes.One, pickle_references)
    assert reference == dumps(classes2.One, version=version)

    reference = dumps(classes.A, version=version)
    assert_same_hash(reference, version, classes.A, pickle_references)
    assert reference != dumps(classes.B, version=version)

    classes.A.y = 2
    assert reference == dumps(classes.A, version=version)
    old = classes.A.x
    classes.A.x = 2
    assert reference != dumps(classes.A, version=version)
    classes.A.x = old
