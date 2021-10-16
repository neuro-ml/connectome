from functools import partial
from hashlib import sha256

import numpy as np
import pytest
from pickler_test_helpers import functions
from pickler_test_helpers import functions2
from pickler_test_helpers import classes
from pickler_test_helpers import classes2

from connectome.cache import is_stable, is_unstable
from connectome.cache.pickler import dumps, AVAILABLE_VERSIONS


def assert_same_hash(reference, version, obj, references):
    key = version, obj
    if key in references:
        values = references[key]
        if isinstance(values, str):
            values = values,
        assert sha256(reference).hexdigest() in values


@pytest.mark.parametrize('version', AVAILABLE_VERSIONS)
def test_equal_functions(pickle_references, version):
    dumper = partial(dumps, version=version)

    reference = dumper(functions.identity)
    assert_same_hash(reference, version, functions.identity, pickle_references)

    assert dumper(functions2.identity) == reference

    # FIXME: can't use functions.identity because the flag NESTED is different
    reference = dumper(functions.nested_identity)
    assert_same_hash(reference, version, functions.nested_identity, pickle_references)

    def identity(x: int) -> int:
        return x

    assert dumper(identity) == reference

    def identity(x: float):
        return x

    assert dumper(identity) == reference

    def identity(x):
        """Doc"""
        return x

    assert dumper(identity) == reference

    def identity(x):
        """Doc2"""
        return x

    assert dumper(identity) == reference

    def identity(x):
        return x

    identity.__doc__ = 'Doc3'

    assert dumper(identity) == reference

    class A:
        @staticmethod
        def identity(x):
            return x

    assert dumper(A.identity) == reference
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
    dumper = partial(dumps, version=version)

    reference = dumper(classes.One)
    assert_same_hash(reference, version, classes.One, pickle_references)
    assert reference == dumper(classes2.One)

    reference = dumper(classes.A)
    assert_same_hash(reference, version, classes.A, pickle_references)
    assert reference != dumper(classes.B)

    classes.A.y = 2
    assert reference == dumper(classes.A)
    old = classes.A.x
    classes.A.x = 2
    assert reference != dumper(classes.A)
    classes.A.x = old


@pytest.mark.parametrize('version', AVAILABLE_VERSIONS)
def test_stable(version, pickle_references):
    dumper = partial(dumps, version=version)

    not_stable = dumper(functions.identity)
    is_stable(functions.identity)
    stable = dumper(functions.identity)
    assert stable != not_stable

    is_unstable(functions.identity)
    assert dumper(functions.identity) == not_stable
    is_stable(functions.identity)
    assert dumper(functions.identity) == stable


@pytest.mark.parametrize('version', AVAILABLE_VERSIONS)
def test_special_cases(version):
    dumper = partial(dumps, version=version)
    dumper(np.dtype)
