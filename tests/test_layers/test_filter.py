import pytest

from connectome import Chain, Filter, Source, impure, meta, HashDigest
from connectome.engine import HashError
from connectome.exceptions import DependencyError


def test_filter(block_maker):
    block = block_maker.first_ds(first_constant=2, ids_arg=15)
    pipeline = Chain(
        block, Filter(lambda id: id in ['2', '5']),
    )
    assert pipeline.ids == ('2', '5')

    pipeline = Chain(
        block, Filter(lambda image: image.endswith('4')),
    )
    ids = pipeline.ids
    assert ids == ('4', '14')

    hash_layer = HashDigest(['image', 'lungs', 'spacing'], 'blake2b', return_value=True)
    hashed = Chain(block, hash_layer)
    pipeline = Chain(
        block, Filter(lambda image: image.endswith('4')), hash_layer,
    )
    for i in ids:
        assert hashed.image(i) == pipeline.image(i)
        assert hashed.lungs(i) == pipeline.lungs(i)
        assert hashed.spacing(i) == pipeline.spacing(i)


def test_impure():
    class A(Source):
        @meta
        def ids():
            return tuple('012')

        @impure
        def _g():
            pass

        def f(i, _g):
            return i

    with pytest.raises(HashError):
        A() >> Filter(lambda f: f)


def test_wrong_predicate():
    class A(Source):
        @meta
        def ids():
            return ()

        def x(i):
            pass

    with pytest.raises(DependencyError):
        A() >> Filter(lambda y: y)
    with pytest.raises(DependencyError):
        A() >> Filter(lambda x, y: y)
