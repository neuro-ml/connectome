from connectome import Source, Merge, Transform, Chain, positional
from connectome.engine.base import NodeHash


class One(Source):
    @staticmethod
    def ids():
        return tuple('123')

    @staticmethod
    def image(i):
        return f'One: {i}'


class Two(Source):
    @staticmethod
    def ids():
        return tuple('456')

    @staticmethod
    def image(i):
        return f'Two: {i}'


def test_simple(hash_layer):
    one, two = One(), Two()
    merged = Merge(one, two)
    assert set(merged.ids) == set('123456')

    # values
    for i in one.ids:
        assert merged.image(i) == one.image(i)
    for i in two.ids:
        assert merged.image(i) == two.image(i)

    # hashes
    hashed = Chain(merged, hash_layer)
    one_hashed, two_hashed = Chain(one, hash_layer), Chain(two, hash_layer)

    for i in one.ids:
        value, node_hash = hashed.image(i)
        assert value == merged.image(i)
        assert isinstance(node_hash, NodeHash)

        assert hashed.image(i) == one_hashed.image(i)

    for i in two.ids:
        value, node_hash = hashed.image(i)
        assert value == merged.image(i)
        assert isinstance(node_hash, NodeHash)

        assert hashed.image(i) == two_hashed.image(i)


def test_nested():
    class Underscore(Transform):
        @staticmethod
        @positional
        def image(x):
            return f'_{x}'

    merged = Merge(One(), Chain(Two(), Underscore()))
    assert set(merged.ids) == set('123456')
    assert merged.image('1') == 'One: 1'
    assert merged.image('5') == '_Two: 5'

    merged = Merge(Chain(
        One(),
        Underscore(),
        Underscore(),
    ), Chain(
        Two(),
        Underscore(),
    ))
    assert set(merged.ids) == set('123456')
    assert merged.image('1') == '__One: 1'
    assert merged.image('5') == '_Two: 5'
