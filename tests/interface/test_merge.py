from connectome import Source, Merge, Transform, Chain, positional, meta, impure
from connectome.engine.base import NodeHash


class One(Source):
    @meta
    def ids():
        return tuple('123')

    def image(i):
        return f'One: {i}'


class Two(Source):
    @meta
    def ids():
        return tuple('456')

    def image(i):
        return f'Two: {i}'


class Three(Source):
    @meta
    def ids():
        return tuple('789')

    def image(i):
        return f'Three: {i}'


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


def test_chained():
    class Underscore(Transform):
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


def test_nested():
    a = One()
    b = Two()
    c = Three()
    ds = Merge(a, Merge(b, c))
    assert ds.ids == a.ids + b.ids + c.ids

    for x in [a, b, c]:
        for i in x.ids:
            assert ds.image(i) == x.image(i)


def test_impure():
    def count():
        nonlocal i
        i += 1

    i = 0

    class A(Source):
        _start: int

        @meta
        def ids(_start):
            return tuple(map(str, range(_start, _start + 3)))

        @impure
        def f(i):
            count()
            return i

    ds = Merge(
        A(start=0),
        A(start=10),
    )

    assert ds.f('0') == '0'
    assert i == 1
    assert ds.f('10') == '10'
    assert i == 2
