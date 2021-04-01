import pytest
from connectome import Source, Transform, Chain, CacheToRam, insert


class DS(Source):
    def image(i):
        return i


# here `image` is a required input that has no output, but must be inherited
class Some(Transform):
    __inherit__ = True

    @insert
    def shape(image):
        return image.shape

    @insert
    def some_false():
        return False


def test_chain():
    ds = Chain(
        DS(),
        Some(),
    )
    assert ds.image('id') == 'id'


def test_nested(block_maker, hash_layer):
    one = block_maker.first_ds(first_constant=2, ids_arg=15)
    two = block_maker.crop()

    base, *variants = [
        Chain(one, two, hash_layer),
        Chain(Chain(one, two), hash_layer),
        Chain(one, Chain(two, hash_layer)),
        Chain(Chain(one), Chain(two), hash_layer),
        Chain(Chain(one, two, hash_layer)),
    ]

    for i in one.ids:
        value = base.image(i)
        for variant in variants:
            assert variant.image(i) == value


def test_cache_removal(block_maker):
    one = block_maker.first_ds(first_constant=2, ids_arg=15)
    two = block_maker.crop()

    simple = one >> two
    cached = Chain(one, two, CacheToRam())._drop_cache()
    nested = (simple >> CacheToRam())._drop_cache()

    for i in one.ids:
        assert simple.image(i) == cached.image(i) == nested.image(i)


def test_inheritance():
    class FirstInheritAll(Transform):
        __inherit__ = True

        def f():
            return 'A.f'

    class FirstInheritPart(Transform):
        __inherit__ = 'g'

        def f():
            return 'A.f'

    class SecondInheritPart(Transform):
        __inherit__ = 'g'

        def h(f, g):
            return f, g

    class SecondInheritAll(Transform):
        __inherit__ = True

        def h(f, g):
            return f, g

    class ThirdInheritAll(Transform):
        __inherit__ = True

        def p(h, g, f):
            return h, g, f

    class ThirdInheritPart(Transform):
        __inherit__ = 'h'

        def p(h, g, f):
            return h, g, f

    ds = FirstInheritAll() >> SecondInheritPart()
    assert ds.g('hello') == 'hello'
    assert ds.h(g='input') == ('A.f', 'input')

    with pytest.raises(AttributeError):
        ds.f()

    ds = FirstInheritAll() >> SecondInheritAll()
    assert ds.h(g='input') == ('A.f', 'input')
    assert ds.g('hello') == 'hello'
    assert ds.f() == 'A.f'

    ds = FirstInheritPart() >> SecondInheritAll()
    assert ds.h(g='input') == ('A.f', 'input')
    assert ds.g('hello') == 'hello'
    assert ds.f() == 'A.f'

    ds = FirstInheritPart() >> SecondInheritPart()
    assert ds.g('hello') == 'hello'
    assert ds.h(g='input') == ('A.f', 'input')

    with pytest.raises(AttributeError):
        ds.f()

    ds = FirstInheritAll() >> SecondInheritAll() >> ThirdInheritAll()
    assert ds.p(g='hello') == (('A.f', 'hello'), 'hello', 'A.f')
    assert ds.g('hello') == 'hello'
    assert ds.h(g='input') == ('A.f', 'input')
    assert ds.f() == 'A.f'

    ds = FirstInheritAll() >> SecondInheritAll() >> ThirdInheritPart()
    assert ds.p(g='hello') == (('A.f', 'hello'), 'hello', 'A.f')
    assert ds.h(g='input') == ('A.f', 'input')

    with pytest.raises(AttributeError):
        ds.g('hello')

    with pytest.raises(AttributeError):
        ds.f()

    with pytest.raises(RuntimeError):
        FirstInheritAll() >> SecondInheritPart() >> ThirdInheritAll()
