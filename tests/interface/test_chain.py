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
    class A(Transform):
        __inherit__ = True

        def f():
            return 'A.f'

    class B(Transform):
        __inherit__ = 'g'

        def h(f, g):
            return f, g

    class C(Transform):
        __inherit__ = True

        def h(f, g):
            return f, g

    ds = A() >> B()
    assert ds.f() == 'A.f'
    assert ds.h(g='input') == ('A.f', 'input')
    ds = A() >> C()
    assert ds.f() == 'A.f'
    assert ds.h(g='input') == ('A.f', 'input')
