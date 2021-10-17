from pathlib import Path

import pytest
from connectome import Source, Transform, Chain, CacheToRam, meta
from connectome.exceptions import DependencyError
from connectome.interface.base import LazyChain
from connectome.interface.blocks import HashDigest, CacheColumns, Merge
from connectome.storage.config import init_storage


class DS(Source):
    def image(i):
        return i


# here `image` is a required input that has no output, but must be inherited
class Some(Transform):
    __inherit__ = True

    def shape(image):
        return image.shape

    def some_false():
        return False


def test_chain():
    ds = Chain(
        DS(),
        Some(),
    )
    assert ds.image('id') == 'id'


def test_nested(block_maker):
    one = block_maker.first_ds(first_constant=2, ids_arg=15)
    two = block_maker.crop()
    hash_layer = HashDigest('image')

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

    assert type(cached[-1]) == type(two)
    assert type(nested[-1]) == type(simple)


def test_filter_removal(block_maker):
    one = block_maker.first_ds(first_constant=2, ids_arg=15)
    two = block_maker.crop()

    cropped = one >> two
    filtered = cropped._filterfalse(isinstance, Transform)

    assert len(filtered._layers) == 1
    assert type(filtered[0]) == type(one)


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

    with pytest.raises(DependencyError):
        FirstInheritAll() >> SecondInheritPart() >> ThirdInheritAll()


@pytest.mark.xfail
def test_all_inherit():
    class A(Transform):
        __inherit__ = 'x'

    class B(Transform):
        def x(x):
            return x

    assert set(dir(A() >> A() >> B())) == {'x'}


def test_lazy(tmpdir, storage_factory):
    class A(Source):
        @meta
        def ids():
            return '0',

        def f(x):
            return x

        def y(x):
            return x

    class B(Transform):
        __inherit__ = True

        def g(y):
            return y

    with storage_factory() as storage:
        root = Path(tmpdir) / 'cache'
        init_storage(root, algorithm={'name': 'blake2b', 'digest_size': 64}, levels=[1, 31, 32])
        cache = CacheColumns(root, storage, [], [])
        A() >> B() >> cache
        with pytest.raises(DependencyError):
            B() >> cache

        lc = LazyChain(B(), cache)
        ds = A() >> lc
        assert ds.ids == ('0',)
        assert ds.g(1) == 1


@pytest.mark.xfail
def test_missing_ids(block_maker):
    ds = block_maker.first_ds(first_constant=2, ids_arg=15)
    for source in [ds, Merge(ds)]:
        ids = source.ids
        for block in [block_maker.crop(), CacheToRam(), Transform(image=lambda image: image)]:
            assert (source >> block).ids == ids

    class A(Transform):
        def image(image):
            pass

    assert 'ids' in dir(ds >> A() >> A())


def test_dir_duplicates():
    class A(Transform):
        def image(x):
            return x

    class B(Transform):
        __inherit__ = True

        def image(image):
            return image

    items = dir(A() >> B())
    assert len(items) == len(set(items))
