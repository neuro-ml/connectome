from connectome import Source, meta, impure, Transform
from connectome.interface.blocks import HashDigest, CacheToRam, Merge
from connectome.interface.prepared import ComputableHash


def prepare(i, _length):
    return i[:_length]


def process(value):
    return f'received {value}'


class Computable(Source):
    _length: int

    field = ComputableHash(prepare, process)


class Stripped(Source):
    field = process


def test_hash():
    one = Computable(length=4) >> HashDigest(['field'])
    two = Stripped() >> HashDigest(['field'])
    assert one.field('12345678')[0] == 'received 1234'
    assert one.field('12345678') == one.field('1234----') == two.field('1234')


def test_impure():
    def count():
        nonlocal counter
        counter += 1
        return counter

    counter = 0

    class A(Source):
        @meta
        def ids():
            return tuple('0123')

        @impure
        def _prefix(i):
            return count()

        def text(i, _prefix):
            return f'{_prefix}_{i}'

    class B(Transform):
        @impure
        def _suffix(text):
            return count()

        def text(text, _suffix):
            return f'{text}{_suffix}'

    source = A()
    cached = source >> CacheToRam()
    assert source.text('0') != source.text('0')
    assert cached.text('0') != cached.text('0')
    assert counter == 4

    change = B()
    changed = source >> change
    cached = changed >> CacheToRam()
    assert change.text('some') != change.text('some')
    assert changed.text('0') != changed.text('0')
    assert cached.text('0') != cached.text('0')
    assert counter == 14


def test_missing_impure():
    def count():
        nonlocal counter
        counter += 1
        return counter

    counter = 0

    class A(Source):
        @meta
        def ids():
            return tuple('0123')

        def _prefix():
            return count()

        def text(i, _prefix):
            return f'{_prefix}_{i}'

    source = A()
    cached = source >> CacheToRam()
    assert source.text('0') != source.text('0')
    assert cached.text('0') == cached.text('0')


def test_hash_graph():
    def reject_object(x):
        if not isinstance(x, str):
            raise ValueError(x)
        return x

    class A(Source):
        def ids():
            return '1',

        f = ComputableHash(reject_object, lambda x: x)

    a = A()
    assert a.f('1') == '1'
    ds = Merge(a)
    assert ds.f('1') == '1'
