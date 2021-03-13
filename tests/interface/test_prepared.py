from connectome import Source
from connectome.interface.blocks import HashDigest
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
