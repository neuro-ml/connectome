from connectome.utils import AntiSet


def test_anti_set():
    x = AntiSet('abcd')
    y = AntiSet('01ab')
    z = set('cdef')

    assert x & y == y & x == AntiSet('01abcd')
    assert x - y == set('01')
    assert y - x == set('cd')
    assert x | y == y | x == AntiSet('ab')

    assert x & z == z & x == set('ef')
    assert x - z == AntiSet('abcdef')
    assert z - x == set('cd')
    assert x | z == z | x == AntiSet('ab')
