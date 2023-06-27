import pytest

from connectome import GroupBy, Source, impure, meta
from connectome.engine.base import HashError


class DS(Source):
    @meta
    def ids():
        return tuple('0123456789')

    def two_int(i):
        return int(i) // 2

    def same(i):
        return i

    def two(i):
        return str(int(i) // 2)

    def three(i):
        return str(int(i) // 3)

    def double(i):
        return str(int(i) * 2)


def test_group_by():
    ds = DS()
    chain = ds >> GroupBy('two')
    assert chain.ids == tuple('01234')

    for i in chain.ids:
        n = int(i)
        assert chain.same(i) == {str(n * 2): str(n * 2), str(n * 2 + 1): str(n * 2 + 1)}
        assert chain.three(i) == {str(n * 2): str(n * 2 // 3), str(n * 2 + 1): str((n * 2 + 1) // 3)}

    chain = ds >> GroupBy('same')
    assert chain.ids == ds.ids

    for i in chain.ids:
        n = int(i)
        assert chain.two(i) == {i: str(n // 2)}
        assert chain.three(i) == {i: str(n // 3)}


def test_missing_id():
    class A(Source):
        @meta
        def ids():
            return tuple('012')

        def f(i):
            return i

    with pytest.raises(KeyError):
        (A() >> GroupBy('f')).f('-1')


def test_single_method():
    class A(Source):
        @meta
        def ids():
            return tuple('012')

        def f(i):
            return i

    ds = A() >> GroupBy('f')
    assert ds.f('0') == {'0': '0'}


@pytest.mark.parametrize('layer,id_groups', (
        (GroupBy(lambda two, three: two + three), {
            '01': '00', '2': '10', '3': '11', '45': '21', '67': '32', '8': '42', '9': '43'}),
        (GroupBy(lambda double: str(len(double))), {'01234': '1', '56789': '2'}),
))
def test_multi_group_by(layer, id_groups):
    ds = DS()
    chain = ds >> layer
    assert set(chain.ids) == set(id_groups.values())

    for k, v in id_groups.items():
        assert chain.same(v) == {idx: idx for idx in k}
        assert chain.double(v) == {idx: str(2 * int(idx)) for idx in k}


def test_impure():
    class A(Source):
        @meta
        def ids():
            return tuple('0123456789')

        @impure
        def _g():
            pass

        def f(i, _g):
            return i

        def g(i):
            return i

    with pytest.raises(HashError):
        A() >> GroupBy('f')
