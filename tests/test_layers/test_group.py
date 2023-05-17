from hashlib import sha256

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
    assert 'two' not in dir(chain)

    for i in chain.ids:
        n = int(i)
        assert chain.same(i) == {str(n * 2): str(n * 2), str(n * 2 + 1): str(n * 2 + 1)}
        assert chain.three(i) == {str(n * 2): str(n * 2 // 3), str(n * 2 + 1): str((n * 2 + 1) // 3)}

    chain = ds >> GroupBy('same')
    assert chain.ids == ds.ids
    assert 'same' not in dir(chain)

    for i in chain.ids:
        n = int(i)
        assert chain.two(i) == {i: str(n // 2)}
        assert chain.three(i) == {i: str(n // 3)}


def test_single_method():
    class A(Source):
        @meta
        def ids():
            return tuple('012')

        def f(i):
            return i

    with pytest.raises(RuntimeError):
        A() >> GroupBy('f')


def test_multi_group_by():
    ds = DS()
    # test single group by
    chain = ds >> GroupBy._multiple('two')
    assert 'two' not in dir(chain)

    def compute_new_id(group_ids):
        first_hash = b''.join(sha256(i.encode()).digest() for i in group_ids)
        second_hash = sha256(first_hash).hexdigest()
        return second_hash

    id_groups = ['01', '23', '45', '67', '89']
    assert chain.ids == tuple(sorted(map(compute_new_id, id_groups)))

    for group in id_groups:
        assert chain.same(compute_new_id(group)) == {idx: idx for idx in group}
        assert chain.three(compute_new_id(group)) == {idx: str(int(idx) // 3) for idx in group}

    # test double group by
    chain = ds >> GroupBy._multiple('two', 'three')
    id_groups = ['01', '2', '3', '45', '67', '8', '9']
    assert chain.ids == tuple(sorted(map(compute_new_id, id_groups)))

    for group in id_groups:
        assert chain.same(compute_new_id(group)) == {idx: idx for idx in group}
        assert chain.double(compute_new_id(group)) == {idx: str(2 * int(idx)) for idx in group}

    # test comparator
    chain = ds >> GroupBy._multiple('double', double=lambda x, y: len(x) == len(y))
    id_groups = ['01234', '56789']
    assert chain.ids == tuple(sorted(map(compute_new_id, id_groups)))

    for group in id_groups:
        assert chain.two(compute_new_id(group)) == {idx: str(int(idx) // 2) for idx in group}
        assert chain.three(compute_new_id(group)) == {idx: str(int(idx) // 3) for idx in group}

    # test multiple comparators
    def cmp_int(x, y):
        assert isinstance(x, int)
        assert isinstance(y, int)
        return x == y

    def cmp_str(x, y):
        assert isinstance(x, str)
        assert isinstance(y, str)
        return x == y

    chain = DS() >> GroupBy._multiple('two_int', 'three', two_int=cmp_int, three=cmp_str)

    id_groups = ['01', '2', '3', '45', '67', '8', '9']
    assert chain.ids == tuple(sorted(map(compute_new_id, id_groups)))

    for group in id_groups:
        assert chain.same(compute_new_id(group)) == {idx: idx for idx in group}
        assert chain.double(compute_new_id(group)) == {idx: str(2 * int(idx)) for idx in group}


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
