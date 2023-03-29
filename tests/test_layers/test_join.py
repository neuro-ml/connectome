import pytest

from connectome import Source, meta
from connectome.layers import Join


@pytest.mark.parametrize('mode', (
        ('inner', '23', '23', '23'),
        ('outer', '012345', '0123', '2345'),
        ('left', '0123', '0123', '23'),
        ('right', '2345', '23', '2345'),
))
def test_one_key_join(mode):
    mode, ids, left, right = mode

    class A(Source):
        @meta
        def ids():
            return tuple('0123')

        def my_key(i):
            return f'{i}-key'

        def value_a(i):
            return f'{i}-a'

    class B(Source):
        @meta
        def ids():
            return tuple('2345')

        def my_key(i):
            return f'{i}-key'

        def value_b(i):
            return f'{i}-b'

    # inner
    a = A()
    b = B()
    c = Join(a, b, 'my_key', how=mode)
    assert set(dir(c)) == {'id', 'ids', 'my_key', 'value_a', 'value_b'}
    assert tuple(map(c.my_key, c.ids)) == c.ids
    assert tuple(map(c.id, c.ids)) == c.ids
    assert set(c.ids) == {f'{i}-key' for i in ids}
    assert list(map(c.value_a, c.ids)) == [f'{i}-a' if i in left else None for i in ids]
    assert list(map(c.value_b, c.ids)) == [f'{i}-b' if i in right else None for i in ids]


def test_nested_join():
    class A(Source):
        @meta
        def ids():
            return '012'

        key = lambda i: i

    class B(Source):
        @meta
        def ids():
            return '123'

        key = lambda i: i

    class C(Source):
        @meta
        def ids():
            return '234'

        key = lambda i: i

    a, b, c = A(), B(), C()
    assert Join(a, Join(b, c, 'key'), 'key').ids == ('2',)
    assert Join(Join(b, c, 'key'), a, 'key').ids == ('2',)
    assert Join(a, Join(b, c, 'key', how='outer'), 'key').ids == ('1', '2')
    assert Join(a, Join(b, c, 'key', how='outer'), 'key', how='left').ids == ('0', '1', '2')
