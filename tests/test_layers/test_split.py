import pytest

from connectome import Source, Split, meta


class A(Source):
    @meta
    def ids():
        return tuple('012')

    def x(i):
        return [f'x-{i}-{j}' for j in range(int(i) + 1)]

    def y(i):
        return [f'y-{i}-{j}' for j in range(int(i) + 1)]


def test_split():
    class SplitList(Split):
        def __split__(id, x):
            for idx, entry in enumerate(x):
                yield f'{id}-{idx}', idx

        def x(x, __part__):
            return x[__part__]

        def y(y, __part__):
            return y[__part__]

    a = A()
    ds = a >> SplitList()
    assert ds.ids == ('0-0', '1-0', '1-1', '2-0', '2-1', '2-2')
    assert [ds.x('0-0')] == a.x('0')
    assert [ds.x('1-0'), ds.x('1-1')] == a.x('1')


@pytest.mark.xfail
def test_split_with_args():
    class SplitListWithArgs(Split):
        _separator: str

        def __split__(id, x, _separator):
            for idx, entry in enumerate(x):
                yield f'{id}{_separator}{idx}', idx

        def x(x, __part__):
            return x[__part__]

        def y(y, __part__):
            return y[__part__]

    a = A()
    ds = a >> SplitListWithArgs('@')
    assert ds.ids == ('0@0', '1@0', '1@1', '2@0', '2@1', '2@2')
    assert [ds.x('0@0')] == a.x('0')
    assert [ds.x('1@0'), ds.x('1@1')] == a.x('1')
