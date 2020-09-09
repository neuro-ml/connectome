from connectome import Source, Merge, Transform, Chain


class One(Source):
    @staticmethod
    def ids():
        return tuple('123')

    @staticmethod
    def image(i):
        return f'One: {i}'


class Two(Source):
    @staticmethod
    def ids():
        return tuple('456')

    @staticmethod
    def image(i):
        return f'Two: {i}'


def test_simple():
    merged = Merge(One(), Two())
    assert set(merged.ids) == set('123456')
    assert merged.image('1') == 'One: 1'
    assert merged.image('5') == 'Two: 5'


def test_nested():
    class Underscore(Transform):
        @staticmethod
        def image(x):
            return f'_{x}'

    merged = Merge(One(), Chain(Two(), Underscore()))
    assert set(merged.ids) == set('123456')
    assert merged.image('1') == 'One: 1'
    assert merged.image('5') == '_Two: 5'

    merged = Merge(Chain(
        One(),
        Underscore(),
        Underscore(),
    ), Chain(
        Two(),
        Underscore(),
    ))
    assert set(merged.ids) == set('123456')
    assert merged.image('1') == '__One: 1'
    assert merged.image('5') == '_Two: 5'
