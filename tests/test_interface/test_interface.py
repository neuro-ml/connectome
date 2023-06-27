import warnings
from collections import Counter
from typing import Callable

import pytest

from connectome import Chain, Merge, Output, Source, Transform, meta, positional
from connectome.interface.metaclasses import SourceBase, TransformBase


def test_single_with_params():
    class ParameterizedObj(Source):
        _some_constant = 1
        _ids_arg = 4

        @meta
        def ids(_ids_arg):
            return _ids_arg

        def output_method(i, _some_constant, _second_param):
            return f'<output>_{i}_{_some_constant}_{_second_param}'

        def _second_param(i, _first_param, _some_constant):
            return f'<second>_{i}_{_some_constant}_{_first_param}'

        def _first_param(i, _some_constant):
            return f'<first>_{i}_{_some_constant}'

    pipeline = ParameterizedObj(some_constant=2, ids_arg=15)
    assert pipeline.output_method(id='666') == '<output>_666_2_<second>_666_2_<first>_666_2'

    defaults = ParameterizedObj()
    explicit = ParameterizedObj(some_constant=1, ids_arg=4)
    assert explicit.output_method('666') == defaults.output_method('666')


def test_single(block_maker):
    pipeline = block_maker.first_ds(first_constant=2, ids_arg=15)
    cc = block_maker.crop()
    assert pipeline.image(id='123123') == 'image, 2: 123123'
    assert cc.image(image='input') == f'input transformed 5'


def test_chain(block_maker):
    pipeline = Chain(
        block_maker.first_ds(first_constant=2, ids_arg=15),
        block_maker.crop(),
    )
    assert pipeline.image(id='123123') == f'image, 2: 123123 transformed 16'

    compiled = pipeline._compile(['image', 'lungs', 'spacing'])
    for i in pipeline.ids:
        assert compiled(i) == (pipeline.image(i), pipeline.lungs(i), pipeline.spacing(i))


def test_inplace_transform(block_maker):
    @positional
    def image(x, _size):
        return x + f' transformed {_size}'

    base = Chain(
        block_maker.first_ds(first_constant=2, ids_arg=15),
        block_maker.crop(),
    )
    inplace = Chain(
        block_maker.first_ds(first_constant=2, ids_arg=15),
        Transform(
            _size=lambda image: len(image),
            spacing=image, lungs=image, image=image,
        ),
    )

    for i in base.ids:
        assert base.image(i) == inplace.image(i)
        assert base.lungs(i) == inplace.lungs(i)
        assert base.spacing(i) == inplace.spacing(i)


def test_merge(block_maker):
    first_ds = block_maker.first_ds(first_constant=1, ids_arg=15)
    second_ds = block_maker.second_ds(second_constant=2, ids_arg=15)

    merged = Merge(first_ds, second_ds)
    assert merged.image('8') == f'image, 1: 8'
    assert merged.image('second:8') == f'second_ds_2_second:8'

    pipeline = Chain(
        merged,
        block_maker.crop(),
    )

    assert pipeline.image('8') == f'image, 1: 8 transformed 11'
    assert pipeline.image('second:8') == f'second_ds_2_second:8 transformed 20'


def test_backward(block_maker):
    pipeline = Chain(
        block_maker.first_ds(first_constant=2, ids_arg=15),
        block_maker.zoom(spacing='123'),
        block_maker.crop()
    )

    processing = pipeline[1:]
    dec = processing._decorate('image')
    identity = dec(lambda x: x)
    under = dec(lambda x: '_' + x)

    assert identity('100500') == '100500'
    assert under('100500') == '_100500'

    @processing._decorate('image', 'lungs')
    def pred(image):
        return 'some_lungs ' + image

    assert pred('some_img') == 'some_lungs some_img'


def test_optional(block_maker):
    pipeline = Chain(
        block_maker.first_ds(first_constant=2, ids_arg=15),
        block_maker.zoom(spacing='123'),
        block_maker.optional(),
        block_maker.identity(),
        block_maker.optional(),
        block_maker.optional(),
        block_maker.crop(),
        block_maker.optional(),
        block_maker.optional(),
        block_maker.identity(),
    )

    identity = pipeline[1:]._wrap(lambda x: x, 'image')
    double = pipeline[1:]._wrap(lambda x: '_' + x, 'image')

    assert identity('100500') == '100500'
    assert double('100500') == '_100500'

    optional = block_maker.optional()
    assert optional.first_optional(10) == 11
    assert optional.second_optional(10) == '10'

    func = optional._container.loopback(
        lambda x: x, 'first_optional', 'first_optional').compile().compile('first_optional')
    assert func(100500) == 100500


def test_persistent(block_maker):
    pipeline = Chain(
        block_maker.first_ds(first_constant=2, ids_arg=4)
    )
    assert Counter(pipeline.ids) == Counter('0123')

    pipeline = Chain(
        block_maker.first_ds(first_constant=2, ids_arg=4),
        block_maker.zoom(spacing=123),
        block_maker.optional(),
        block_maker.optional(),
        block_maker.zoom(spacing=123),
    )
    assert Counter(pipeline.ids) == Counter('0123')

    class Partial(Transform):
        __inherit__ = 'image'

    pipeline = Chain(
        block_maker.first_ds(first_constant=2, ids_arg=4),
        block_maker.zoom(spacing=123),
        block_maker.optional(),
        block_maker.optional(),
        block_maker.zoom(spacing=123),
        Partial(),
    )
    assert Counter(pipeline.ids) == Counter('0123')


def test_instance(block_maker):
    pipeline = Chain(
        block_maker.first_ds(first_constant=2, ids_arg=15),
        block_maker.crop(),
    )

    for i in pipeline.ids:
        instance = pipeline(i)
        assert instance.id == instance['id'] == pipeline.id(i) == i
        assert instance.image == instance['image'] == pipeline.image(i)
        assert instance.lungs == instance['lungs'] == pipeline.lungs(i)

        loader = pipeline._compile(['image', 'lungs'])
        assert loader(i) == instance['image', 'lungs']
        loader = pipeline._compile(['id', 'lungs'])
        assert loader(i) == instance['id', 'lungs']


def test_input_spec(block_maker):
    class NoOutput(Transform):
        def _shape(image):
            return len(image)

        def shape(_shape):
            return _shape

        def image(image, _shape):
            return f'{image} shape: {_shape}'

    class WithOutput(Transform):
        def shape(image):
            return len(image)

        def image(image, shape: Output):
            return f'{image} shape: {shape}'

    ds = block_maker.first_ds(first_constant=2, ids_arg=15)
    one = ds >> NoOutput()
    two = ds >> WithOutput()

    for i in ds.ids:
        assert one.image(i) == two.image(i)
        assert one.shape(i) == two.shape(i)


def test_constructor():
    class A(Transform):
        _a: int
        _b: int = 1

    class B(Transform):
        _a: int

    with pytest.raises(TypeError):
        A(a=1, c=2)
    with pytest.raises(TypeError):
        A()
    with pytest.raises(TypeError):
        A(1)

    B(1)


def test_callable_argument():
    class A(Transform):
        _a: Callable = lambda: []

        def x(_a):
            return _a()

    assert A().x() == []
    assert A(a=int).x() == 0

    with warnings.catch_warnings():
        warnings.filterwarnings('error')
        with pytest.raises(UserWarning, match='Are you trying to pass a default value for an argument?'):
            class B(Transform):
                _a = lambda: []

        with pytest.raises(UserWarning, match='Did you forget to remove the type annotation?'):
            class C(Transform):
                _a: int

                def _a(x):
                    pass


def test_plain_inheritance():
    class A(TransformBase):
        def __init__(self):
            super().__init__({'x': lambda x: x + 1})

    class B(SourceBase):
        def __init__(self):
            super().__init__({'x': lambda i: len(i), 'ids': meta(lambda: ['0'])})

    a = A()
    b = B()
    assert a.x(1) == 2
    assert b.ids == ['0']
    assert b.x('0') == 1


def test_wrong_arg_type():
    with pytest.raises(TypeError, match='The parameter "a" must be "positional or keyword"'):
        class A(Transform):
            def f(*, a):
                pass
    with pytest.raises(TypeError, match='The parameter "a" must be "positional or keyword"'):
        class A(Transform):
            def f(*a):
                pass
    with pytest.raises(TypeError, match='The parameter "a" must be "positional or keyword"'):
        class A(Transform):
            def f(**a):
                pass
