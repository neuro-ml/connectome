from collections import Counter

from connectome.interface.base import Source, Chain, Transform
from connectome.interface.blocks import Merge


def test_single_with_params():
    class ParameterizedObj(Source):
        _some_constant = 1
        _ids_arg = 4

        @staticmethod
        def ids(_ids_arg):
            return _ids_arg

        @staticmethod
        def output_method(i, _some_constant, _second_param):
            return f'<output>_{i}_{_some_constant}_{_second_param}'

        @staticmethod
        def _second_param(i, _first_param, _some_constant):
            return f'<second>_{i}_{_some_constant}_{_first_param}'

        @staticmethod
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

    func = optional._layer.loopback(lambda x: x, 'first_optional', 'first_optional')['first_optional']
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
