from connectome.interface.base import Source, Chain
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


def test_single(block_builder):
    pipeline = block_builder.first_ds(first_constant=2, ids_arg=15)
    cc = block_builder.crop()
    assert pipeline.image(id='123123') == 'image, 2: 123123'
    assert cc.image(image='input') == f'input transformed 5'


def test_chain(block_builder):
    pipeline = Chain(
        block_builder.first_ds(first_constant=2, ids_arg=15),
        block_builder.crop(),
    )
    assert pipeline.image(id='123123') == f'image, 2: 123123 transformed 16'


def test_merge(block_builder):
    first_ds = block_builder.first_ds(first_constant=1, ids_arg=15)
    second_ds = block_builder.second_ds(second_constant=2, ids_arg=15)

    merged = Merge(first_ds, second_ds)
    assert merged.image(8) == f'image, 1: 8'
    assert merged.image('8') == f'second_ds_2_8'

    pipeline = Chain(
        merged,
        block_builder.crop(),
    )

    assert pipeline.image(8) == f'image, 1: 8 transformed 11'
    assert pipeline.image('8') == f'second_ds_2_8 transformed 13'


def test_backward(block_builder):
    pipeline = Chain(
        block_builder.first_ds(first_constant=2, ids_arg=15),
        block_builder.zoom(spacing=123),
        block_builder.crop()
    )

    identity = pipeline[1:].wrap_predict(lambda x: x, ['image'], 'image')
    double = pipeline[1:].wrap_predict(lambda x: 2 * x, ['image'], 'image')

    assert identity(100500) == 100500
    assert double(100500) == 100623100500


def test_optional(block_builder):
    pipeline = Chain(
        block_builder.first_ds(first_constant=2, ids_arg=15),
        block_builder.zoom(spacing=123),
        block_builder.optional(),
        block_builder.identity(),
        block_builder.crop(),
        block_builder.optional(),
        block_builder.identity(),
    )

    identity = pipeline[1:].wrap_predict(lambda x: x, ['image'], 'image')
    double = pipeline[1:].wrap_predict(lambda x: 2 * x, ['image'], 'image')

    assert identity(100500) == 100500
    assert double(100500) == 100623100500

    optional = block_builder.optional()
    assert optional.first_optional(10) == 11
    assert optional.second_optional(10) == '10'

    layer = optional._layer
    assert layer.get_backward_method('first_optional')(layer.get_forward_method('first_optional')(100500)) == 100500
