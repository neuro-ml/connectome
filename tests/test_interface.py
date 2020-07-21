import re
from connectome.interface.base import Source, Transform, Chain, Merge
from connectome.interface.decorators import inverse


class SomeDS(Source):
    _first_constant = 1
    _ids_arg = 4

    @staticmethod
    def ids(_ids_arg):
        return list(range(_ids_arg))

    @staticmethod
    def image(i, _first_constant):
        return f'image, {_first_constant}: {i}'

    @staticmethod
    def lungs(i):
        return f'lungs: {i}'

    @staticmethod
    def spacing(i):
        return f'spacing: {i}'


class SomeDS2(Source):
    _ids_arg = 4
    _second_constant = 3

    @staticmethod
    def ids(_ids_arg):
        return [str(i) for i in range(_ids_arg)]

    @staticmethod
    def image(i, _second_constant):
        return f'second_ds_{_second_constant}_' + i

    @staticmethod
    def lungs(i):
        return f'lungs: {i}'

    @staticmethod
    def spacing(i):
        return f'spacing: {i}'


class Crop(Transform):
    @staticmethod
    def _size(image):
        return len(image)

    @staticmethod
    def image(x, _size):
        return x + f' transformed {_size}'

    spacing = lungs = image

    @staticmethod
    @inverse
    def image(x, _size):
        return re.sub(f' transformed {_size}', '', x)


class Zoom(Transform):
    _spacing = None

    @staticmethod
    def image(x, _spacing):
        return str(x + _spacing)

    spacing = lungs = image

    @staticmethod
    @inverse
    def image(x, _spacing):
        return int(x) - _spacing


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


def test_single():
    pipeline = SomeDS(first_constant=2, ids_arg=15)
    cc = Crop()
    assert pipeline.image(id='123123') == 'image, 2: 123123'
    assert cc.image(image='input') == f'input transformed 5'


def test_single_with_params():
    pipeline = ParameterizedObj(some_constant=2, ids_arg=15)
    assert pipeline.output_method(id='666') == '<output>_666_2_<second>_666_2_<first>_666_2'


def test_chain():
    pipeline = Chain(
        SomeDS(first_constant=2, ids_arg=15),
        Crop(),
    )
    assert pipeline.image(id='123123') == f'image, 2: 123123 transformed 16'


def test_merge():
    first_ds = SomeDS(first_constant=1, ids_arg=15)
    second_ds = SomeDS2(second_constant=2, ids_arg=15)

    merged = Merge(first_ds, second_ds)
    assert merged.image(8) == f'image, 1: 8'
    assert merged.image('8') == f'second_ds_2_8'

    pipeline = Chain(
        merged,
        Crop(),
    )

    assert pipeline.image(id=8) == f'image, 1: 8 transformed 11'
    assert pipeline.image(id='8') == f'second_ds_2_8 transformed 13'


def test_backward():
    pipeline = Chain(
        SomeDS(first_constant=2, ids_arg=15),
        Zoom(spacing=123),
        Crop()
    )

    identity = pipeline[1:].wrap_predict(lambda x: x, ['image'], 'image')
    double = pipeline[1:].wrap_predict(lambda x: 2 * x, ['image'], 'image')

    assert identity(100500) == 100500
    assert double(100500) == 100623100500
