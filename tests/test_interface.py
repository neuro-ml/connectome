from connectome.interface import Source, Transform, Chain


class SomeDS(Source):
    _some_constant = 1

    @staticmethod
    def image(i, _some_constant):
        return f'image, {_some_constant}: {i}'

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


class ParameterizedObj(Source):
    _some_constant = 1

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
    pipeline = SomeDS(_some_constant=2)
    cc = Crop()
    assert pipeline.image(id='123123') == 'image, 2: 123123'
    assert cc.image(image='input') == f'input transformed 5'


def test_params():
    pipeline = ParameterizedObj(_some_constant=2)
    assert pipeline.output_method(id='666') == '<output>_666_2_<second>_666_2_<first>_666_2'


def test_chain():
    pipeline = Chain(
        SomeDS(some_constant=2),
        Crop(),
    )
    assert pipeline.image(id='123123') == f'image, 2: 123123 transformed 16'
