import pytest

from connectome.layers import PipelineLayer


@pytest.fixture
def first_simple(builder):
    return PipelineLayer(builder.build_layer(
        sum=lambda x, y: x + y,
        sub=lambda x, y: x - y,
        squared=lambda x: x ** 2,
        cube=lambda x: x ** 3,
        x=lambda x: x,
        y=lambda y: y,
    ))


@pytest.fixture
def second_simple(builder):
    return builder.build_layer(
        prod=lambda squared, cube: squared * cube,
        min=lambda squared, cube: min(squared, cube),
        x=lambda x: x,
        y=lambda y: y,
        sub=lambda sub: sub,
    )


@pytest.fixture
def third_simple(builder):
    return builder.build_layer(
        div=lambda prod, x: prod / x,
        original=lambda sub, y: sub + y,
    )


@pytest.fixture
def first_backward(builder):
    return builder.build_layer(
        prod=lambda x, _spacing: x * _spacing,
        inverse_prod=lambda prod, _spacing: prod / _spacing,
        _spacing=lambda: 2
    )


@pytest.fixture
def second_backward(builder):
    return builder.build_layer(
        prod=lambda prod: str(prod + 1),
        inverse_prod=lambda prod: int(prod) - 1,
    )
