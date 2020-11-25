from connectome import Chain, Filter


def test_filter(block_maker, hash_layer):
    block = block_maker.first_ds(first_constant=2, ids_arg=15)
    pipeline = Chain(
        block, Filter(lambda image: image.endswith('4')),
    )
    ids = pipeline.ids
    assert ids == ('4', '14')

    hashed = Chain(block, hash_layer)
    pipeline = Chain(
        block, Filter(lambda image: image.endswith('4')), hash_layer,
    )
    for i in ids:
        assert hashed.image(i) == pipeline.image(i)
        assert hashed.lungs(i) == pipeline.lungs(i)
        assert hashed.spacing(i) == pipeline.spacing(i)
