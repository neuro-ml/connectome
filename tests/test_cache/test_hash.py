from pathlib import Path
from tempfile import TemporaryDirectory

from tarn.config import init_storage, StorageConfig

from connectome import Chain, CacheToRam, CacheToDisk, CacheColumns
from connectome.interface.blocks import HashDigest
from connectome.serializers import JsonSerializer


def test_hash(block_maker, storage_factory):
    hash_layer = HashDigest(['image'])
    pipeline = Chain(
        block_maker.first_ds(first_constant=2, ids_arg=15),
        block_maker.crop(),
    )

    hashed = Chain(pipeline, hash_layer)
    ram = Chain(pipeline, CacheToRam(['image']), hash_layer)
    with TemporaryDirectory() as root, storage_factory() as storage:
        root = Path(root) / 'cache'
        init_storage(StorageConfig(hash='blake2b', levels=[1, 63]), root)

        disk = Chain(pipeline, CacheToDisk(root, storage, names=['image'], serializer=JsonSerializer()), hash_layer)
        rows = Chain(pipeline, CacheColumns(root, storage, names=['image'], serializer=JsonSerializer()), hash_layer)

        rows.image(pipeline.ids[0])
        for i in pipeline.ids:
            assert hashed.image(i)[0] == pipeline.image(i)
            assert hashed.image(i) == ram.image(i) == disk.image(i) == rows.image(i)
