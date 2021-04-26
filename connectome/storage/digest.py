from hashlib import blake2b
from pathlib import Path

FOLDER_LEVELS = 1, 31, 32
DIGEST_SIZE = sum(FOLDER_LEVELS)


def digest_file(path: Path, block_size=2 ** 20) -> str:
    hasher = blake2b(digest_size=DIGEST_SIZE)

    with open(path, 'rb') as f:
        while True:
            buffer = f.read(block_size)
            if not buffer:
                break
            hasher.update(buffer)

    return hasher.hexdigest()


def digest_to_relative(key: str, suffix: str = None):
    assert len(key) == DIGEST_SIZE * 2, len(key)

    parts = []
    start = 0
    for level in FOLDER_LEVELS:
        stop = start + level * 2
        parts.append(key[start:stop])
        start = stop

    path = Path(*parts)
    if suffix is not None:
        path /= suffix
    return path


def digest_bytes(pickled: bytes) -> str:
    return blake2b(pickled, digest_size=DIGEST_SIZE).hexdigest()
