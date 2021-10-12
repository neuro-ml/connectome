import sys

import pytest

from pickler_test_helpers import functions, classes

# using sha256 here is enough because we can always access the commit used to generate it
REFERENCES = {
    (3, 9): {
        (0, functions.identity): '9b8b1e3ae950963e609f8164ba76403c6790731db5e1cbbdb56a0cf9cd886005',
        (0, functions.nested_identity): '9e23d47084c40f37054da37a345e26fd85bbf5ad733d84f790acb03d8e16d115',
        (0, classes.One): '4acfbb8cc1dfb43db698ebedc9c6eb0b23d71b2dc021e2f684cfa321b8d5aa01',
        (0, classes.A): 'afef34c32cf2c8e94350c2bf02d57d8a1f7cdf5f0a81a180892276011f6594cd',
    }
}


@pytest.fixture
def pickle_references():
    return REFERENCES.get(sys.version_info[:2], {})
