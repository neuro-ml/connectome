import sys

import pytest

from pickler_test_helpers import functions, classes

# using sha256 here is enough because we can always access the commit used to generate it
REFERENCES = {
    (3, 9): {
        (0, functions.identity): '5dc0d0cf39f9f4e1c5f1701fe8acec030f1434842fbc53c8bf2a56b16a1ee38d',
        (0, functions.nested_identity): '45bc2140d5c8e8f9672a49fb52f84390e90baa95f2a1e18943234db4c7c46155',
        (0, classes.One): '1cf94fed74ae4a5e456040e3b23905e05b7e117189c57b482165712f4f266030',
        (0, classes.A): '921691616291f6d2626f8da5c6c1734084542f8deee1c9ff903cb9c770b8b48a',
    }
}


@pytest.fixture
def pickle_references():
    return REFERENCES.get(sys.version_info[:2], {})
