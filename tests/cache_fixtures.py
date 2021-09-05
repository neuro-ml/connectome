import sys

import pytest

from pickler_test_helpers import functions

REFERENCES = {
    (3, 9): {
        (0, functions.identity): b'\x80\x05\x95\xe8\x00\x00\x00\x00\x00\x00\x00\x8c\x17cloudpickle.cloudpickle\x94\x8c\x0e_fill_function\x93(h\x00\x8c\x0f_make_skel_func\x93h\x00\x8c\r_builtin_type\x93\x8c\x08CodeType\x85R(K\x01K\x00K\x00K\x01K\x01KCC\x04|\x00S\x00N\x85)\x8c\x01x\x85\x8c\x08identity\x94))tRJ\xff\xff\xff\xff\x86R(\x8c\x17_cloudpickle_submodules]\x86\x8c\x0eclosure_valuesN\x86\x8c\x08defaultsN\x86\x8c\x04dict)\x86\x8c\x07globals)\x86\x8c\x04nameh\x01\x86ttR.'
    }
}


@pytest.fixture
def pickle_references():
    return REFERENCES.get(sys.version_info[:2], {})
