"""
This module contains a relaxed but portable version of cloudpickle:
    1. any object, after being pickled, must produce the same output every time
    2. two different objects cannot have equal results after pickling
    3. we don't need to restore the pickled object, so we can drop any information
        as long as it helps to achieve (or doesn't impede) 1 and 2
"""
import types
from collections import OrderedDict
from io import BytesIO

from cloudpickle.cloudpickle import CloudPickler


def sort_dict(d: dict):
    return OrderedDict([(k, d[k]) for k in sorted(d)])


class PortablePickler(CloudPickler):
    dispatch = CloudPickler.dispatch.copy()

    def save_codeobject(self, obj):
        """
        Same reducer as in cloudpickle, except `co_filename`, `co_firstlineno` are not saved.
        """
        if hasattr(obj, "co_posonlyargcount"):
            args = (
                obj.co_argcount, obj.co_posonlyargcount,
                obj.co_kwonlyargcount, obj.co_nlocals, obj.co_stacksize,
                obj.co_flags, obj.co_code, obj.co_consts, obj.co_names,
                obj.co_varnames,  # obj.co_filename,
                obj.co_name,  # obj.co_firstlineno,
                obj.co_lnotab, obj.co_freevars, obj.co_cellvars
            )
        else:
            args = (
                obj.co_argcount, obj.co_kwonlyargcount, obj.co_nlocals,
                obj.co_stacksize, obj.co_flags, obj.co_code, obj.co_consts,
                obj.co_names, obj.co_varnames,  # obj.co_filename,
                obj.co_name,  # obj.co_firstlineno,
                obj.co_lnotab, obj.co_freevars, obj.co_cellvars
            )

        self.save_reduce(types.CodeType, args, obj=obj)

    dispatch[types.CodeType] = save_codeobject

    def extract_func_data(self, func):
        code, f_globals, defaults, closure_values, dct, base_globals = super().extract_func_data(func)
        # order these dicts
        f_globals, dct, base_globals = map(sort_dict, [f_globals, dct, base_globals])
        return code, f_globals, defaults, closure_values, dct, base_globals


def dumps(obj, protocol=None):
    with BytesIO() as file:
        PortablePickler(file, protocol=protocol).dump(obj)
        return file.getvalue()
