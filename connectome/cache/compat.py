"""
This set of functions was extracted from the `cloudpickle` package, for the sole purpose of
reproducibility across various versions of `cloudpickle`.

`cloudpickle` in turn is based on the `cloud` package, developed by `PiCloud, Inc.
<https://web.archive.org/web/20140626004012/http://www.picloud.com/>`_.

Copyright (c) 2012, Regents of the University of California.
Copyright (c) 2009 `PiCloud, Inc. <https://web.archive.org/web/20140626004012/http://www.picloud.com/>`_.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the University of California, Berkeley nor the
      names of its contributors may be used to endorse or promote
      products derived from this software without specific prior written
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
import importlib
import sys
from _weakrefset import WeakSet
from collections import ChainMap
from pickle import _getattribute, _Pickler as Pickler
from importlib._bootstrap import _find_spec

from cloudpickle import CloudPickler
from cloudpickle.cloudpickle import _whichmodule, _extract_code_globals, _get_cell_contents

try:
    from gzip import BadGzipFile
except ImportError:
    BadGzipFile = OSError


def _is_global(obj, name=None):
    if name is None:
        name = getattr(obj, '__qualname__', None)
    if name is None:
        name = getattr(obj, '__name__', None)

    module_name = _whichmodule(obj, name)

    if module_name is None:
        return False

    if module_name == "__main__":
        return False

    module = sys.modules.get(module_name, None)
    if module is None:
        return False

    if _is_dynamic(module):
        return False

    try:
        obj2, parent = _getattribute(module, name)
    except AttributeError:
        return False
    return obj2 is obj


def _is_dynamic(module):
    if hasattr(module, '__file__'):
        return False

    if module.__spec__ is not None:
        return False

    parent_name = module.__name__.rpartition('.')[0]
    if parent_name:
        try:
            parent = sys.modules[parent_name]
        except KeyError:
            msg = "parent {!r} not in sys.modules"
            raise ImportError(msg.format(parent_name))
        else:
            pkgpath = parent.__path__
    else:
        pkgpath = None
    return _find_spec(module.__name__, pkgpath, module) is None


def _is_under_development(obj, name):
    # the user opted out this function/class
    if obj in NO_PICKLE_SET:
        return False

    if name is None:
        name = getattr(obj, '__qualname__', None)
    if name is None:
        name = getattr(obj, '__name__', None)

    base_module = _whichmodule(obj, name).split('.', 1)[0]
    base = sys.modules.get(base_module)
    if base is None:
        base = importlib.import_module(base_module)

    return getattr(base, '__development__', False)


def _is_truly_global(obj, name):
    return _is_global(obj, name=name) and not _is_under_development(obj, name)


def extract_func_data(func):
    """A simplified version of the same function from cloudpickle"""
    code = func.__code__
    func_global_refs = _extract_code_globals(code)
    f_globals = {}
    for var in func_global_refs:
        if var in func.__globals__:
            f_globals[var] = func.__globals__[var]

    defaults = func.__defaults__

    closure = (
        list(map(_get_cell_contents, func.__closure__))
        if func.__closure__ is not None
        else None
    )
    # save the dict
    dct = func.__dict__
    # base globals - only needed for unpickling
    base_globals = None
    return code, f_globals, defaults, closure, dct, base_globals


DISPATCH, DISPATCH_TABLE = Pickler.dispatch.copy(), {}
# various versions of cloudpickle store the reducers/dispatch table in different places
#  1. the simplest case:
if hasattr(CloudPickler, 'dispatch_table') and isinstance(CloudPickler.dispatch_table, (dict, ChainMap)):
    DISPATCH_TABLE = CloudPickler.dispatch_table.copy()
# 2. the `dispatch` dict can actually be a `dispatch_table`:
elif 'reduce' in CloudPickler.dispatch[classmethod].__name__:
    DISPATCH_TABLE = CloudPickler.dispatch.copy()
# 3. the `dispatch` is really a dispatch
else:
    DISPATCH.update(CloudPickler.dispatch)


def to_dispatch(func):
    return lambda self, obj: self.save_reduce(*func(obj), obj=obj)


DISPATCH.update({k: to_dispatch(v) for k, v in DISPATCH_TABLE.items()})
# we use a set of weak refs, because we don't want to cause memory leaks
NO_PICKLE_SET = WeakSet()
