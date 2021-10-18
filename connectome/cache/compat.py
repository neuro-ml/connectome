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
import warnings
from weakref import WeakSet
from collections import ChainMap
from enum import Enum
from pickle import _Pickler as Pickler
from types import ModuleType
from typing import Union, Set

from cloudpickle import CloudPickler
from cloudpickle.cloudpickle import _whichmodule, _extract_code_globals, _get_cell_contents, _lookup_module_and_qualname

try:
    from gzip import BadGzipFile
except ImportError:
    BadGzipFile = OSError


class PickleMode(Enum):
    Global, Deep = range(2)


def get_pickle_mode(obj, name=None):
    if obj in STABLE_OBJECTS:
        return PickleMode.Global
    if obj in UNSTABLE_OBJECTS:
        return PickleMode.Deep

    pair = _lookup_module_and_qualname(obj, name)
    if pair is None:
        return PickleMode.Deep
    module, _ = pair
    module = module.__name__

    while True:
        if module in UNSTABLE_MODULES:
            return PickleMode.Deep

        split = module.rsplit(".", 1)
        if len(split) == 1:
            break
        module, _ = split

    _check_is_under_development(obj, name)
    return PickleMode.Global


def _check_is_under_development(obj, name):
    if name is None:
        name = getattr(obj, '__qualname__', None)
    if name is None:
        name = getattr(obj, '__name__', None)

    base_module = _whichmodule(obj, name).split('.', 1)[0]
    base = sys.modules.get(base_module)
    if base is None:
        base = importlib.import_module(base_module)

    if hasattr(base, '__development__'):
        raise RuntimeError(f'Error in "{base_module}": you are relying on an old cache invalidation machinery: '
                           'use `unstable_module(__name__)` instead')


def extract_func_data(func):
    """A simplified version of the same function from cloudpickle"""
    # `base globals` are not used - they are only needed for unpickling
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
    return code, f_globals, defaults, closure, dct


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
    def save(self, obj):
        args = func(obj)
        if isinstance(args, str):
            self.save_global(obj, args)
        else:
            self.save_reduce(*args, obj=obj)

    return save


DISPATCH.update({k: to_dispatch(v) for k, v in DISPATCH_TABLE.items()})
# we use a set of weak refs, because we don't want to cause memory leaks
STABLE_OBJECTS = WeakSet()
UNSTABLE_OBJECTS = WeakSet()
UNSTABLE_MODULES: Set[str] = set()


def is_stable(obj):
    """
    Decorator that opts out a function or class from being pickled during node hash calculation.
    Use it if you are sure that your function/class will never change in a way that might affect its behaviour.
    """
    if obj in UNSTABLE_OBJECTS:
        warnings.warn('The object was already marked as unstable')
        UNSTABLE_OBJECTS.remove(obj)
    STABLE_OBJECTS.add(obj)
    return obj


def is_unstable(obj):
    if obj in STABLE_OBJECTS:
        warnings.warn('The object was already marked as stable')
        STABLE_OBJECTS.remove(obj)
    UNSTABLE_OBJECTS.add(obj)
    return obj


def unstable_module(module: Union[str, ModuleType]):
    if not isinstance(module, str):
        module = module.__name__
    UNSTABLE_MODULES.add(module)
