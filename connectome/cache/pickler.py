"""
This module contains a relaxed but reproducible version of cloudpickle:
    1. any object, after being pickled, must produce the same output every time
    2. two different objects cannot have equal results after pickling
    3. we don't need to restore the pickled object, so we can drop any information
        as long as it helps to achieve (or doesn't impede) 1 and 2
"""
import abc
import itertools
import pickle
import pickletools
import types
from operator import itemgetter
from typing import NamedTuple, Any
from contextlib import suppress
from enum import Enum
from io import BytesIO

from cloudpickle.cloudpickle import is_tornado_coroutine, PYPY, builtin_code_type, \
    _rebuild_tornado_coroutine, _find_imported_submodules, _BUILTIN_TYPE_NAMES, _builtin_type, _extract_class_dict

from .compat import DISPATCH, extract_func_data, Pickler, get_pickle_mode, PickleMode


def sort_dict(d):
    return tuple(sorted(d.items()))


VERSION_METHOD = '__getversion__'


class VersionedClass(NamedTuple):
    type: type
    version: Any


class PickleError(TypeError):
    pass


# new invalidation bugs will inevitably arise
# versioning will help diminish the pain from transitioning between updates
AVAILABLE_VERSIONS = 0,
*PREVIOUS_VERSIONS, LATEST_VERSION = AVAILABLE_VERSIONS

_custom = types.CodeType, types.FunctionType, type, property
for _key in _custom:
    DISPATCH.pop(_key, None)


class PortablePickler(Pickler):
    dispatch = DISPATCH

    def __init__(self, file, protocol=None, version=None):
        if version is None:
            version = LATEST_VERSION

        super().__init__(file, protocol=protocol)
        self.version = version
        self.dispatch_table = {
            types.CodeType: self.reduce_code,
            property: self.reduce_property,
        }

    def save(self, obj, *args, **kwargs):
        try:
            return super().save(obj, *args, **kwargs)
        except PickleError as e:
            raise PickleError(str(e)) from None
        except BaseException as e:
            raise PickleError(f'Exception "{e.__class__.__name__}: {e}" '
                              f'while pickling object {obj}') from None

    # copied from cloudpickle==2.0.0
    # used to save functions and classes
    def _save_reduce(self, func, args, state=None, listitems=None, dictitems=None, state_setter=None, obj=None):
        self.save_reduce(func, args, state=None, listitems=listitems, dictitems=dictitems, obj=obj)
        self.save(state_setter)
        self.save(obj)
        self.save(state)
        self.write(pickle.TUPLE2)
        self.write(pickle.REDUCE)
        self.write(pickle.POP)

    @staticmethod
    def _is_global(obj, name):
        return get_pickle_mode(obj, name) == PickleMode.Global

    @staticmethod
    def reduce_property(obj: property):
        return property, (obj.fget, obj.fset, obj.fdel, obj.__doc__)

    @staticmethod
    def reduce_code(obj: types.CodeType):
        """
        Same reducer as in cloudpickle, except `co_filename`, `co_firstlineno`, `lnotab` are not saved.
        """
        consts = obj.co_consts
        # remove the docstring:
        #  as of py3.9 the docstring is always stored in co_consts[0]
        #  associated issue: https://bugs.python.org/issue36521
        if consts and isinstance(consts[0], str):
            consts = list(consts)[1:]
            # TODO: this is not enough, None might be referenced in the bytecode under a wrong index
            if None in consts:
                consts.remove(None)
            consts = (None, *consts)

        if hasattr(obj, "co_posonlyargcount"):
            posonlyargcount = obj.co_posonlyargcount,
        else:
            posonlyargcount = ()

        args = (
            obj.co_argcount, *posonlyargcount,
            obj.co_kwonlyargcount, obj.co_nlocals, obj.co_stacksize,
            obj.co_flags, obj.co_code, consts, obj.co_names,
            obj.co_varnames,  # obj.co_filename,
            obj.co_name,  # obj.co_firstlineno, obj.co_lnotab,
            obj.co_freevars, obj.co_cellvars
        )
        return types.CodeType, args

    # function reducers will be used after we migrate to newer version
    def reduce_function(self, obj):
        """ Patched version that knows about __development__ mode """
        if self._is_global(obj, None):
            return NotImplemented
        elif PYPY and isinstance(obj.__code__, builtin_code_type):
            raise NotImplementedError
        else:
            return self.reduce_dynamic_function(obj)

    @staticmethod
    def reduce_dynamic_function(func):
        if is_tornado_coroutine(func):
            return _rebuild_tornado_coroutine, (func.__wrapped__,)

        # stuff we're dropping:
        #  1. base globals - only needed for unpickling
        #  2. __qualname__, __module__ - only used for debug
        #  3. __annotations__, __doc__ - not used at runtime

        code, f_globals, defaults, closure_values, dct = extract_func_data(func)
        f_globals, dct = map(sort_dict, [f_globals, dct])

        # args
        args = code, (len(closure_values) if closure_values is not None else -1)
        # state
        # TODO: do we need this?
        submodules = _find_imported_submodules(
            code,
            # same as f_globals.values()
            itertools.chain(map(itemgetter(1), f_globals), closure_values or ()),
        )
        name = func.__name__
        kw_defaults = getattr(func, '__kwdefaults__', None)
        if kw_defaults is not None:
            kw_defaults = sort_dict(kw_defaults)
        slot_state = f_globals, defaults, dct, closure_values, name, submodules, kw_defaults
        return types.FunctionType, args, (slot_state, func.__dict__)

    # for now we'll use the old `save_function`
    def save_function(self, obj, name=None):
        """ Patched version that knows about __development__ mode """
        if self._is_global(obj, name):
            return Pickler.save_global(self, obj, name=name)
        elif PYPY and isinstance(obj.__code__, builtin_code_type):
            return self.save_pypy_builtin_func(obj)
        else:
            return self._save_reduce(*self.reduce_dynamic_function(obj), obj=obj)

    dispatch[types.FunctionType] = save_function

    @staticmethod
    def _get_cls_params(obj):
        clsdict = _extract_class_dict(obj)
        clsdict.pop('__weakref__', None)

        if "_abc_impl" in clsdict:
            (registry, _, _, _) = abc._get_dump(obj)
            clsdict["_abc_impl"] = [subclass_weakref() for subclass_weakref in registry]

        # originally here was the __doc__
        type_kwargs = {}
        if hasattr(obj, "__slots__"):
            type_kwargs['__slots__'] = obj.__slots__
            if isinstance(obj.__slots__, str):
                clsdict.pop(obj.__slots__)
            else:
                for k in obj.__slots__:
                    clsdict.pop(k, None)

        __dict__ = clsdict.pop('__dict__', None)
        if isinstance(__dict__, property):
            type_kwargs['__dict__'] = __dict__

        # reproducibility
        clsdict.pop('__doc__', None)
        clsdict.pop('__module__', None)
        return clsdict, sort_dict(type_kwargs)

    def reduce_dynamic_class(self, obj):
        clsdict, type_kwargs = self._get_cls_params(obj)

        args = type(obj), obj.__name__, obj.__bases__, type_kwargs
        state = sort_dict(clsdict)
        return type, args, state

    def reduce_dynamic_enum(self, obj):
        clsdict, type_kwargs = self._get_cls_params(obj)

        members = {e.name: e.value for e in obj}
        for attrname in ["_generate_next_value_", "_member_names_", "_member_map_", "_member_type_",
                         "_value2member_map_"] + list(members):
            clsdict.pop(attrname, None)

        args = obj.__bases__, obj.__name__, sort_dict(members), obj.__module__
        state = sort_dict(clsdict)
        return type, args, state

    def save_dynamic_class(self, obj):
        method = self.reduce_dynamic_enum if isinstance(obj, Enum) else self.reduce_dynamic_class
        return self._save_reduce(*method(obj), obj=obj)

    def save_global(self, obj, name=None):
        """ Save a "global" which is not under __development__ """
        if obj is type(None):
            return self.save_reduce(type, (None,), obj=obj)
        elif obj is type(Ellipsis):
            return self.save_reduce(type, (Ellipsis,), obj=obj)
        elif obj is type(NotImplemented):
            return self.save_reduce(type, (NotImplemented,), obj=obj)
        elif obj in _BUILTIN_TYPE_NAMES:
            return self.save_reduce(_builtin_type, (_BUILTIN_TYPE_NAMES[obj],), obj=obj)
        elif name is not None:
            Pickler.save_global(self, obj, name=name)

        elif hasattr(obj, VERSION_METHOD):
            self.save(VersionedClass)
            version = getattr(obj, VERSION_METHOD)()
            Pickler.save_global(self, obj)
            self.save(version)
            self.write(pickle.TUPLE2)
            self.write(pickle.REDUCE)

        elif not self._is_global(obj, name=name):
            self.save_dynamic_class(obj)
        else:
            Pickler.save_global(self, obj, name=name)

    dispatch[type] = save_global

    with suppress(ImportError):
        from _functools import _lru_cache_wrapper

        # caching should not affect pickling
        def save_lru_cache(self, obj):
            # lru_cache uses functools.wrap
            self.save(obj.__wrapped__)

        dispatch[_lru_cache_wrapper] = save_lru_cache


def dumps(obj, protocol: int = None, version: int = None) -> bytes:
    with BytesIO() as file:
        PortablePickler(file, protocol=protocol, version=version).dump(obj)
        result = file.getvalue()
        result = pickletools.optimize(result)
        return result
