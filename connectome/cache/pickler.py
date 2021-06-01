"""
This module contains a relaxed but reproducible version of cloudpickle:
    1. any object, after being pickled, must produce the same output every time
    2. two different objects cannot have equal results after pickling
    3. we don't need to restore the pickled object, so we can drop any information
        as long as it helps to achieve (or doesn't impede) 1 and 2
"""
import importlib
import itertools
import pickle
import pickletools
import struct
import sys
import types
from operator import itemgetter
from typing import NamedTuple, Any
from weakref import WeakSet
from contextlib import suppress
from enum import Enum
from io import BytesIO

from cloudpickle.cloudpickle import CloudPickler, is_tornado_coroutine, _rebuild_tornado_coroutine, _fill_function, \
    _find_imported_submodules, _make_skel_func, _is_global, PYPY, builtin_code_type, Pickler, _whichmodule, \
    _BUILTIN_TYPE_NAMES, _builtin_type, _extract_class_dict, string_types


def sort_dict(d):
    return tuple(sorted(d.items()))


# we use a set of weak refs, because we don't want to cause memory leaks
NO_PICKLE_SET = WeakSet()
VERSION_METHOD = '__getversion__'


class VersionedClass(NamedTuple):
    type: type
    version: Any


def no_pickle(obj):
    """
    Decorator that opts out a function or class from being pickled during node hash calculation.
    Use it if you are sure that your function/class will never change in a way that might affect its behaviour.
    """
    NO_PICKLE_SET.add(obj)
    return obj


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


class PickleError(TypeError):
    pass


# new invalidation bugs will inevitable arise
# versioning will help diminish the pain from transitioning between updates
AVAILABLE_VERSIONS = 0,
*PREVIOUS_VERSIONS, LATEST_VERSION = AVAILABLE_VERSIONS


class PortablePickler(CloudPickler):
    dispatch = CloudPickler.dispatch.copy()

    def __init__(self, file, protocol=None, version=LATEST_VERSION):
        super().__init__(file, protocol=protocol)
        self.version = version

    def save(self, obj, *args, **kwargs):
        try:
            return super().save(obj, *args, **kwargs)
        except PickleError as e:
            raise PickleError(str(e)) from None
        except BaseException as e:
            raise PickleError(f'Exception "{e.__class__.__name__}: {e}" '
                              f'while pickling object {obj}') from None

    def save_codeobject(self, obj):
        """
        Same reducer as in cloudpickle, except `co_filename`, `co_firstlineno`, `lnotab` are not saved.
        """
        consts = obj.co_consts
        # remove the docstring:
        #  as of py3.9 the docstring is always stored in co_consts[0]
        #  associated issue: https://bugs.python.org/issue36521
        if consts and isinstance(consts[0], str):
            consts = list(consts)[1:]
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
        self.save_reduce(types.CodeType, args, obj=obj)

    dispatch[types.CodeType] = save_codeobject

    def save_function(self, obj, name=None):
        """ Patched version that knows about __development__ mode """
        if _is_truly_global(obj, name):
            return Pickler.save_global(self, obj, name=name)
        elif PYPY and isinstance(obj.__code__, builtin_code_type):
            return self.save_pypy_builtin_func(obj)
        else:
            return self.save_function_tuple(obj)

    dispatch[types.FunctionType] = save_function

    def save_function_tuple(self, func):
        """ Reproducible function tuple """
        if is_tornado_coroutine(func):
            self.save_reduce(_rebuild_tornado_coroutine, (func.__wrapped__,), obj=func)
            return

        save = self.save
        write = self.write

        # base globals are only needed for unpickling
        code, f_globals, defaults, closure_values, dct, _ = self.extract_func_data(func)
        f_globals, dct = map(sort_dict, [f_globals, dct])

        save(_fill_function)
        write(pickle.MARK)

        submodules = _find_imported_submodules(
            code,
            # same as f_globals.values()
            itertools.chain(map(itemgetter(1), f_globals), closure_values or ()),
        )

        save(_make_skel_func)
        save((
            code,
            len(closure_values) if closure_values is not None else -1,
        ))
        write(pickle.REDUCE)
        self.memoize(func)

        state = {
            'globals': f_globals,
            'defaults': defaults,
            'dict': dct,
            'closure_values': closure_values,
            'name': func.__name__,
            '_cloudpickle_submodules': submodules
        }
        # __qualname__ is only used fo debug
        if getattr(func, '__kwdefaults__', False):
            state['kwdefaults'] = func.__kwdefaults__

        state = sort_dict(state)

        save(state)
        write(pickle.TUPLE)
        write(pickle.REDUCE)

    def _save_dynamic_enum(self, obj, clsdict):
        raise NotImplementedError

    def save_dynamic_class(self, obj):
        clsdict = _extract_class_dict(obj)
        clsdict.pop('__weakref__', None)

        if "_abc_impl" in clsdict:
            import abc
            (registry, _, _, _) = abc._get_dump(obj)
            clsdict["_abc_impl"] = [subclass_weakref() for subclass_weakref in registry]

        # originally here was the __doc__
        type_kwargs = {}
        if hasattr(obj, "__slots__"):
            type_kwargs['__slots__'] = obj.__slots__
            if isinstance(obj.__slots__, string_types):
                clsdict.pop(obj.__slots__)
            else:
                for k in obj.__slots__:
                    clsdict.pop(k, None)

        __dict__ = clsdict.pop('__dict__', None)
        if isinstance(__dict__, property):
            type_kwargs['__dict__'] = __dict__

        save = self.save
        write = self.write

        # reproducibility
        clsdict.pop('__doc__', None)
        clsdict.pop('__module__', None)
        type_kwargs = sort_dict(type_kwargs)

        save(types.ClassType)
        if issubclass(obj, Enum):
            members = tuple(sorted([(e.name, e.value) for e in obj]))
            # __qualname__ is only used for debug
            save((obj.__bases__, obj.__name__, members, obj.__module__))

            for attrname in ["_generate_next_value_", "_member_names_", "_member_map_", "_member_type_",
                             "_value2member_map_"] + list(map(itemgetter(0), members)):
                clsdict.pop(attrname, None)
        else:
            save((type(obj), obj.__name__, obj.__bases__, type_kwargs))

        save(sort_dict(clsdict))
        write(pickle.TUPLE2)
        write(pickle.REDUCE)

    def save_global(self, obj, name=None, pack=struct.pack):
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

        elif not _is_truly_global(obj, name=name):
            self.save_dynamic_class(obj)
        else:
            Pickler.save_global(self, obj, name=name)

    dispatch[type] = save_global
    dispatch[types.ClassType] = save_global

    with suppress(ImportError):
        from _functools import _lru_cache_wrapper

        # caching should not affect pickling
        def save_lru_cache(self, obj):
            # lru_cache uses functools.wrap
            self.save(obj.__wrapped__)

        dispatch[_lru_cache_wrapper] = save_lru_cache


def dumps(obj, protocol: int = None, version: int = LATEST_VERSION) -> bytes:
    with BytesIO() as file:
        PortablePickler(file, protocol=protocol, version=version).dump(obj)
        result = file.getvalue()
        result = pickletools.optimize(result)
        return result
