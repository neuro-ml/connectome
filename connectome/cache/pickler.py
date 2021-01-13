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
from weakref import WeakSet
from collections import OrderedDict
from contextlib import suppress
from enum import Enum
from io import BytesIO

from cloudpickle.cloudpickle import CloudPickler, is_tornado_coroutine, _rebuild_tornado_coroutine, _fill_function, \
    _find_imported_submodules, _make_skel_func, _is_global, PYPY, builtin_code_type, Pickler, _whichmodule, \
    _BUILTIN_TYPE_NAMES, _builtin_type, _extract_class_dict, string_types


# TODO: replace by tuple
def sort_dict(d: dict):
    return OrderedDict([(k, d[k]) for k in sorted(d)])


# we use a set of weak refs, because we don't want to cause memory leaks
NO_PICKLE_SET = WeakSet()


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
AVAILABLE_VERSIONS = 0, 1, 2, 3
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
        Same reducer as in cloudpickle, except `co_filename`, `co_firstlineno` are not saved.
        """
        consts = obj.co_consts
        lnotab = obj.co_lnotab,
        if self.version >= 1:
            # remove the line number table
            lnotab = ()
            # remove the docstring
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
            obj.co_name,  # obj.co_firstlineno,
            *lnotab, obj.co_freevars, obj.co_cellvars
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

        code, f_globals, defaults, closure_values, dct, base_globals = self.extract_func_data(func)
        f_globals, dct, base_globals = map(sort_dict, [f_globals, dct, base_globals])

        base_globals = base_globals.copy()
        assert set(base_globals).issubset({'__package__', '__name__', '__path__', '__file__'})
        if '__file__' in base_globals:
            base_globals.pop('__file__')
        # as of py3.8 the docstring is always stored in co_consts[0]
        # need this assertion to detect any changes in further versions
        if func.__doc__ is not None:
            assert code.co_consts
            assert func.__doc__ == code.co_consts[0]

        save(_fill_function)
        write(pickle.MARK)

        submodules = _find_imported_submodules(
            code,
            itertools.chain(f_globals.values(), closure_values or ()),
        )

        save(_make_skel_func)
        if self.version >= 3:
            # base globals are only needed for unpickling
            save((
                code,
                len(closure_values) if closure_values is not None else -1,
            ))
        else:
            save((
                code,
                len(closure_values) if closure_values is not None else -1,
                base_globals,
            ))
        write(pickle.REDUCE)
        self.memoize(func)

        state = {
            'globals': f_globals,
            'defaults': defaults,
            'dict': dct,
            'closure_values': closure_values,
            'module': func.__module__,
            'name': func.__name__,
            '_cloudpickle_submodules': submodules
        }
        if hasattr(func, '__qualname__'):
            # TODO: drop __qualname__ ?
            state['qualname'] = func.__qualname__
        if getattr(func, '__kwdefaults__', False):
            state['kwdefaults'] = func.__kwdefaults__

        if self.version >= 3:
            del state['module']
            state = sort_dict(state)

        save(tuple(state.items()))
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

        write(pickle.MARK)

        # reproducibility
        # TODO: drop __module__ ?
        clsdict.pop('__doc__', None)
        clsdict = sort_dict(clsdict)
        type_kwargs = sort_dict(type_kwargs)

        save(types.ClassType)
        if issubclass(obj, Enum):
            members = sort_dict(dict((e.name, e.value) for e in obj))
            qualname = getattr(obj, "__qualname__", None)
            save((obj.__bases__, obj.__name__, qualname, members, obj.__module__))

            for attrname in ["_generate_next_value_", "_member_names_", "_member_map_", "_member_type_",
                             "_value2member_map_"] + list(members):
                clsdict.pop(attrname, None)
        else:
            save((type(obj), obj.__name__, obj.__bases__, type_kwargs))

        write(pickle.REDUCE)
        save(clsdict)
        write(pickle.TUPLE)
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
        if version >= 2:
            result = pickletools.optimize(result)
        return result
