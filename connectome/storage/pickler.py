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
import sys
import types
from collections import OrderedDict
from contextlib import suppress
from io import BytesIO

from cloudpickle.cloudpickle import CloudPickler, is_tornado_coroutine, _rebuild_tornado_coroutine, _fill_function, \
    _find_imported_submodules, _make_skel_func, _is_global, PYPY, builtin_code_type, Pickler, _whichmodule


def sort_dict(d: dict):
    return OrderedDict([(k, d[k]) for k in sorted(d)])


NO_PICKLE_ATTRIBUTE = '__connectome_no_pickle__'


def no_pickle(func):
    """
    Decorator that opts out a function from being pickled during node hash calculation.
    Use it if you are sure that your function will never change in a way that might affect its behaviour.
    """
    # TODO: maybe keep a global set of all decorated functions
    #  instead of writing some attribute
    setattr(func, NO_PICKLE_ATTRIBUTE, True)
    return func


def _is_under_development(obj, name):
    # the user opted out this function
    if hasattr(obj, NO_PICKLE_ATTRIBUTE):
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


class PickleError(TypeError):
    pass


# new invalidation bugs will inevitable arise
# versioning will help diminish the pain from transitioning between updates
AVAILABLE_VERSIONS = 0, 1
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
        """ Registered with the dispatch to handle all function types.

        Determines what kind of function obj is (e.g. lambda, defined at
        interactive prompt, etc) and handles the pickling appropriately.
        """
        if _is_global(obj, name=name) and not _is_under_development(obj, name):
            return Pickler.save_global(self, obj, name=name)
        elif PYPY and isinstance(obj.__code__, builtin_code_type):
            return self.save_pypy_builtin_func(obj)
        else:
            return self.save_function_tuple(obj)

    dispatch[types.FunctionType] = save_function

    def save_function_tuple(self, func):
        """
        Reproducible function tuple
        """
        if is_tornado_coroutine(func):
            self.save_reduce(_rebuild_tornado_coroutine, (func.__wrapped__,), obj=func)
            return

        save = self.save
        write = self.write

        code, f_globals, defaults, closure_values, dct, base_globals = self.extract_func_data(func)
        f_globals, dct, base_globals = map(sort_dict, [f_globals, dct, base_globals])
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
            # 'doc': func.__doc__, - Don't need the doc
            '_cloudpickle_submodules': submodules
        }
        if hasattr(func, '__qualname__'):
            state['qualname'] = func.__qualname__
        # don't need annotations
        # if getattr(func, '__annotations__', False):
        #     state['annotations'] = sort_dict(func.__annotations__)
        if getattr(func, '__kwdefaults__', False):
            state['kwdefaults'] = func.__kwdefaults__

        save(tuple(state.items()))
        write(pickle.TUPLE)
        write(pickle.REDUCE)

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
        return file.getvalue()
