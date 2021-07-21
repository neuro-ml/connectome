import logging
from typing import Callable

from .compat import SafeMeta
from ..containers.transform import InheritType
from ..utils import MultiDict
from .factory import SourceFactory, TransformFactory, FactoryLayer, add_from_mixins, add_quals

logger = logging.getLogger(__name__)


def _check_duplicates(namespace):
    duplicates = {name for name, values in namespace.groups() if len(values) > 1}
    if duplicates:
        raise TypeError(f'Duplicated methods found: {duplicates}')


class APIMeta(SafeMeta):
    @classmethod
    def __prepare__(mcs, *args, **kwargs):
        return MultiDict()

    def __new__(mcs, class_name, bases, namespace, **flags):
        if flags.pop('__root', False):
            assert bases == (FactoryLayer,)
            scope = namespace.to_dict()
            return super().__new__(mcs, class_name, bases, scope, **flags)

        bases = set(bases)
        intersection = {Transform, Source, Mixin} & bases
        if len(intersection) != 1:
            raise TypeError('Layers must inherit from either Source, Transform or Mixin.')

        main, = intersection
        bases -= intersection
        base_name = main.__name__
        for base in bases:
            if not issubclass(base, Mixin):
                raise TypeError(f'{base_name}s can only inherit directly from "{base_name}" or other mixins.')

        logger.info('Compiling the layer "%s" of type %s', class_name, base_name)

        if main == Mixin:
            add_from_mixins(namespace, bases)
            scope = add_quals({'__methods__': namespace}, namespace)
        elif main == Transform:
            add_from_mixins(namespace, bases)
            scope = TransformFactory.make_scope(namespace)
        elif main == Source:
            _check_duplicates(namespace)
            add_from_mixins(namespace, bases)
            scope = SourceFactory.make_scope(namespace)
        else:
            assert False, main

        return super().__new__(mcs, class_name, (main,), scope, **flags)


class Source(FactoryLayer, metaclass=APIMeta, __root=True):
    """
    Base class for all sources.
    """

    def __init__(self, *args, **kwargs):
        raise RuntimeError("\"Source\" can't be directly initialized. You must subclass it first.")


class Transform(FactoryLayer, metaclass=APIMeta, __root=True):
    """
    Base class for all transforms.

    Can also be used as an inplace factory for transforms.

    Examples
    --------
    # class-based transforms
    >>> class Zoom(Transform):
    >>>     def image(image):
    >>>         return zoom(image, scale_factor=2)
    # inplace transforms
    >>> Transform(image=lambda image: zoom(image, scale_factor=2))
    """
    __inherit__: InheritType = ()

    def __init__(*args, __inherit__=(), **kwargs: Callable):
        assert args
        if len(args) > 1:
            raise TypeError('This constructor accepts only keyword arguments.')
        self, = args

        local = MultiDict()
        local['__inherit__'] = __inherit__
        for name, value in kwargs.items():
            if not callable(value):
                raise TypeError(f'All arguments for Transform must be callable. "{name}" is not callable')

            local[name] = value

        factory = TransformFactory(local)
        super(Transform, self).__init__(factory.build({}), factory.property_names)

    def __repr__(self):
        return 'Transform(' + ', '.join(self._methods.methods) + ')'


class Mixin(FactoryLayer, metaclass=APIMeta, __root=True):
    """
    Base class for all Mixins.
    """

    def __init__(*args, **kwargs):
        raise RuntimeError("Mixins can't be directly initialized.")

    __methods__: dict = {}
