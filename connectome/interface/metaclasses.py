import logging
from typing import Callable, Dict, Type, Union, Iterable

from .compat import SafeMeta
from ..utils import MultiDict
from .factory import SourceFactory, TransformFactory, FactoryLayer, add_from_mixins, add_quals, GraphFactory, \
    is_detectable

logger = logging.getLogger(__name__)

BASES: Dict[Type[FactoryLayer], GraphFactory] = {}


class APIMeta(SafeMeta):
    @classmethod
    def __prepare__(mcs, *args, **kwargs):
        return MultiDict()

    def __new__(mcs, class_name, bases, namespace, **flags):
        if '__factory' in flags:
            factory = flags.pop('__factory')
            assert bases == (FactoryLayer,)
            scope = namespace.to_dict()
            base = super().__new__(mcs, class_name, bases, scope, **flags)
            BASES[base] = factory
            return base

        bases = set(bases)
        intersection = set(BASES) & bases
        if len(intersection) != 1:
            raise TypeError(f'Layers must inherit from on of ' + ', '.join(x.__name__ for x in BASES))

        main, = intersection
        factory = BASES[main]
        bases -= intersection
        base_name = main.__name__
        for base in bases:
            if not issubclass(base, Mixin):
                raise TypeError(f'{base_name}s can only inherit directly from "{base_name}" or other mixins.')

        logger.info('Compiling the layer "%s" of type %s', class_name, base_name)

        if main == Mixin:
            add_from_mixins(namespace, bases)
            scope = add_quals({'__methods__': namespace}, namespace)

        else:
            factory.validate_before_mixins(namespace)
            add_from_mixins(namespace, bases)
            scope = factory.make_scope(namespace)

        return super().__new__(mcs, class_name, (main,), scope, **flags)


class Source(FactoryLayer, metaclass=APIMeta, __factory=SourceFactory):
    """
    Base class for all sources.
    """

    def __init__(self, *args, **kwargs):
        raise RuntimeError("\"Source\" can't be directly initialized. You must subclass it first.")


class Transform(FactoryLayer, metaclass=APIMeta, __factory=TransformFactory):
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
    __inherit__: Union[str, Iterable[str], bool] = ()

    def __init__(*args, __inherit__=(), **kwargs: Callable):
        assert args
        if len(args) > 1:
            raise TypeError('This constructor accepts only keyword arguments.')
        self, = args

        local = MultiDict()
        local['__inherit__'] = __inherit__
        for name, value in kwargs.items():
            if not is_detectable(value):
                raise TypeError(
                    f'All arguments for Transform must be callable. "{name}" is not callable but {type(value)}'
                )

            local[name] = value

        factory = TransformFactory(local)
        if factory.special_methods:
            raise TypeError(f"This constructor doesn't accept special methods: {tuple(factory.special_methods)}")
        super(Transform, self).__init__(factory.build({}), factory.property_names, ())

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join(self._methods.methods)})"


class Mixin(FactoryLayer, metaclass=APIMeta, __factory=None):
    """
    Base class for all Mixins.
    """

    def __init__(*args, **kwargs):
        raise RuntimeError("Mixins can't be directly initialized.")

    __methods__: dict = {}
