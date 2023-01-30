import logging
from typing import Callable, Dict, Type, Union, Iterable, Tuple

from ..engine import Details
from ..utils import MultiDict
from ..layers import CallableLayer
from .compat import SafeMeta
from .factory import (
    SourceFactory, TransformFactory, FactoryLayer, add_from_mixins, add_quals, GraphFactory, items_to_container
)

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
            BASES[base] = factory  # noqa
            return base

        bases = set(bases)
        intersection = set(BASES) & bases
        if len(intersection) != 1:
            raise TypeError(f'Layers must inherit from one of ' + ', '.join(x.__name__ for x in BASES))

        main, = intersection
        factory = BASES[main]
        bases -= intersection
        base_name = main.__name__
        for base in bases:
            if not issubclass(base, Mixin):
                raise TypeError(f'{base_name}s can only inherit directly from "{base_name}" or other mixins.')

        logger.info('Compiling the layer "%s" of type %s', class_name, base_name)

        # a temporary crutch, because we don't have the type itself yet, only its type
        details = Details(class_name)
        if main == Mixin:
            add_from_mixins(namespace, bases)
            scope = add_quals({'__methods__': namespace}, namespace)

        else:
            factory.validate_before_mixins(namespace)
            add_from_mixins(namespace, bases)
            scope = factory.make_scope(details, namespace)

        result = super().__new__(mcs, class_name, (main,), scope, **flags)
        details.layer = result
        return result


class Source(FactoryLayer, metaclass=APIMeta, __factory=SourceFactory):
    """
    Base class for all sources.
    """

    def __init__(self, *args, **kwargs):  # noqa
        raise RuntimeError("\"Source\" can't be directly initialized. You must subclass it first.")


class SourceBase(CallableLayer):
    def __init__(self, items: Union[Iterable[Tuple[str, Callable]], Dict[str, Callable]]):
        super().__init__(*items_to_container(items, None, type(self), SourceFactory))


class Transform(FactoryLayer, metaclass=APIMeta, __factory=TransformFactory):
    """
    Base class for all transforms.

    Can also be used as an inplace factory for transforms.

    Examples
    --------
    # class-based transforms
    >>> from imops import zoom
    >>>
    >>> class Zoom(Transform):
    ...     def image(image):
    ...         return zoom(image, scale_factor=2)
    # inplace transforms
    >>> Transform(image=lambda image: zoom(image, scale_factor=2))
    """
    __inherit__: Union[str, Iterable[str], bool] = ()

    def __init__(*args, __inherit__: Union[str, Iterable[str], bool] = (), **kwargs: Callable):
        assert args
        if len(args) > 1:
            raise TypeError('This constructor accepts only keyword arguments.')
        self, = args
        super(Transform, self).__init__(*items_to_container(kwargs, __inherit__, type(self), TransformFactory), ())

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join(self._methods.fields())})"


class TransformBase(CallableLayer):
    def __init__(self, items: Union[Iterable[Tuple[str, Callable]], Dict[str, Callable]],
                 inherit: Union[str, Iterable[str], bool] = ()):
        super().__init__(*items_to_container(items, inherit, type(self), TransformFactory))


class Mixin(FactoryLayer, metaclass=APIMeta, __factory=None):
    """
    Base class for all Mixins.
    """

    def __init__(*args, **kwargs):  # noqa
        raise RuntimeError("Mixins can't be directly initialized.")

    __methods__: dict = {}
