import logging
from typing import Callable, Collection, Dict, Iterable, Tuple, Type, Union

from .edges import DecoratorMixin
# from ..layers import CallableLayer, Layer
from ..layer import Layer
from ..utils import MultiDict
from .decorators import RuntimeAnnotation
from .factory import GraphFactory, add_from_mixins, add_quals, items_to_container

logger = logging.getLogger(__name__)
BASES: Dict[Type[Layer], GraphFactory] = {}


class APIMeta(type):
    @classmethod
    def __prepare__(mcs, *args, **kwargs):
        return MultiDict()

    def __getattr__(self, item):
        # protection from recursion
        if item == '__original__scope__':
            raise AttributeError(item)

        # we need this behaviour mostly to support pickling of functions defined inside the class
        try:
            value = self.__original__scope__[item]
            while isinstance(value, RuntimeAnnotation):
                value = value.__func__
            while isinstance(value, DecoratorMixin):
                value = value.unwrap()
            return value
        except KeyError:
            raise AttributeError(item) from None

    def __new__(mcs, class_name, bases, namespace, **flags):
        if '__factory' in flags:
            factory = flags.pop('__factory')
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

        if main == Mixin:
            add_from_mixins(namespace, bases)
            scope = add_quals({'__methods__': namespace}, namespace)

        else:
            factory.validate_before_mixins(namespace)
            add_from_mixins(namespace, bases)
            scope = factory.make_scope(class_name, namespace)

        # TODO: need a standardized set of magic fields
        scope['__original__scope__'] = namespace
        return super().__new__(mcs, class_name, (main,), scope, **flags)






class Mixin(metaclass=APIMeta, __factory=None):
    """
    Base class for all Mixins.
    """

    def __init__(*args, **kwargs):  # noqa
        raise RuntimeError("Mixins can't be directly initialized.")

    __methods__: dict = {}
