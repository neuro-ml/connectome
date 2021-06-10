import logging

from .base import CallableBlock
from .compat import SafeMeta

from ..layers.transform import InheritType
from ..utils import MultiDict
from .factory import SourceFactory, TransformFactory

logger = logging.getLogger(__name__)


class APIMeta(SafeMeta):
    @classmethod
    def __prepare__(mcs, *args, **kwargs):
        return MultiDict()


def _check_duplicates(namespace):
    duplicates = {name for name, values in namespace.groups() if len(values) > 1}
    if duplicates:
        raise TypeError(f'Duplicated methods found: {duplicates}')


# TODO: move all the logic to a single metaclass
class SourceBase(APIMeta):
    def __new__(mcs, class_name, bases, namespace, **flags):
        if flags.get('__root', False):
            def __init__(*args, **kwargs):
                raise RuntimeError("\"Source\" can't be directly initialized. You must subclass it first.")

            assert bases == (CallableBlock,)
            scope = {'__init__': __init__}

        else:
            bases = set(bases) - {Source}
            for base in bases:
                if not issubclass(base, Mixin):
                    raise TypeError('Source datasets can only inherit directly from "Source" or mixins.')

            _check_duplicates(namespace)
            _add_from_mixins(namespace, bases)
            bases = CallableBlock,
            scope = SourceFactory.make_scope(namespace)

        return super().__new__(mcs, class_name, bases, scope)


class TransformBase(APIMeta):
    def __new__(mcs, class_name, bases, namespace, **flags):
        if flags.get('__root', False):
            # we can construct transforms on the fly
            def __init__(*args, __inherit__=(), **kwargs):
                assert args
                if len(args) > 1:
                    raise TypeError('This constructor accepts only keyword arguments.')
                self, = args

                local = MultiDict()
                local['__inherit__'] = __inherit__
                for name, value in kwargs.items():
                    assert callable(value)
                    local[name] = value

                factory = TransformFactory(local)
                super(type(self), self).__init__(factory.build({}), factory.property_names)

            assert bases == (CallableBlock,)
            scope = {'__init__': __init__, '__doc__': namespace['__doc__']}

        else:
            bases = set(bases) - {Transform}
            for base in bases:
                if not issubclass(base, Mixin):
                    raise TypeError('Transforms datasets can only inherit directly from "Transform" or mixins.')

            _add_from_mixins(namespace, bases)
            bases = CallableBlock,
            logger.info('Compiling the block "%s"', class_name)
            scope = TransformFactory.make_scope(namespace)

        return super().__new__(mcs, class_name, bases, scope)


class Source(CallableBlock, metaclass=SourceBase, __root=True):
    """
    Base class for all sources.
    """

    def __init__(self, *args, **kwargs):
        pass


class Transform(CallableBlock, metaclass=TransformBase, __root=True):
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

    # these methods are ignored in the metaclass
    # we use them only to help IDEs
    def __init__(self, *args, **kwargs):
        pass


class Mixin:
    """
    Base class for all Mixins.
    """

    def __init__(*args, **kwargs):
        raise RuntimeError("Mixins can't be directly initialized.")

    __methods__: dict = {}

    def __init_subclass__(cls, **kwargs):
        if cls.__init__ != Mixin.__init__:
            raise RuntimeError("Mixins can't be directly initialized.")

        bases = cls.__bases__
        for base in bases:
            if not issubclass(base, Mixin):
                raise TypeError(f'Mixins can only inherit other mixins.')

        namespace = {name: getattr(cls, name) for name in dir(cls) if not name.startswith('__')}
        cls.__methods__ = _add_from_mixins(dict(namespace.items()), bases)


def _add_from_mixins(namespace, mixins):
    for mixin in mixins:
        assert issubclass(mixin, Mixin), mixin
        # update without overwriting
        local = mixin.__methods__
        intersection = set(local) & set(namespace)
        if intersection:
            raise RuntimeError(f'Trying to overwrite the names {intersection} from mixin {mixin}')

        for name in local:
            namespace[name] = local[name]

    return namespace
