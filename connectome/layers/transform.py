from typing import Type, Union, Collection, Callable, Iterable, Tuple, Dict

from .reversible import ReversibleContainer
from ..interface import RuntimeAnnotation
from ..containers.reversible import normalize_inherit
from ..interface.factory import is_detectable, items_to_container
from ..interface.nodes import NodeTypes
from ..utils import MultiDict, AntiSet
from ..interface import APIMeta, GraphFactory
from ..layer import Layer, EdgeContainer


class TransformFactory(GraphFactory):
    layer_cls = ReversibleContainer

    def __init__(self, layer: str, scope: MultiDict):
        self._exclude = None
        super().__init__(layer, scope)

    def prepare_layer_arguments(self, arguments: dict):
        diff = list(set(self.arguments) - set(arguments))
        if diff:
            raise TypeError(f'Missing required arguments: {diff}.')

        # logger.info(
        #     'Compiling layer. Inputs: %s, Outputs: %s, BackwardInputs: %s, BackwardOutputs: %s',
        #     list(self.inputs), list(self.outputs), list(self.backward_inputs), list(self.backward_outputs),
        # )
        return (
            list(self.inputs.values()), list(self.outputs.values()),
            self.edges + list(self._get_constant_edges(arguments)),
            list(self.backward_inputs.values()), list(self.backward_outputs.values()),
            self.forward_inherit, self.backward_inherit,
            # self.property_names,
        )
        # return container, self.property_names

    def _before_collect(self):
        self.magic_dispatch['__inherit__'] = self._process_inherit
        self.magic_dispatch['__exclude__'] = self._process_exclude

    def _after_collect(self):
        if self.forward_inherit and self._exclude:
            raise ValueError('Can specify either "__inherit__" or "__exclude__" but not both')
        if self._exclude:
            self.forward_inherit = AntiSet(self._exclude)

        forward, valid = normalize_inherit(self.forward_inherit, self.outputs)
        if not valid:
            raise TypeError(f'"__inherit__" can be either True, a string, or a sequence of strings, but got {forward}')

        backward, valid = normalize_inherit(self.forward_inherit, self.backward_outputs)
        assert valid

        self.forward_inherit = forward
        self.backward_inherit = backward

    def _validate_inputs(self, inputs: NodeTypes) -> NodeTypes:
        return inputs

    def _process_inherit(self, value):
        # save this value for final step
        self.forward_inherit = value

    def _process_exclude(self, value):
        if isinstance(value, str):
            value = value,
        value = tuple(value)
        if not all(isinstance(x, str) for x in value):
            raise TypeError('"__exclude__" must be either a string or a sequence of strings')
        self._exclude = value


class Transform(ReversibleContainer, metaclass=APIMeta, __factory=TransformFactory):
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
    __inherit__: Union[str, Collection[str], bool] = ()
    __exclude__: Union[str, Collection[str]] = ()

    def __init__(*args, __inherit__: Union[str, Collection[str], bool] = (),
                 __exclude__: Union[str, Collection[str]] = (), **kwargs: Callable):
        assert args
        if len(args) > 1:
            raise TypeError('This constructor accepts only keyword arguments.')
        self, = args
        super(Transform, self).__init__(*items_to_container(
            kwargs, type(self), TransformFactory, __inherit__=__inherit__, __exclude__=__exclude__
        ))

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join(self._methods.fields())})"


class TransformBase(ReversibleContainer):
    def __init__(self, items: Union[Iterable[Tuple[str, Callable]], Dict[str, Callable]],
                 inherit: Union[str, Collection[str], bool] = (), exclude: Union[str, Collection[str]] = ()):
        super().__init__(*items_to_container(
            items, type(self), TransformFactory, __inherit__=inherit, __exclude__=exclude
        ))
