from typing import Type, Union, Collection, Callable, Iterable, Tuple, Dict

from .reversible import ReversibleContainer
from ..engine import IdentityEdge
from ..exceptions import FieldError
from ..interface import RuntimeAnnotation
from ..containers.reversible import normalize_inherit
from ..interface.factory import is_detectable, items_to_container
from ..interface.nodes import NodeTypes, Input
from ..utils import MultiDict, AntiSet
from ..interface import APIMeta, GraphFactory
from ..layer import Layer, EdgeContainer


class SourceFactory(GraphFactory):
    layer_cls = ReversibleContainer

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

    @staticmethod
    def validate_before_mixins(namespace: MultiDict):
        duplicates = {name for name, values in namespace.groups() if len(values) > 1}
        if duplicates:
            raise TypeError(f'Duplicated methods found: {duplicates}')

    def _before_collect(self):
        self._key_name = 'id'
        if self._key_name in self.scope:
            raise FieldError(f'Cannot override the key attribute ({self._key_name})')

        self.ID = self.inputs[self._key_name]
        self.inputs.freeze()
        self.edges.append(IdentityEdge().bind(self.ID, self.outputs[self._key_name]))
        self.persistent_names.add(self._key_name)

    def _after_collect(self):
        self.persistent_names.update(self.property_names)

    def _validate_inputs(self, inputs: NodeTypes) -> NodeTypes:
        ids = {x for x in inputs if isinstance(x, Input)}
        if not ids:
            return inputs
        if len(ids) > 1:
            raise FieldError(f'Trying to use multiple arguments as keys: {tuple(sorted(x.name for x in ids))}')
        i, = ids
        return [x if x != i else Input(self._key_name) for x in inputs]


class Source(ReversibleContainer, metaclass=APIMeta, __factory=SourceFactory):
    """
    Base class for all sources.
    """

    def __init__(self, *args, **kwargs):  # noqa
        raise RuntimeError("\"Source\" can't be directly initialized. You must subclass it first.")


class SourceBase(ReversibleContainer):
    def __init__(self, items: Union[Iterable[Tuple[str, Callable]], Dict[str, Callable]]):
        super().__init__(*items_to_container(items, type(self), SourceFactory))
