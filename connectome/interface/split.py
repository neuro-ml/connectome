from typing import Union, Collection, Callable, Iterable

from ..containers import EdgesBag
from ..engine import IdentityEdge
from ..interface.factory import TransformFactory
from ..interface.metaclasses import APIMeta
from ..layers.split import SplitBase


class SplitFactory(TransformFactory):
    _part_name = '__part__'
    _split_name = '__split__'
    layer_cls = SplitBase

    def __init__(self, layer: str, scope):
        self._split: Callable = None
        super().__init__(layer, scope)

    def _prepare_layer_arguments(self, container: EdgesBag, properties: Iterable[str]):
        assert not properties, properties
        return self._split, container

    def _before_collect(self):
        super()._before_collect()
        self.edges.append(IdentityEdge().bind(self.inputs[self._part_name], self.parameters[self._part_name]))
        self.magic_dispatch[self._split_name] = self._handle_split

    def _handle_split(self, value):
        assert self._split is None, self._split
        self._split = value

    def _after_collect(self):
        super()._after_collect()
        assert self._split is not None
        assert not self.special_methods, self.special_methods


# TODO: Examples
class Split(SplitBase, metaclass=APIMeta, __factory=SplitFactory):
    """
    Split a dataset entries into several parts.

    This layer requires a `__split__` magic method, which takes an entry id, and returns a list of parts -
    (part_id, part_context) pairs, "part_id" will become the part's id, and "part_context" is accessible in other
    methods as part-specific useful info.
    """

    __inherit__: Union[str, Collection[str], bool] = ()
    __exclude__: Union[str, Collection[str]] = ()

    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    def __split__(*args, **kwargs):
        raise NotImplementedError
