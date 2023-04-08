from abc import ABC, abstractmethod

from ..containers import EdgesBag
from .base import Layer
from .chain import connect


class DynamicConnectLayer(Layer, ABC):
    def _connect(self, previous: EdgesBag) -> EdgesBag:
        return connect(previous, self._prepare_container(previous))

    @abstractmethod
    def _prepare_container(self, previous: EdgesBag) -> EdgesBag:
        """ Create a fitting container which will be connected to the `previous` one """
