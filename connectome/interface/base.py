import logging
import warnings
from typing import TypeVar

from .compat import Generic
from ..containers.base import Container
from ..layers.base import CallableLayer, Chain, LazyChain, chained  # noqa
from ..utils import deprecation_warn

logger = logging.getLogger(__name__)
warnings.warn('This module is deprecated', DeprecationWarning)
warnings.warn('This module is deprecated', UserWarning)
T = TypeVar('T', bound=Container)


class BaseLayer(Generic[T]):
    def __init__(self, container: T):
        deprecation_warn()
        self._container: T = container
