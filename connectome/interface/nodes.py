import logging
from typing import Sequence

from ..engine.base import Node

logger = logging.getLogger(__name__)


class NodeStorage(dict):
    def __init__(self):
        super().__init__()
        self.frozen = False

    def add(self, name):
        assert isinstance(name, str)
        if name not in self:
            assert not self.frozen
            super().__setitem__(name, Node(name))

    def freeze(self):
        self.frozen = True

    def __getitem__(self, name):
        self.add(name)
        return super().__getitem__(name)

    def __setitem__(self, key, value):
        raise ValueError


class NodeType:
    __slots__ = 'name',

    def __init__(self, name: str):
        self.name = name


NodeTypes = Sequence[NodeType]


class Input(NodeType):
    pass


class Output(NodeType):
    pass


class InverseInput(NodeType):
    pass


class InverseOutput(NodeType):
    pass


class Parameter(NodeType):
    pass
