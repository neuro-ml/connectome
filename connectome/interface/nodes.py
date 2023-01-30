import logging
from typing import Sequence, Type, Union

from ..engine import Node

logger = logging.getLogger(__name__)


class NodeStorage(dict):
    def __init__(self, details):
        super().__init__()
        self.frozen = False
        self.details = details

    def add(self, name):
        assert isinstance(name, str)
        if name not in self:
            assert not self.frozen
            super().__setitem__(name, Node(name, self.details))

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

    def __repr__(self):
        return f'<{type(self).__name__}: {self.name}>'


NodeTypes = Sequence[NodeType]


class FinalNodeType(NodeType):
    """ Eventually the graph factory contains only this type of nodes """


class Input(FinalNodeType):
    pass


class Output(FinalNodeType):
    pass


class InverseInput(FinalNodeType):
    pass


class InverseOutput(FinalNodeType):
    pass


class Parameter(FinalNodeType):
    pass


# special types

class Default(NodeType):
    pass


class AsOutput(NodeType):
    def __init__(self):
        super().__init__('')


class Intermediate(NodeType):
    def __init__(self):
        super().__init__('')


class NodeModifier:
    __slots__ = 'node',

    def __init__(self, node: Union[NodeType, 'NodeModifier', Type[NodeType]]):
        self.node = node


class Silent(NodeModifier):
    pass


def is_private(name: str):
    return name.startswith('_')
