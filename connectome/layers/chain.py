from ..layer import Layer, Slice
from ..containers.base import connect_bags


class Chain(Layer):
    def __init__(self, *layers: Layer):
        head, *tail = layers
        for layer in tail:
            head = layer._connect(head)

        self._connected = head
        self._layers = layers

    def _slice(self, names) -> Slice:
        return self._connected._slice(names)


EdgesBag = object

def connect(head: EdgesBag, *tail: EdgesBag, freeze: bool = True) -> EdgesBag:
    for container in tail:
        head = connect_bags(head, container, freeze=freeze)
    return head
