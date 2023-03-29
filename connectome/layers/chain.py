from ..containers import EdgesBag
from ..containers.base import connect_bags


def connect(head: EdgesBag, *tail: EdgesBag, freeze: bool = True) -> EdgesBag:
    for container in tail:
        head = connect_bags(head, container, freeze=freeze)
    return head
